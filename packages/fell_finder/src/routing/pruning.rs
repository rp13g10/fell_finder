//! This module defines the code which keeps the number of candidates being
//! processed at any one time under control. By pruning routes, we are able to
//! retain the most promising candidates, with any near-duplicates being
//! removed. Additional logic is implemented to ensure a good geographical
//! spread of candidates in the output.

use rustc_hash::FxHashMap;
use std::cmp::{Ordering, min};
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::env;
use std::io::{Error, ErrorKind};
use std::iter::zip;
use std::sync::Arc;

use rayon::prelude::*;

use crate::common::config::{RouteConfig, RouteMode};
use crate::routing::common::Candidate;

/// For a provided vector of floats, retrieve the minimum and maximum values
/// and return them as a tuple. If an empty vector is provided, an Error will
/// be returned
fn get_min_max_vals(vals: &Vec<f64>) -> Result<(f64, f64), Error> {
    let min_val: f64;
    let max_val: f64;

    if let Some(val) = vals.iter().min_by(|a, b| a.total_cmp(b)) {
        min_val = *val;
    } else {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "Unable to get minimum value, maybe the input vector is empty?",
        ));
    }

    if let Some(val) = vals.iter().max_by(|a, b| a.total_cmp(b)) {
        max_val = *val;
    } else {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "Unable to get maximum value, maybe the input vector is empty?",
        ));
    }

    Ok((min_val, max_val))
}

/// Calculate the bin for a provided point (latitude or longitude)
///
/// Args:
/// min_val: The minimum value across all values being binned
/// delta: The difference between the minimum and maximum across all values
///        being binned
/// steps: The total number of steps/bins to be created
/// val: The value to be binned
fn get_bin(min_val: f64, delta: f64, steps: u32, val: f64) -> u32 {
    let step_size = delta / steps as f64;

    let val_offset = val - min_val;

    // Prevent floating point errors from creating a n+1th bin
    min((val_offset / step_size) as u32, steps - 1)
}

#[derive(Eq, Hash, PartialEq, Debug)]
struct Bin(u32, u32);

///Assign all of the provided routes to bins based on their central
///coordinates. Routes are binned across both lats and lons, analagous
///to placing them into a 2d grid.
fn bin_candidates(
    candidates: Vec<Candidate>,
) -> Result<FxHashMap<Bin, Vec<Candidate>>, Error> {
    // Fetch target number of bins (for each dimension, total will be N**2)
    let n_bins: u32 = match env::var("FF_NUM_BINS") {
        Ok(val) => val
            .parse()
            .expect("FF_NUM_BINS must be set to an integer value"),
        Err(_) => 4, // Default behaviour, will apply during test runs
    };

    let mut binned: FxHashMap<Bin, Vec<Candidate>> = FxHashMap::default();

    let (lats, lons): (Vec<f64>, Vec<f64>) = candidates
        .iter()
        // TODO:  See if there's a faster way to get route centroids
        .map(|cand| cand.geometry.get_pos())
        .unzip();

    let (min_lat, max_lat) =
        get_min_max_vals(&lats).expect("Error calculating min/max lat");
    let (min_lon, max_lon) =
        get_min_max_vals(&lons).expect("Error calculating min/max lon");

    let delta_lat = max_lat - min_lat;
    let delta_lon = max_lon - min_lon;

    for ((lat, lon), candidate) in zip(zip(lats, lons), candidates) {
        let lat_bin = get_bin(min_lat, delta_lat, n_bins, lat);
        let lon_bin = get_bin(min_lon, delta_lon, n_bins, lon);

        let bin = Bin(lat_bin, lon_bin);

        match binned.entry(bin) {
            Occupied(cands) => cands.into_mut().push(candidate),
            Vacant(cands) => {
                cands.insert(Vec::from([candidate]));
            }
        };
    }

    Ok(binned)
}

/// Determine the level of similarity between two candidates based on the
/// degree of crossover between the nodes in each
fn get_similarity(c1: &Candidate, c2: &Candidate) -> f64 {
    let union = c1.visited.union(&c2.visited).into_iter().count() as f64;
    let intersection =
        c1.visited.intersection(&c2.visited).into_iter().count() as f64;
    intersection / union
}

/// Return the Ordering of candidate a relative to candidate b. Candidates are
/// sorted by distance in KMs, then by elevation gain
fn get_cand_ordering(
    a: &Candidate,
    b: &Candidate,
    mode: &RouteMode,
) -> Ordering {
    let a_t1 = (a.metrics.common.dist / 1000.0) as isize;
    let b_t1 = (b.metrics.common.dist / 1000.0) as isize;

    let a_t2 = match mode {
        RouteMode::Hilly => a.metrics.common.gain,
        RouteMode::Flat => -a.metrics.common.gain,
    } as isize;

    let b_t2 = match mode {
        RouteMode::Hilly => b.metrics.common.gain,
        RouteMode::Flat => -b.metrics.common.gain,
    } as isize;

    let a_data = (a_t1, a_t2);

    let b_data = (b_t1, b_t2);

    a_data.cmp(&b_data)
}

// Sort a vector of candidates according to the user preference, the
// hilliest/flattest route will become the first item in the vector
pub fn sort_candidates(
    candidates: &mut Vec<Candidate>,
    config: Arc<RouteConfig>,
) {
    // Note inverse comparison to sort in descending order
    candidates.sort_by(|a, b| get_cand_ordering(b, a, &config.route_mode));
}

/// For the selected threshold, check whether the provided candidate is below
/// the threshold for every candidate which has already been selected
fn check_if_candidate_is_dissimilar(
    candidate: &Candidate,
    selected: &Vec<Candidate>,
    threshold: &f64,
) -> bool {
    for selected_candidate in selected.iter() {
        // Compare scores
        let similarity = &get_similarity(candidate, &selected_candidate);
        if similarity > threshold {
            return false;
        }
    }
    true
}

/// Retain only sufficiently different routes, the similarity threshold will be
/// set dynamically in order to reach the target count of routes. Note that the
/// output may not be sorted, use sort_candidates before presenting to the user
pub fn get_dissimilar_routes(
    candidates: &mut Vec<Candidate>,
    target_count: usize,
    config: Arc<RouteConfig>,
    max_similarity: f64,
) -> Vec<Candidate> {
    // TODO: Run profiler and check if there are any bottlenecks here
    // TODO: Add support for max_similarity here using FF_MAX_SIMILARITY evar

    // Nothing to do if count is already below target
    if candidates.len() <= target_count {
        return candidates.to_owned();
    }

    // Otherwise, sort candidates and prepare to select
    sort_candidates(candidates, Arc::clone(&config));

    // Take the first entry and use it as a seed for the output vector
    let split = candidates.split_at_mut(1);
    let mut selected: Vec<Candidate> = split.0.into();
    let mut to_process: Vec<Candidate> = split.1.into();

    // Start with similarity threshold of 0.5
    let mut threshold = 0.5;

    // Create a new vector to hold candidates not selected at current
    // threshold, just in case we need to increase it and try again
    let mut too_similar = Vec::<Candidate>::new();
    let mut target_met = false;

    // Keep going until required number of routes has been selected
    while (!target_met) & (to_process.len() > 0) {
        // Compare every candidate to the ones already selected, keep them if
        // they are sufficiently different from all other selected routes
        for candidate in to_process.drain(..) {
            match check_if_candidate_is_dissimilar(
                &candidate, &selected, &threshold,
            ) {
                true => {
                    selected.push(candidate);

                    // Stop checking once target count is met
                    if selected.len() == target_count {
                        target_met = true;
                        break;
                    }
                }
                false => too_similar.push(candidate),
            }
        }

        // If required number of candidates has not been seleted, increase
        // the threshold and try again
        if (!target_met) & (threshold < max_similarity) {
            to_process.extend(too_similar.drain(..));
            threshold += 0.1;
            if threshold > max_similarity {
                threshold = max_similarity;
            }
        }
    }

    to_process.clear();
    selected
}

/// Retrieve a limited subset of routes, with an initial binning step to ensure
/// a good distribution of route shapes. This aims to avoid the trap of routes
/// which find hills earlier on from being selected over those which find them
/// later in the process
pub fn prune_candidates(
    candidates: Vec<Candidate>,
    config: Arc<RouteConfig>,
) -> Vec<Candidate> {
    if candidates.len() <= config.max_candidates {
        return candidates;
    }

    // Set max similarity between candidates
    let threshold: f64 = match env::var("FF_PRUNING_THRESHOLD") {
        Ok(val) => val
            .parse()
            .expect("FF_PRUNING_THRESHOLD must be set to a float value"),
        Err(_) => 0.95, // Default behaviour, will apply during test runs
    };

    // TODO: Improve error handling here

    let binned = match bin_candidates(candidates) {
        Ok(cands) => cands,
        Err(_) => panic!("Error while binning candidates"),
    };

    let mut vec_binned: Vec<Vec<Candidate>> =
        binned.into_iter().map(|(_, cands)| cands).collect();
    let mut vec_selected: Vec<Vec<Candidate>> = Vec::new();

    let bin_target: usize = config.max_candidates / vec_binned.len();

    vec_binned
        .par_iter_mut()
        .map(|bin_cands| {
            get_dissimilar_routes(
                bin_cands,
                bin_target,
                Arc::clone(&config),
                threshold,
            )
        })
        .collect_into_vec(&mut vec_selected);

    vec_selected.into_iter().flatten().collect()
}

#[cfg(test)]
mod tests {

    use std::collections::HashSet;

    use petgraph::graph::NodeIndex;
    use rustc_hash::FxHashMap;

    use crate::routing::common::geometry::CandidateGeometry;
    use crate::routing::common::metrics::CandidateMetrics;

    use super::*;

    fn get_test_config() -> RouteConfig {
        RouteConfig {
            centre: (0.0, 0.0).into(),
            route_mode: RouteMode::Hilly,
            max_candidates: 1,
            min_distance: 9000.0,
            max_distance: 10000.0,
            highways: vec!["highway_1".to_string()],
            surfaces: vec!["surface_1".to_string(), "surface_2".to_string()],
            surface_restriction: None,
        }
    }

    fn get_test_candidate() -> Candidate {
        Candidate {
            points: Vec::new(),
            visited: HashSet::default(),
            geometry: CandidateGeometry::new(),
            metrics: CandidateMetrics::new(),
            config: Arc::new(get_test_config()),
            cur_inx: NodeIndex::new(0),
        }
    }
    #[cfg(test)]
    mod test_get_min_max_vals {
        use super::*;

        #[test]
        fn test_error() {
            let test_vals = Vec::<f64>::new();

            match get_min_max_vals(&test_vals) {
                Ok(_) => panic!("This shouldn't have happened!"),
                Err(_) => (),
            }
        }

        #[test]
        fn test_success() {
            let test_vals = vec![1.0, 0.0, 10.0, 5.0];

            let (tgt_min, tgt_max) = (0.0, 10.0);

            let (res_min, res_max) = match get_min_max_vals(&test_vals) {
                Ok((min, max)) => (min, max),
                Err(_) => panic!("This shouldn't have happened"),
            };

            assert_eq!(res_min, tgt_min);
            assert_eq!(res_max, tgt_max);
        }
    }

    #[cfg(test)]
    mod test_get_bin {

        use super::*;

        #[test]
        fn start_of_range() {
            let test_min_val = 10.0;
            let test_delta = 25.0;
            let test_steps: u32 = 5;
            let test_val = 10.0;

            let target = 0;

            let result =
                get_bin(test_min_val, test_delta, test_steps, test_val);

            assert_eq!(result, target);
        }

        #[test]
        fn middle_of_range() {
            let test_min_val = 10.0;
            let test_delta = 25.0;
            let test_steps: u32 = 5;
            let test_val = 22.5;

            let target = 2;

            let result =
                get_bin(test_min_val, test_delta, test_steps, test_val);

            assert_eq!(result, target);
        }

        #[test]
        fn end_of_range() {
            let test_min_val = 10.0;
            let test_delta = 25.0;
            let test_steps: u32 = 5;
            let test_val = 35.0;

            let target = 4;

            let result =
                get_bin(test_min_val, test_delta, test_steps, test_val);

            assert_eq!(result, target);
        }
    }

    #[test]
    fn test_bin_candidates() {
        let mut test_cand_1 = get_test_candidate();
        let mut test_cand_2 = get_test_candidate();
        let mut test_cand_3 = get_test_candidate();

        test_cand_1.geometry.lats = vec![0.0];
        test_cand_1.geometry.lons = vec![0.0];

        test_cand_2.geometry.lats = vec![10.0];
        test_cand_2.geometry.lons = vec![10.0];

        test_cand_3.geometry.lats = vec![25.0];
        test_cand_3.geometry.lons = vec![25.0];

        let test_candidates = vec![
            test_cand_1.clone(),
            test_cand_2.clone(),
            test_cand_3.clone(),
        ];

        let mut target = FxHashMap::<Bin, Vec<Candidate>>::default();

        target.insert(Bin(0, 0), vec![test_cand_1.clone()]);
        target.insert(Bin(1, 1), vec![test_cand_2.clone()]);
        target.insert(Bin(3, 3), vec![test_cand_3.clone()]);

        let result = match bin_candidates(test_candidates) {
            Ok(candidates) => candidates,
            Err(_) => panic!("Error while binning test data"),
        };

        assert_eq!(result, target);
    }

    #[cfg(test)]
    mod test_get_similarity {
        use super::*;

        #[test]
        fn test_no_overlap() {
            let mut test_c1 = get_test_candidate();
            let mut test_c2 = get_test_candidate();

            for point in [0, 1, 2, 3, 4].into_iter() {
                test_c1.visited.insert(point);
            }

            for point in [5, 6, 7, 8, 9].into_iter() {
                test_c2.visited.insert(point);
            }

            let target = 0.0;

            let result = get_similarity(&test_c1, &test_c2);

            assert_eq!(result, target);
        }

        #[test]
        fn test_half_overlap() {
            let mut test_c1 = get_test_candidate();
            let mut test_c2 = get_test_candidate();

            for point in [0, 1, 2].into_iter() {
                test_c1.visited.insert(point);
            }

            for point in [1, 2, 3].into_iter() {
                test_c2.visited.insert(point);
            }

            let target = 0.5;

            let result = get_similarity(&test_c1, &test_c2);

            assert_eq!(result, target);
        }

        #[test]
        fn test_full_overlap() {
            let mut test_c1 = get_test_candidate();
            let mut test_c2 = get_test_candidate();

            for point in [0, 1, 2, 3, 4].into_iter() {
                test_c1.visited.insert(point);
                test_c2.visited.insert(point);
            }

            let target = 1.0;

            let result = get_similarity(&test_c1, &test_c2);

            assert_eq!(result, target);
        }
    }

    #[cfg(test)]
    mod test_check_if_candidate_is_dissimilar {
        use super::*;

        #[test]
        fn test_true() {
            let mut test_candidate = get_test_candidate();
            for point in [0, 1, 2, 3].into_iter() {
                test_candidate.visited.insert(point);
            }

            let mut test_selected_1 = get_test_candidate();
            let mut test_selected_2 = get_test_candidate();
            let mut test_selected_3 = get_test_candidate();

            // 3 in 5 points match for each comparison -> 60% similarity

            for point in [1, 2, 3, 4].into_iter() {
                test_selected_1.visited.insert(point);
            }

            for point in [0, 1, 7, 3].into_iter() {
                test_selected_2.visited.insert(point);
            }

            for point in [1, 2, 3, 5].into_iter() {
                test_selected_3.visited.insert(point);
            }

            let test_selected =
                vec![test_selected_1, test_selected_2, test_selected_3];

            let test_threshold = 0.7;

            let result = check_if_candidate_is_dissimilar(
                &test_candidate,
                &test_selected,
                &test_threshold,
            );

            assert!(result);
        }

        #[test]
        fn test_false() {
            let mut test_candidate = get_test_candidate();
            for point in [0, 1, 2, 3].into_iter() {
                test_candidate.visited.insert(point);
            }

            let mut test_selected_1 = get_test_candidate();
            let mut test_selected_2 = get_test_candidate();
            let mut test_selected_3 = get_test_candidate();

            // 3 in 5 points match for each comparison -> 60% similarity

            for point in [1, 2, 3, 4].into_iter() {
                test_selected_1.visited.insert(point);
            }

            for point in [0, 1, 7, 3].into_iter() {
                test_selected_2.visited.insert(point);
            }

            for point in [1, 2, 3, 5].into_iter() {
                test_selected_3.visited.insert(point);
            }

            let test_selected =
                vec![test_selected_1, test_selected_2, test_selected_3];

            let test_threshold = 0.5;

            let result = check_if_candidate_is_dissimilar(
                &test_candidate,
                &test_selected,
                &test_threshold,
            );

            assert!(!result);
        }
    }

    #[cfg(test)]
    mod test_get_cand_ordering {
        use super::*;

        #[test]
        fn test_gt_same_kms_hilly() {
            let test_mode = RouteMode::Hilly;
            let mut c1 = get_test_candidate();
            let mut c2 = get_test_candidate();

            // Distances should be same after cast to int --> order by gain
            c1.metrics.common.dist = 10543.2;
            c2.metrics.common.dist = 10987.6;

            c1.metrics.common.gain = 1000.0;
            c2.metrics.common.gain = 900.0;

            let target = Ordering::Greater;

            let result = get_cand_ordering(&c1, &c2, &test_mode);

            assert_eq!(result, target);
        }

        #[test]
        fn test_lt_same_kms_hilly() {
            let test_mode = RouteMode::Hilly;
            let mut c1 = get_test_candidate();
            let mut c2 = get_test_candidate();

            // Distances should be same after cast to int --> order by gain
            c1.metrics.common.dist = 10543.2;
            c2.metrics.common.dist = 10987.6;

            c1.metrics.common.gain = 900.0;
            c2.metrics.common.gain = 1000.0;

            let target = Ordering::Less;

            let result = get_cand_ordering(&c1, &c2, &test_mode);

            assert_eq!(result, target);
        }

        #[test]
        fn test_gt_same_kms_flat() {
            let test_mode = RouteMode::Flat;
            let mut c1 = get_test_candidate();
            let mut c2 = get_test_candidate();

            // Distances should be same after cast to int --> order by gain
            c1.metrics.common.dist = 10543.2;
            c2.metrics.common.dist = 10987.6;

            c1.metrics.common.gain = 900.0;
            c2.metrics.common.gain = 1000.0;

            let target = Ordering::Greater;

            let result = get_cand_ordering(&c1, &c2, &test_mode);

            assert_eq!(result, target);
        }

        #[test]
        fn test_lt_same_kms_flat() {
            let test_mode = RouteMode::Flat;
            let mut c1 = get_test_candidate();
            let mut c2 = get_test_candidate();

            // Distances should be same after cast to int --> order by gain
            c1.metrics.common.dist = 10543.2;
            c2.metrics.common.dist = 10987.6;

            c1.metrics.common.gain = 1000.0;
            c2.metrics.common.gain = 900.0;

            let target = Ordering::Less;

            let result = get_cand_ordering(&c1, &c2, &test_mode);

            assert_eq!(result, target);
        }

        #[test]
        fn test_gt_diff_kms() {
            let test_mode = RouteMode::Hilly;
            let mut c1 = get_test_candidate();
            let mut c2 = get_test_candidate();

            // Different distances after rounding, sort should prefer the
            // longer route
            c1.metrics.common.dist = 11543.2;
            c2.metrics.common.dist = 10987.6;

            c1.metrics.common.gain = 1000.0;
            c2.metrics.common.gain = 900.0;

            let target = Ordering::Greater;

            let result = get_cand_ordering(&c1, &c2, &test_mode);

            assert_eq!(result, target);
        }

        #[test]
        fn test_lt_diff_kms() {
            let test_mode = RouteMode::Flat;
            let mut c1 = get_test_candidate();
            let mut c2 = get_test_candidate();

            // Different distances after rounding, sort should prefer the
            // longer route
            c1.metrics.common.dist = 10543.2;
            c2.metrics.common.dist = 11987.6;

            c1.metrics.common.gain = 1000.0;
            c2.metrics.common.gain = 900.0;

            let target = Ordering::Less;

            let result = get_cand_ordering(&c1, &c2, &test_mode);

            assert_eq!(result, target);
        }

        #[test]
        fn test_eq() {
            let test_mode = RouteMode::Hilly;
            let mut c1 = get_test_candidate();
            let mut c2 = get_test_candidate();

            // Distances should be same after cast to int --> order by gain
            c1.metrics.common.dist = 10543.2;
            c2.metrics.common.dist = 10987.6;

            c1.metrics.common.gain = 1000.0;
            c2.metrics.common.gain = 1000.0;

            let target = Ordering::Equal;

            let result = get_cand_ordering(&c1, &c2, &test_mode);

            assert_eq!(result, target);
        }
    }

    #[test]
    fn test_sort_candidates() {
        let mut c1 = get_test_candidate();
        let mut c2 = get_test_candidate();
        let mut c3 = get_test_candidate();

        // Test config has RouteType::Hilly
        let test_config = Arc::new(get_test_config());

        // 1 == 2 < 3
        c1.metrics.common.dist = 10543.2;
        c2.metrics.common.dist = 10987.6;
        c3.metrics.common.dist = 11456.7;

        // 1 > 2 > 3
        c1.metrics.common.gain = 1300.0;
        c2.metrics.common.gain = 1200.0;
        c3.metrics.common.gain = 1100.0;

        // Longest first, then sorted by gain. Hillest route first in output.
        let target = vec![c3.clone(), c1.clone(), c2.clone()];

        let mut result = vec![c3.clone(), c2.clone(), c1.clone()];
        sort_candidates(&mut result, test_config);

        assert_eq!(result, target);
    }

    #[test]
    fn test_get_dissimilar_routes() {
        let mut test_candidate_1 = get_test_candidate();
        let mut test_candidate_2 = get_test_candidate();
        let mut test_candidate_3 = get_test_candidate();
        let mut test_candidate_4 = get_test_candidate();
        let mut test_candidate_5 = get_test_candidate();

        // Candidate 5 is the longest, and will be selected first
        for point in [0, 1, 2, 3].into_iter() {
            test_candidate_5.visited.insert(point);
        }
        test_candidate_5.metrics.common.dist = 11000.0;
        test_candidate_5.metrics.common.gain = 500.0;

        // Candidates 2 and 3 are very similar, 2 selected first due to gain
        for point in [4, 5, 6, 7, 8].into_iter() {
            test_candidate_2.visited.insert(point.clone());
            test_candidate_3.visited.insert(point);
        }
        test_candidate_2.visited.insert(9);

        test_candidate_2.metrics.common.dist = 10000.0;
        test_candidate_3.metrics.common.dist = 10000.0;
        test_candidate_2.metrics.common.gain = 500.0;
        test_candidate_3.metrics.common.gain = 400.0;

        // Candidates 1 and 4 are different, 1 selected first due to gain
        // Over 50% similarity, but picked up on later iterations
        for point in [6, 7, 8].into_iter() {
            test_candidate_1.visited.insert(point.clone());
            test_candidate_4.visited.insert(point);
        }
        test_candidate_1.visited.insert(10);
        test_candidate_4.visited.insert(11);

        test_candidate_1.metrics.common.dist = 10000.0;
        test_candidate_4.metrics.common.dist = 10000.0;
        test_candidate_1.metrics.common.gain = 300.0;
        test_candidate_4.metrics.common.gain = 200.0;

        let test_config = Arc::new(get_test_config());
        let test_target_count: usize = 3;
        let test_threshold: f64 = 0.95;

        let mut test_candidates = vec![
            test_candidate_1.clone(),
            test_candidate_2.clone(),
            test_candidate_3.clone(),
            test_candidate_4.clone(),
            test_candidate_5.clone(),
        ];

        let target =
            vec![test_candidate_5, test_candidate_2, test_candidate_1];

        let result = get_dissimilar_routes(
            &mut test_candidates,
            test_target_count,
            test_config,
            test_threshold,
        );

        assert_eq!(result, target);
        // panic!("Panic!")
    }

    #[test]
    fn test_prune_candidates() {
        // Skipping this for now, setup will need some thought and complex
        // logic is tested separately
    }
}
