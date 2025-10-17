//! This module defines the logic which is used to reduce the total number of
//! candidates held in memory at any one time. This is required in order to
//! generate routes within a reasonable time-frame.

use std::cmp::Ordering;
use std::sync::Arc;

use rayon::prelude::*;

use crate::common::config::{BackendConfig, RouteConfig, RouteMode};
use crate::common::routes::Candidate;
use crate::pruning::binning::{bin_candidates, get_bin_details};

mod binning;

/// Determine the level of similarity between two candidates based on the
/// degree of crossover between the nodes in each
fn get_similarity(c1: &Candidate, c2: &Candidate) -> f64 {
    let union = c1.visited.union(&c2.visited).count() as f64;
    let intersection = c1.visited.intersection(&c2.visited).count() as f64;
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

/// Sort a vector of candidates according to the user preference, the
/// hilliest/flattest route will become the first item in the vector
pub fn sort_candidates(
    candidates: &mut [Candidate],
    config: Arc<RouteConfig>,
) {
    // Note inverse comparison to sort in descending order
    candidates.sort_by(|a, b| get_cand_ordering(b, a, &config.route_mode));
}

/// For the selected threshold, check whether the provided candidate is below
/// the threshold for every candidate which has already been selected
fn check_if_candidate_is_dissimilar(
    candidate: &Candidate,
    selected: &[Candidate],
    threshold: &f64,
) -> bool {
    // https://www.sciencedirect.com/science/article/pii/S1319157817304512

    let cand_points = candidate.visited.len() as f64;

    for selected_candidate in selected.iter() {
        let sel_points = selected_candidate.visited.len() as f64;

        // Only do detailed check if routes are of approximately the same
        // length
        let most_points = f64::max(cand_points, sel_points);
        let least_points = f64::min(cand_points, sel_points);
        if (least_points < most_points * threshold)
            | (most_points > least_points * (2.0 - threshold))
        {
            return false;
        };

        // Compare scores
        let similarity = &get_similarity(candidate, selected_candidate);
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
    route_config: Arc<RouteConfig>,
    max_similarity: f64,
) -> Vec<Candidate> {
    // Nothing to do if count is already below target
    if candidates.len() <= target_count {
        return candidates.to_owned();
    }

    // Otherwise, sort candidates and prepare to select
    sort_candidates(candidates, Arc::clone(&route_config));

    // Take the first entry and use it as a seed for the output vector
    let split = candidates.split_at_mut(1);
    let mut selected: Vec<Candidate> = split.0.into();
    let mut to_process: Vec<Candidate> = split.1.into();

    // Start with similarity threshold of 0.7
    let mut threshold = 0.7;

    // Create a new vector to hold candidates not selected at current
    // threshold, just in case we need to increase it and try again
    let mut too_similar = Vec::<Candidate>::new();
    let mut target_met = false;

    // Keep going until required number of routes has been selected
    while (!target_met) & (!to_process.is_empty()) {
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

        // If required number of candidates has not been selected, increase
        // the threshold and try again
        if (!target_met) & (threshold < max_similarity) {
            to_process.append(&mut too_similar);
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
    max_cands: &usize,
    route_config: Arc<RouteConfig>,
    backend_config: Arc<BackendConfig>,
) -> Vec<Candidate> {
    // Nothing to do if already below target count
    if candidates.len() <= *max_cands {
        return candidates;
    }

    // Create equal size bins along lat & lon, creating a grid over the problem
    // space
    let bin_details = get_bin_details(max_cands, Arc::clone(&backend_config));
    let mut binned = bin_candidates(bin_details, candidates);

    // Create container for selected candidates, set target number of cands
    // to retain per bin
    let mut vec_selected: Vec<Vec<Candidate>> = Vec::new();
    let bin_target: usize = *max_cands / binned.len();

    // Sort according to user preference (hilliest/flattest), dropping
    // near-duplicates to ensure a good distribution
    binned
        .par_iter_mut()
        .map(|bin_cands| {
            get_dissimilar_routes(
                bin_cands,
                bin_target,
                Arc::clone(&route_config),
                backend_config.pruning_threshold,
            )
        })
        .collect_into_vec(&mut vec_selected);

    vec_selected.into_iter().flatten().collect()
}

// MARK: Tests

#[cfg(test)]
mod tests {

    use std::collections::HashSet;

    use petgraph::graph::NodeIndex;

    use crate::common::config::{BackendConfig, RouteConfig, RouteMode};
    use crate::common::routes::geometry::CandidateGeometry;
    use crate::common::routes::metrics::CandidateMetrics;

    use super::*;

    /// Quickly generate a valid RouteConfig option with some dummy data
    fn get_test_route_config() -> RouteConfig {
        RouteConfig {
            centre: (0.0, 0.0).into(),
            route_mode: RouteMode::Hilly,
            min_distance: 9000.0,
            max_distance: 10000.0,
            highways: vec!["highway_1".to_string()],
            surfaces: vec!["surface_1".to_string(), "surface_2".to_string()],
            surface_restriction: None,
            job_id: "42".to_string(),
        }
    }

    /// Quickly generate a valid Candidate with some dummy data
    fn get_test_candidate() -> Candidate {
        Candidate {
            points: Vec::new(),
            visited: HashSet::default(),
            geometry: CandidateGeometry::new(),
            metrics: CandidateMetrics::new(),
            route_config: Arc::new(get_test_route_config()),
            backend_config: Arc::new(BackendConfig::default(
                "dummy".to_string(),
                "dummy".to_string(),
            )),
            cur_inx: NodeIndex::new(0),
        }
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
        let test_config = Arc::new(get_test_route_config());

        // 1 == 2 < 3
        c1.metrics.common.dist = 10543.2;
        c2.metrics.common.dist = 10987.6;
        c3.metrics.common.dist = 11456.7;

        // 1 > 2 > 3
        c1.metrics.common.gain = 1300.0;
        c2.metrics.common.gain = 1200.0;
        c3.metrics.common.gain = 1100.0;

        // Longest first, then sorted by gain. Hilliest route first in output.
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

        // TODO: Fix this test, degree of overlap needs adjusting to
        //       reflect updated 70% similarity threshold (or set 0.5 as
        //       default and control via an environment variable)
        //       SET BOTH MIN AND MAX AS ARGS, PROVIDE AS ENV VARS!

        // Candidate 5 is the longest, and will be selected first
        for point in 0..10 {
            test_candidate_5.visited.insert(point);
        }
        test_candidate_5.metrics.common.dist = 11000.0;
        test_candidate_5.metrics.common.gain = 500.0;

        // Candidates 2 and 3 are very similar, 2 selected first due to gain
        // but 3 discarded
        for point in 10..20 {
            test_candidate_2.visited.insert(point.clone());
            test_candidate_3.visited.insert(point);
        }
        test_candidate_2.visited.insert(20);

        test_candidate_2.metrics.common.dist = 10000.0;
        test_candidate_3.metrics.common.dist = 10000.0;
        test_candidate_2.metrics.common.gain = 500.0;
        test_candidate_3.metrics.common.gain = 400.0;

        // Candidates 1 and 4 are different, 1 selected first due to gain
        // Over 50% similarity, but picked up on later iterations
        for point in 2..12 {
            test_candidate_1.visited.insert(point.clone());
            test_candidate_4.visited.insert(point);
        }
        test_candidate_1.visited.insert(13);
        test_candidate_4.visited.insert(14);

        test_candidate_1.metrics.common.dist = 10000.0;
        test_candidate_4.metrics.common.dist = 10000.0;
        test_candidate_1.metrics.common.gain = 300.0;
        test_candidate_4.metrics.common.gain = 200.0;

        let test_config = Arc::new(get_test_route_config());
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
    }

    #[test]
    fn test_prune_candidates() {
        // Skipping this for now, setup will need some thought and complex
        // logic is tested separately
    }
}
