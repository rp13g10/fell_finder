//! This module defines the logic which is used to split candidate routes
//! across multiple 'bins'. When determining which routes to keep, the
//! sort operation is carried out inside each bin. The intention is that this
//! will result in a better geographic spread of generated routes vs. simply
//! sorting by ratio and taking the best N.

use std::cmp::Ordering;
use std::sync::Arc;

use rayon::prelude::*;

use crate::common::config::{BackendConfig, BinStrategy};
use crate::common::routes::Candidate;

#[derive(Eq, PartialEq, Debug)]
pub struct BinDetails {
    bin_size_x: usize,
    bin_size_y: usize,
}

/// Based on the number of candidates expected to be in memory when route
/// pruning takes place, determine the number of bins required in order for
/// each bin to have the desired size
pub fn get_bin_details(
    max_cands: &usize,
    backend_config: Arc<BackendConfig>,
) -> BinDetails {
    // Fetch target bin size
    let bin_size: usize = backend_config.bin_size;

    // Total number of bins which will be required
    let num_bins = (*max_cands as f64 / bin_size as f64).ceil();

    // Set x bins based on square root of total, then determine number of y
    // bins required to get to the desired total
    let x_bins = (num_bins.sqrt()).ceil();
    let y_bins = (num_bins / x_bins).ceil();

    BinDetails {
        // Each x bin will be subdivided into y bins before use
        bin_size_x: bin_size * (y_bins as usize),
        bin_size_y: bin_size,
    }
}

// TODO: Use a macro to combine functionality of these two functions, if
//       you're feeling brave enough

fn compare_points(maybe_c1: Option<f64>, maybe_c2: Option<f64>) -> Ordering {
    // Define behaviour if one value is missing
    let (c1_pos, c2_pos) = match maybe_c1 {
        Some(c1_pos) => match maybe_c2 {
            Some(c2_pos) => (c1_pos, c2_pos),
            None => return Ordering::Greater,
        },
        None => return Ordering::Equal,
    };

    // Standard comparison
    if c1_pos < c2_pos {
        Ordering::Less
    } else if c1_pos > c2_pos {
        Ordering::Greater
    } else {
        Ordering::Equal
    }
}

fn get_lat_for_comparison(cand: &Candidate) -> Option<f64> {
    match cand.backend_config.bin_strategy {
        BinStrategy::Last => match cand.geometry.get_current_position() {
            Some((lat, _)) => Some(lat),
            None => None,
        },
        BinStrategy::Centre => match cand.geometry.get_midpoint_position() {
            Some((lat, _)) => Some(lat),
            None => None,
        },
    }
}

/// For 2 candidates, compare their current latitudes
fn compare_lats(c1: &Candidate, c2: &Candidate) -> Ordering {
    let maybe_c1 = get_lat_for_comparison(c1);
    let maybe_c2 = get_lat_for_comparison(c2);

    compare_points(maybe_c1, maybe_c2)
}

fn get_lon_for_comparison(cand: &Candidate) -> Option<f64> {
    match cand.backend_config.bin_strategy {
        BinStrategy::Last => match cand.geometry.get_current_position() {
            Some((_, lon)) => Some(lon),
            None => None,
        },
        BinStrategy::Centre => match cand.geometry.get_midpoint_position() {
            Some((_, lon)) => Some(lon),
            None => None,
        },
    }
}

/// For 2 candidates, compare their current longitudes
fn compare_lons(c1: &Candidate, c2: &Candidate) -> Ordering {
    let maybe_c1 = get_lon_for_comparison(c1);
    let maybe_c2 = get_lon_for_comparison(c2);

    compare_points(maybe_c1, maybe_c2)
}

/// Assign all of the provided routes to bins based on their central
/// coordinates. Routes are binned across both lats and lons, analagous
/// to placing them into a 2d grid.
pub fn bin_candidates(
    bin_details: BinDetails,
    mut candidates: Vec<Candidate>,
) -> Vec<Vec<Candidate>> {
    // First, bin by latitude
    candidates.par_sort_by(compare_lats);
    let x_bins = candidates.par_chunks_mut(bin_details.bin_size_x);

    // Then, bin each of those bins by longitude
    let mut par_bins: Vec<Vec<Vec<Candidate>>> = Vec::new();
    x_bins
        .map(|x_bin| {
            x_bin.sort_by(compare_lons);
            let x_y_bins: Vec<Vec<Candidate>> = x_bin
                .chunks(bin_details.bin_size_y)
                .map(|x_y_bin| x_y_bin.to_vec())
                .collect();
            x_y_bins
        })
        .collect_into_vec(&mut par_bins);

    par_bins.into_iter().flatten().collect()
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
            start_inx: NodeIndex::new(0),
        }
    }

    #[cfg(test)]
    mod test_get_bin_details {
        use super::*;

        #[test]
        fn test_square_number() {
            let max_cands = 1024; // 64 * 4 * 4
            let cfg = Arc::new(BackendConfig::default(
                "dummy".to_string(),
                "dummy".to_string(),
            ));

            let target = BinDetails {
                bin_size_x: 256,
                bin_size_y: 64,
            };

            let result = get_bin_details(&max_cands, cfg);

            assert_eq!(result, target);
        }

        #[test]
        fn test_no_leftovers() {
            let max_cands = 1280; // 64 * 5 * 4
            let cfg = Arc::new(BackendConfig::default(
                "dummy".to_string(),
                "dummy".to_string(),
            ));

            // --> num_bins = 20
            // --> x_bins = 5, y_bins = 4

            let target = BinDetails {
                bin_size_x: 256,
                bin_size_y: 64,
            };

            let result = get_bin_details(&max_cands, cfg);

            assert_eq!(result, target);
        }

        #[test]
        fn test_leftovers() {
            let max_cands = 5432;
            let cfg = Arc::new(BackendConfig::default(
                "dummy".to_string(),
                "dummy".to_string(),
            ));

            // --> num_bins = 85 (5432 / 64 = 84.875)
            // --> x_bins = 10 (sqrt(85) = 9.22)
            // --> y_bins = 9 (85 / 10 = 8.5)

            let target = BinDetails {
                bin_size_x: 576,
                bin_size_y: 64,
            };

            let result = get_bin_details(&max_cands, cfg);

            assert_eq!(result, target);
        }
    }

    #[cfg(test)]
    mod test_compare_coords {
        use super::*;

        #[test]
        fn test_less() {
            let mut test_candidate_1 = get_test_candidate();
            let mut test_candidate_2 = get_test_candidate();

            test_candidate_1.geometry.lats.push(0.0);
            test_candidate_1.geometry.lons.push(0.0);

            test_candidate_2.geometry.lats.push(1.0);
            test_candidate_2.geometry.lons.push(1.0);

            let target = Ordering::Less;

            let result_lat =
                compare_lats(&test_candidate_1, &test_candidate_2);
            let result_lon =
                compare_lons(&test_candidate_1, &test_candidate_2);

            assert_eq!(result_lat, target);
            assert_eq!(result_lon, target);
        }

        #[test]
        fn test_greater() {
            let mut test_candidate_1 = get_test_candidate();
            let mut test_candidate_2 = get_test_candidate();

            test_candidate_1.geometry.lats.push(1.0);
            test_candidate_1.geometry.lons.push(1.0);

            test_candidate_2.geometry.lats.push(0.0);
            test_candidate_2.geometry.lons.push(0.0);

            let target = Ordering::Greater;

            let result_lat =
                compare_lats(&test_candidate_1, &test_candidate_2);
            let result_lon =
                compare_lons(&test_candidate_1, &test_candidate_2);

            assert_eq!(result_lat, target);
            assert_eq!(result_lon, target);
        }

        #[test]
        fn test_equal() {
            let mut test_candidate_1 = get_test_candidate();
            let mut test_candidate_2 = get_test_candidate();

            test_candidate_1.geometry.lats.push(0.0);
            test_candidate_1.geometry.lons.push(0.0);

            test_candidate_2.geometry.lats.push(0.0);
            test_candidate_2.geometry.lons.push(0.0);

            let target = Ordering::Equal;

            let result_lat =
                compare_lats(&test_candidate_1, &test_candidate_2);
            let result_lon =
                compare_lons(&test_candidate_1, &test_candidate_2);

            assert_eq!(result_lat, target);
            assert_eq!(result_lon, target);
        }
    }

    /// Within a single bin, get the min & max coordinate. For tests lat and
    /// lon are always set to the same value, so we can safely combine them
    /// into a single list
    fn get_bin_min_max(bin_cands: &Vec<Candidate>) -> (f64, f64) {
        let mut all_points: Vec<f64> = Vec::new();
        for cand in bin_cands {
            all_points.extend(cand.geometry.lats.clone());
            all_points.extend(cand.geometry.lons.clone());
        }

        let bin_min = all_points
            .clone()
            .into_iter()
            .reduce(|x, y| f64::min(x, y))
            .unwrap();
        let bin_max = all_points
            .into_iter()
            .reduce(|x, y| f64::max(x, y))
            .unwrap();
        (bin_min, bin_max)
    }

    #[test]
    fn test_bin_candidates() {
        // Target: 512 candidates, 16 bins (4x4) of 32 candidates each
        let test_bin_details = BinDetails {
            bin_size_x: 128, // 4 x 32
            bin_size_y: 32,
        };

        // Generate 512 candidates, evenly spread over an 8x8 grid
        let mut test_candidates: Vec<Candidate> = Vec::new();
        for lat in 0..8 {
            for lon in 0..8 {
                for _ in 0..8 {
                    let mut point_cand = get_test_candidate();
                    point_cand.geometry.lats = vec![lat as f64];
                    point_cand.geometry.lons = vec![lon as f64];
                    test_candidates.push(point_cand)
                }
            }
        }

        // Set up target properties
        let target_n_bins = 16;
        let target_bin_size = 32;

        let target_first_bin_min = 0.0; // eq
        let target_first_bin_max = 2.0; // lt
        let target_last_bin_min = 6.0; // eq
        let target_last_bin_max = 8.0; // lt

        // Act
        let result = bin_candidates(test_bin_details, test_candidates);

        // Correct number of bins generated
        assert!(result.len() == target_n_bins);

        // Check contents of bins
        let mut n_bins_matching_first = 0;
        let mut n_bins_matching_last = 0;
        for bin in result {
            // All bins are of the expected size
            assert!(bin.len() == target_bin_size);

            // Only one bin matches the range of values expected for the first
            // and last bins. i.e. the bins do not overlap, and have the
            // expected values
            let (bin_min, bin_max) = get_bin_min_max(&bin);

            let in_first = (bin_min >= target_first_bin_min)
                & (bin_min < target_first_bin_max)
                & (bin_max >= target_first_bin_min)
                & (bin_max < target_first_bin_max);
            let in_last = (bin_min >= target_last_bin_min)
                & (bin_min < target_last_bin_max)
                & (bin_max >= target_last_bin_min)
                & (bin_max < target_last_bin_max);

            match in_first {
                true => n_bins_matching_first += 1,
                false => {}
            }

            match in_last {
                true => n_bins_matching_last += 1,
                false => {}
            }
        }

        assert_eq!(n_bins_matching_first, 1);
        assert_eq!(n_bins_matching_last, 1);
    }
}
