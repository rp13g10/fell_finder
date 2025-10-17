//! This module defines the logic which is used to reduce the total number of
//! candidates held in memory at any one time. This is required in order to
//! generate routes within a reasonable time-frame.

mod binning;
pub mod selecting;

use std::sync::Arc;

use rayon::prelude::*;

use crate::common::config::{BackendConfig, PruningStrategy, RouteConfig};
use crate::common::routes::Candidate;

use crate::pruning::binning::{bin_candidates, get_bin_details};
use crate::pruning::selecting::{
    get_best_routes_fuzzy, get_best_routes_naive,
};

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

    match backend_config.pruning_strategy {
        PruningStrategy::Fuzzy => {
            // Sort according to user preference (hilliest/flattest), dropping
            // near-duplicates to ensure a good distribution
            binned
                .par_iter_mut()
                .map(|bin_cands| {
                    get_best_routes_fuzzy(
                        bin_cands,
                        bin_target,
                        Arc::clone(&route_config),
                        backend_config.pruning_threshold,
                    )
                })
                .collect_into_vec(&mut vec_selected);

            vec_selected.into_iter().flatten().collect()
        }
        PruningStrategy::Naive => {
            binned
                .into_par_iter()
                .map(|bin_cands| {
                    get_best_routes_naive(
                        bin_cands,
                        bin_target,
                        Arc::clone(&route_config),
                    )
                })
                .collect_into_vec(&mut vec_selected);

            vec_selected.into_iter().flatten().collect()
        }
    }
}
