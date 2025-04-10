use rustc_hash::FxHashMap;
use std::cmp::{Ordering, min};
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::io::{Error, ErrorKind};
use std::iter::zip;
use std::sync::Arc;

use rayon::prelude::*;

use crate::common::config::RouteConfig;
use crate::routing::structs::Candidate;

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
    // TODO: Set this to pull from config file
    let n_bins = 4;

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

    // println!("Candidates binned into: {:?}", binned.keys());

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

// Sort candidates by distance (to the nearest km), then gain (to the nearest
// 10m)
fn get_cand_ordering(a: &Candidate, b: &Candidate) -> Ordering {
    let a_data = (
        a.metrics.common.dist as usize,
        (a.metrics.common.gain / 10.0) as usize,
    );

    let b_data = (
        b.metrics.common.dist as usize,
        (b.metrics.common.gain / 10.0) as usize,
    );

    a_data.cmp(&b_data)
}

/// Retain only sufficiently different routes, the similarity threshold will be
/// set dynamically in order to reach the target count of routes
pub fn get_dissimilar_routes(
    candidates: &mut Vec<Candidate>,
    target_count: usize,
) -> Vec<Candidate> {
    // TODO: Set reversing behavior based on user config, must be possible to
    //       adjust sort_by to do this

    // TODO: Find a way to improve performance, this feels like it has too
    //       many if statements and loops

    // TODO: Enable use of unchanged candidate vector if it contains fewer than
    //       the requested number of routes. Set this up to return an enum
    //       and match the contents? NoChange | Change perhaps.
    if candidates.len() <= target_count {
        return candidates.drain(..).collect();
    }

    let mut dissimilar = Vec::<Candidate>::new();

    candidates.sort_by(|a, b| get_cand_ordering(a, b));

    let mut threshold = 0.5;
    let mut target_met = false;

    // Pop takes last item, list is sorted in ascending order so this is the
    // hilliest route
    match Vec::pop(candidates) {
        Some(candidate) => dissimilar.push(candidate),
        None => return dissimilar,
    }

    // sort_by will have sorted in ascending order, we want to iterate in
    // descending order
    candidates.reverse();

    while (!target_met) & (candidates.len() > 0) {
        // Create a new vector to hold candidates not selected at current
        // threshold, just in case we need to increase it and try again
        let mut too_similar = Vec::<Candidate>::new();

        // Compare every candidate to the ones already selected, keep them if
        // they are sufficiently different from all other selected routes
        for candidate in candidates.drain(..) {
            let mut accepted = true;
            for selected_candidate in dissimilar.iter() {
                // Compare scores
                let similarity =
                    get_similarity(&candidate, &selected_candidate);
                if similarity > threshold {
                    accepted = false;
                    break;
                }
            }
            if accepted {
                dissimilar.push(candidate);
                if dissimilar.len() == target_count {
                    target_met = true;
                    break;
                }
            } else {
                too_similar.push(candidate)
            }
        }
        candidates.extend(too_similar);
        threshold += 0.1;
    }

    dissimilar
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
        .map(|bin_cands| get_dissimilar_routes(bin_cands, bin_target))
        .collect_into_vec(&mut vec_selected);

    vec_selected.into_iter().flatten().collect()
}
