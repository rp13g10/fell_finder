use indicatif::ProgressBar;
use rayon::prelude::*;
use std::cmp::{Ordering, min};
use std::sync::Arc;

use crate::common::config::{BackendConfig, RouteConfig, RouteMode};
use crate::common::exceptions::RoutingError;
use crate::common::graph_data::{EdgeData, NodeData, TaggedGraph};
use crate::routing::common::{Candidate, Route, StepResult};
use petgraph::graph::EdgeReference;
use petgraph::visit::EdgeRef;
use petgraph::{Directed, Graph};

use crate::routing::pruning::{get_dissimilar_routes, prune_candidates};

/// For a single candidate, determine all edges which can be reached and
/// check whether it is valid to do so
fn process_candidate(
    graph: &Graph<NodeData, EdgeData, Directed, u32>,
    candidate: &Candidate,
) -> Vec<StepResult> {
    // Get all edges accessible from the current point
    let edges: Vec<EdgeReference<EdgeData>> =
        graph.edges(candidate.cur_inx).collect();

    // Check whether traversing these edges is valid
    let mut cand_results = Vec::<StepResult>::new();
    for eref in edges {
        let dst = eref.target();
        let maybe_ddata = graph.node_weight(dst);

        match maybe_ddata {
            Some(ddata) => {
                cand_results.push(candidate.clone().take_step(&eref, &ddata))
            }
            None => cand_results.push(StepResult::Invalid),
        };
    }
    cand_results
}

/// Process a vector of candidates, using Rayon to distribute processing
fn process_candidates(
    graph: &Graph<NodeData, EdgeData, Directed, u32>,
    candidates: Vec<Candidate>,
) -> (Vec<Candidate>, Vec<Candidate>) {
    let mut new_candidates: Vec<Candidate> = Vec::new();
    let mut completed: Vec<Candidate> = Vec::new();
    let mut results: Vec<Vec<StepResult>> = Vec::new();

    candidates
        .par_iter()
        .map(|candidate| process_candidate(graph, candidate))
        .collect_into_vec(&mut results);

    for result_vec in results {
        for result in result_vec {
            match result {
                StepResult::Complete(route) => completed.push(route),
                StepResult::Valid(cand) => {
                    new_candidates.push(cand);
                }
                StepResult::Invalid => {}
            }
        }
    }

    (new_candidates, completed)
}

/// Return the Ordering of candidate a relative to candidate b. Candidates are
/// sorted by distance in KMs, then by elevation gain
fn get_route_ordering(a: &Route, b: &Route, mode: &RouteMode) -> Ordering {
    let a_ratio = match mode {
        RouteMode::Hilly => a.metrics.common.gain / a.metrics.common.dist,
        RouteMode::Flat => {
            -1.0 * (a.metrics.common.gain / a.metrics.common.dist)
        }
    };
    let b_ratio = match mode {
        RouteMode::Hilly => b.metrics.common.gain / b.metrics.common.dist,
        RouteMode::Flat => {
            -1.0 * (b.metrics.common.gain / b.metrics.common.dist)
        }
    };

    match a_ratio.partial_cmp(&b_ratio) {
        Some(ordering) => ordering,
        None => Ordering::Equal,
    }
}

/// Sort a vector of routes according to the user preference, the
/// hilliest/flattest route will become the first item in the vector
pub fn sort_routes(routes: &mut Vec<Route>, config: Arc<RouteConfig>) {
    // Note inverse comparison to sort in descending order
    routes.sort_by(|a, b| get_route_ordering(b, a, &config.route_mode));
}

/// Sets the desired maximum number of candidate routes for the current
/// iteration. For now, this is simply set to the number of edges in the
/// graph data, multiplied by the attempt number. Future builds may look to
/// refine this with a simple linear regression.
pub fn get_max_cands(
    graph: &Graph<NodeData, EdgeData, Directed, u32>,
    attempt: &usize,
    config: Arc<BackendConfig>,
) -> usize {
    let n_edges = graph.edge_count();
    let attempt = *attempt as usize;

    let max_cands = n_edges.clone() * attempt;

    // Apply global maximum
    let max_cands = min(max_cands, config.max_candidates);

    max_cands
}

/// Recursive algorithm which crawls the provided graph for routes, starting at
/// the provided start_inx. This will attempt to return routes which meet all
/// of the criteria provided by the user
pub fn generate_routes(
    tagged_graph: TaggedGraph,
    route_config: Arc<RouteConfig>,
    backend_config: Arc<BackendConfig>,
    attempt: usize,
) -> Result<Vec<Route>, RoutingError> {
    // Determine and set optimal number of candidates
    let max_cands = get_max_cands(
        &tagged_graph.graph,
        &attempt,
        Arc::clone(&backend_config),
    );

    // Create 'seed' candidate
    let mut candidates: Vec<Candidate> = Vec::new();
    let seed = Candidate::new(
        &tagged_graph.graph,
        Arc::clone(&route_config),
        Arc::clone(&backend_config),
        &tagged_graph.start_inx,
    );
    candidates.push(seed);

    // Attempt route generation
    let bar = ProgressBar::new(route_config.max_distance as u64);

    let mut completed: Vec<Candidate> = Vec::new();
    let mut completed_buf: Vec<Candidate>;
    while !(candidates.is_empty()) {
        (candidates, completed_buf) =
            process_candidates(&tagged_graph.graph, candidates);
        completed.extend(completed_buf.into_iter());
        candidates = prune_candidates(
            candidates,
            &max_cands,
            Arc::clone(&route_config),
            Arc::clone(&backend_config),
        );

        let tot_dist = candidates
            .iter()
            .fold(0.0, |a, b| a + b.metrics.common.dist);
        let avg_dist = (tot_dist / candidates.len() as f64) as u64;
        bar.set_position(avg_dist);
    }

    bar.finish_and_clear();

    // If no completed candidates, try again with higher max candidates
    let no_cands = candidates.into_iter().count() == 0;
    let attempts_remaining = attempt < 3;
    let not_at_global_max = max_cands < backend_config.max_candidates;
    if no_cands & attempts_remaining & not_at_global_max {
        return generate_routes(
            tagged_graph,
            route_config,
            backend_config,
            attempt + 1,
        );
    }

    completed = get_dissimilar_routes(
        &mut completed,
        backend_config.max_routes,
        Arc::clone(&route_config),
        backend_config.display_threshold,
    );

    let mut routes: Vec<Route> = completed
        .into_iter()
        .filter_map(|cand| match cand.finalize() {
            Ok(route) => Some(route),
            _ => None,
        })
        .collect();

    sort_routes(&mut routes, Arc::clone(&route_config));

    // If no completed routes, try again with higher max candidates
    let no_routes = routes.iter().count() == 0;
    if no_routes & attempts_remaining & not_at_global_max {
        return generate_routes(
            tagged_graph,
            route_config,
            backend_config,
            attempt + 1,
        );
    } else if no_routes {
        return Err(RoutingError::NoRoutesError);
    }

    Ok(routes)
}

// TODO: Write tests for this module
