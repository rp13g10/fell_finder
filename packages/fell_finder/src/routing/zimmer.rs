use indicatif::ProgressBar;
use rayon::prelude::*;
use std::cmp::{Ordering, min};
use std::env;
use std::sync::Arc;

use crate::common::config::RouteConfig;
use crate::common::graph_data::{EdgeData, NodeData};
use crate::routing::common::{
    Candidate, Route, RoutegenMetrics, RoutegenResponse, StepResult,
};
use petgraph::graph::{EdgeReference, NodeIndex};
use petgraph::visit::EdgeRef;
use petgraph::{Directed, Graph};

use crate::common::config::RouteMode;
use crate::routing::pruning::{get_dissimilar_routes, prune_candidates};

/// For a single candidate, determine all edges which can be reached and
/// check whether it is valid to do so
fn process_candidate(
    graph: &Graph<NodeData, EdgeData, Directed, u32>,
    candidate: &Candidate,
) -> Vec<StepResult> {
    let edges: Vec<EdgeReference<EdgeData>> =
        graph.edges(candidate.cur_inx).collect();
    let mut cand_results = Vec::<StepResult>::new();
    for eref in edges {
        let dst = eref.target();
        let ddata = graph
            .node_weight(dst)
            .expect("Destination doesn't exist in the graph!");
        cand_results.push(candidate.clone().take_step(&eref, &ddata));
    }
    cand_results
}

/// Process a vector of candidates, using Rayon to distribute processing
fn process_candidates_threads(
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

/// Fetch the user preference for number of routes to display. Defaults to 10
/// if the FF_MAX_ROUTES environment variable has not been set
fn get_num_routes_to_display() -> usize {
    let maybe_usr_pref = env::var("FF_MAX_ROUTES");

    match maybe_usr_pref {
        Ok(str) => match str.parse() {
            Ok(int) => int,
            Err(_) => 10,
        },
        Err(_) => 10,
    }
}

/// Fetch the desired similarity score to be used when selecting routes for
/// display in the webapp
fn get_max_similarity_for_display() -> f64 {
    let maybe_usr_pref = env::var("FF_DISPLAY_THRESHOLD");

    match maybe_usr_pref {
        Ok(str) => match str.parse() {
            Ok(float) => float,
            Err(_) => 0.9,
        },
        Err(_) => 0.9,
    }
}

/// Sets the desired maximum number of candidate routes for the current
/// iteration. For now, this is simply set to the number of edges in the
/// graph data, multiplied by the attempt number. Future builds may look to
/// refine this with a simple linear regression.
pub fn get_max_cands(
    graph: &Graph<NodeData, EdgeData, Directed, u32>,
    attempt: &u16,
    config: Arc<RouteConfig>,
) -> (usize, usize, usize) {
    let n_nodes = graph.node_count();
    let n_edges = graph.edge_count();
    let attempt = *attempt as usize;

    let max_cands = n_edges.clone() * attempt;

    // Apply global maximum
    let max_cands = min(max_cands, config.max_candidates);

    (n_nodes, n_edges, max_cands)
}

/// Recursive algorithm which crawls the provided graph for routes, starting at
/// the provided start_inx. This will attempt to return routes which meet all
/// of the criteria provided by the user
pub fn generate_routes(
    graph: Graph<NodeData, EdgeData, Directed, u32>,
    config: Arc<RouteConfig>,
    start_inx: NodeIndex,
    attempt: u16,
) -> RoutegenResponse {
    // Determine and set optimal number of candidates
    // TODO: Ensure max_cands propagates through to route creation
    let (n_nodes, n_edges, max_cands) =
        get_max_cands(&graph, &attempt, Arc::clone(&config));

    // Create 'seed' candidate
    let mut candidates: Vec<Candidate> = Vec::new();
    let seed = Candidate::new(&graph, Arc::clone(&config), &start_inx);
    candidates.push(seed);

    // Attempt route generation
    let bar = ProgressBar::new(config.max_distance as u64);

    let mut completed: Vec<Candidate> = Vec::new();
    let mut completed_buf: Vec<Candidate>;
    while !(candidates.is_empty()) {
        (candidates, completed_buf) =
            process_candidates_threads(&graph, candidates);
        completed.extend(completed_buf.into_iter());
        candidates =
            prune_candidates(candidates, &max_cands, Arc::clone(&config));

        let tot_dist = candidates
            .iter()
            .fold(0.0, |a, b| a + b.metrics.common.dist);
        let avg_dist = (tot_dist / candidates.len() as f64) as u64;
        bar.set_position(avg_dist);
    }

    bar.finish_and_clear();

    // If no routes generated, try again with higher max candidates
    let no_cands = candidates.into_iter().count() == 0;
    let attempts_remaining = attempt < 3;
    let not_at_global_max = max_cands < config.max_candidates;
    if no_cands & attempts_remaining & not_at_global_max {
        return generate_routes(graph, config, start_inx, attempt + 1);
    }

    let to_display = get_num_routes_to_display();
    let max_similarity = get_max_similarity_for_display();
    completed = get_dissimilar_routes(
        &mut completed,
        to_display,
        Arc::clone(&config),
        max_similarity,
    );

    let mut routes: Vec<Route> =
        completed.into_iter().map(|cand| cand.finalize()).collect();

    sort_routes(&mut routes, Arc::clone(&config));

    // Log other stats
    let n_routes = routes.iter().count(); // Post-filter, shows if requested N was met

    let routegen_metrics = RoutegenMetrics::new(
        n_nodes,
        n_edges,
        n_routes,
        max_cands.try_into().unwrap(),
        attempt.into(),
    );

    RoutegenResponse {
        routes: routes,
        metrics: routegen_metrics,
    }
}
