use std::sync::Arc;

use indicatif::ProgressBar;
use rayon::prelude::*;

use crate::common::config::RouteConfig;
use crate::common::graph_data::{EdgeData, NodeData};
use crate::routing::common::{Candidate, Route, StepResult};
use petgraph::graph::{EdgeReference, NodeIndex};
use petgraph::visit::EdgeRef;
use petgraph::{Directed, Graph};

use crate::routing::pruning::{
    get_dissimilar_routes, prune_candidates, sort_candidates,
};

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

/// Recursive algorithm which crawls the provided graph for routes, starting at
/// the provided start_inx. This will attempt to return routes which meet all
/// of the criteria provided by the user
pub fn generate_routes(
    graph: Graph<NodeData, EdgeData, Directed, u32>,
    config: RouteConfig,
    start_inx: NodeIndex,
) -> Vec<Route> {
    // Config needs to be shared across all candidates
    let shared_config = Arc::new(config);

    // Create 'seed' candidate
    let mut candidates: Vec<Candidate> = Vec::new();
    let seed = Candidate::new(&graph, Arc::clone(&shared_config), &start_inx);
    candidates.push(seed);

    // let mut counter: usize = 0;

    let bar = ProgressBar::new(shared_config.max_distance as u64);

    let mut completed: Vec<Candidate> = Vec::new();
    let mut completed_buf: Vec<Candidate>;
    while !(candidates.is_empty()) {
        (candidates, completed_buf) =
            process_candidates_threads(&graph, candidates);
        completed.extend(completed_buf.into_iter());
        candidates = prune_candidates(candidates, Arc::clone(&shared_config));

        let tot_dist = candidates
            .iter()
            .fold(0.0, |a, b| a + b.metrics.common.dist);
        let avg_dist = (tot_dist / candidates.len() as f64) as u64;
        bar.set_position(avg_dist);

        // Early stopping for profiling
        // counter += 1;
        // if counter == 32 {
        //     return completed;
        // }
    }

    bar.finish();

    completed =
        get_dissimilar_routes(&mut completed, 25, Arc::clone(&shared_config));
    sort_candidates(&mut completed, Arc::clone(&shared_config));

    let routes: Vec<Route> =
        completed.into_iter().map(|cand| cand.finalize()).collect();

    routes
}
