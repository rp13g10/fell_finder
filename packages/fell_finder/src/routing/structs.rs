pub mod geometry;
pub mod metrics;

use crate::routing::structs::geometry::{CandidateGeometry, RouteGeometry};
use crate::routing::structs::metrics::{CandidateMetrics, RouteMetrics};

use petgraph::graph::{EdgeReference, NodeIndex};
use petgraph::visit::EdgeRef;
use petgraph::{Directed, Graph};

use rustc_hash::FxHashSet;
use serde::Serialize;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use crate::common::config::RouteConfig;
use crate::common::graph_data::{EdgeData, NodeData};

/// Container for a single candidate route
#[derive(Clone)]
pub struct Candidate {
    // Both points and visited are tracked using identifiers from OSM, as these
    // are stored as data attributes of nodes/edges
    pub points: Vec<i64>,
    pub visited: FxHashSet<i64>,
    pub geometry: CandidateGeometry,
    pub metrics: CandidateMetrics,
    config: Arc<RouteConfig>,
    pub cur_inx: NodeIndex,
}

pub enum StepResult {
    Valid(Candidate),
    Complete(Candidate),
    Invalid,
}

impl Candidate {
    pub fn new(
        graph: &Graph<NodeData, EdgeData, Directed, u32>,
        config: Arc<RouteConfig>,
        start_inx: &NodeIndex,
    ) -> Candidate {
        let start_id = graph
            .node_weight(*start_inx)
            .expect("Provided start node isn't in the graph!")
            .id;

        let mut visited = FxHashSet::<i64>::default();
        visited.insert(start_id);

        Candidate {
            points: vec![start_id],
            visited: visited,
            geometry: CandidateGeometry::new(),
            metrics: CandidateMetrics::new(),
            config: config,
            cur_inx: start_inx.clone(),
        }
    }

    pub fn finalize(self) -> Route {
        let mut hasher = DefaultHasher::new();
        self.points.hash(&mut hasher);
        let id = hasher.finish();

        Route {
            geometry: self.geometry.finalize(),
            metrics: self.metrics.finalize(),
            id: id,
        }
    }

    fn check_for_overlap(&self, eref: &EdgeReference<EdgeData>) -> bool {
        // TODO: Enable configuration of max no. overlaps

        let edata = eref.weight();

        // Completion of a route is always allowed
        match self.points.get(0) {
            Some(start) => {
                if &edata.dst == start {
                    return false;
                } else {
                    ();
                }
            }
            // If there is no 0th point, the candidate has not been set up
            // properly
            None => panic!(
                "Trying to take a step, but this candidate has no points"
            ),
        }

        // Route is almost finished. Trying to get to one of first 3 nodes, has
        // not visited that node in the last 3 loops
        let mut nearly_loop = false;
        let almost_finished =
            self.metrics.common.dist >= self.config.min_distance;
        if (self.points.len() >= 3) & almost_finished {
            let first_three = self.points[..3].to_vec();
            let last_three = self.points[self.points.len() - 3..].to_vec();

            let close_to_start = first_three.contains(&edata.dst);
            let not_backtracking = !last_three.contains(&edata.dst);

            nearly_loop = close_to_start & not_backtracking;
            // println!("Route is nearly a loop");
        }

        if !nearly_loop {
            self.visited.contains(&edata.dst)
        } else {
            false
        }
    }

    fn sum_surfaces_distance(&self, surfaces: &Vec<String>) -> f64 {
        let mut dist = 0.0;
        for surface in surfaces {
            match self.metrics.common.s_dists.get(surface) {
                Some(s_dist) => dist += s_dist,
                None => (),
            }
        }
        dist
    }

    fn validate_route_completion(&self) -> bool {
        let rest_pass = match &self.config.surface_restriction {
            // TODO: Move this into validation during route creation, route may
            //       go over the limit before completion
            Some(restriction) => {
                let s_dist = self
                    .sum_surfaces_distance(&restriction.restricted_surfaces);
                let max_s_dist = restriction.restricted_surfaces_perc
                    * self.config.max_distance;
                if s_dist > max_s_dist { true } else { false }
            }
            None => true,
        };

        match rest_pass {
            true => {
                // println!(
                //     "Circular candidate has distance {:?}",
                //     self.metrics.common.dist
                // );
                (self.metrics.common.dist <= self.config.max_distance)
                    & (self.metrics.common.dist >= self.config.min_distance)
            }
            false => false,
        }
    }

    pub fn take_step(mut self, eref: &EdgeReference<EdgeData>) -> StepResult {
        let edata = eref.weight();

        // let to_avoid = self.get_nodes_to_avoid(graph, eref);

        // Pre-validation -----------------------------------------------------
        // - Node has not already been visited
        if self.check_for_overlap(eref) {
            // println!("Step overlaps with visited nodes");
            return StepResult::Invalid;
        }

        // - Possible to get back without going over max distance. Note that
        //   a more robust solution would be to use shortest path on a filtered
        //   view of the graph, but it has been determined that the performance
        //   impact is too high for the limited improvement in completion rates
        //   it provides
        if self.metrics.common.dist > self.config.max_distance {
            return StepResult::Invalid;
        }

        // Step ---------------------------------------------------------------
        self.geometry.take_step(edata);
        self.metrics.take_step(edata);
        self.visited.insert(edata.dst);
        self.points.push(edata.dst);
        self.cur_inx = eref.target();

        // Post-validation ----------------------------------------------------

        // Check if circular routes meet user criteria
        if self.points[0] == edata.dst {
            match self.validate_route_completion() {
                true => StepResult::Complete(self),
                false => StepResult::Invalid,
            }
        } else {
            StepResult::Valid(self)
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Route {
    geometry: RouteGeometry,
    pub metrics: RouteMetrics,
    pub id: u64,
}
