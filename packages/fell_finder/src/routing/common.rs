pub mod geometry;
pub mod metrics;

use crate::routing::common::geometry::{CandidateGeometry, RouteGeometry};
use crate::routing::common::metrics::{CandidateMetrics, RouteMetrics};

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
#[derive(Clone, Debug, PartialEq)]
pub struct Candidate {
    // Both points and visited are tracked using identifiers from OSM, as these
    // are stored as data attributes of nodes/edges
    pub points: Vec<i64>,
    pub visited: FxHashSet<i64>,
    pub geometry: CandidateGeometry,
    pub metrics: CandidateMetrics,
    pub config: Arc<RouteConfig>,
    pub cur_inx: NodeIndex,
}

/// Defines the 3 possible outcome states of a candidate after a step has been
/// taken
pub enum StepResult {
    Valid(Candidate),
    Complete(Candidate),
    Invalid,
}

impl Candidate {
    /// Create a new Candidate route, starting at the provided start_inx. An
    /// Arc pointing to the route config is stored internally for reference
    /// in other operations, and the graph which the NodeIndex points to must
    /// be provided so that the ID of the corresponding node can be stored
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

    /// Once a candidate has formed a circular route, it can be finalized. This
    /// returns a Route object, which drops any data which is only required
    /// during the route finding process
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

    /// Check whether stepping along the provided edge reference would result
    /// in the route overlapping with itself. To prevent routes from simply
    /// finding the nearest hill and going up/down it repeatedly, this check
    /// is required. Exceptions are made when the route is nearly completed,
    /// as this improves the overall completion rate
    fn check_for_overlap(&self, eref: &EdgeReference<EdgeData>) -> bool {
        // TODO: Enable configuration of max no. overlaps
        // TODO: Set up error propagation so that this can factor in to the
        //       returned error code

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
        }

        if !nearly_loop {
            self.visited.contains(&edata.dst)
        } else {
            false
        }
    }

    /// For a provided vector of surfaces (as they appear in the OSM dataset),
    /// calculate the total distance which the candidate has travelled along
    /// them. This is required in order to determine whether a route has
    /// exceeded the user-provided surface restrictions
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

    /// Once a circular route has been formed, check it against all of the
    /// user-provided restrictions (target distance, surface restrictions)
    /// and determine whether all requirements have been met
    fn validate_route_completion(&self) -> bool {
        let rest_pass = match &self.config.surface_restriction {
            // TODO: Move this into validation during route creation, route may
            //       go over the limit before completion
            Some(restriction) => {
                let s_dist = self
                    .sum_surfaces_distance(&restriction.restricted_surfaces);
                let max_s_dist = restriction.restricted_surfaces_perc
                    * self.config.max_distance;
                if s_dist > max_s_dist { false } else { true }
            }
            None => true,
        };

        match rest_pass {
            true => {
                (self.metrics.common.dist <= self.config.max_distance)
                    & (self.metrics.common.dist >= self.config.min_distance)
            }
            false => false,
        }
    }

    /// Update the current candidate based on stepping along the provided
    /// edge, to the provided destination node. A StepResult will be returned
    /// based on the outcome of this operation
    pub fn take_step(
        mut self,
        eref: &EdgeReference<EdgeData>,
        ddata: &NodeData,
    ) -> StepResult {
        let edata = eref.weight();

        // TODO: UPDATE THIS TO TAKE DESTINATION NODE, CHECK DISTANCE TO START
        //       AGAINST CURRENT DISTANCE

        // Pre-validation -----------------------------------------------------
        // Check if node has already been visited
        if self.check_for_overlap(eref) {
            return StepResult::Invalid;
        }

        // Determine the minimum possible distance of a completed route after
        // taking the proposed step
        let dist_to_start = match ddata.dist_to_start {
            Some(dist) => dist,
            None => {
                if ddata.is_start {
                    0.0
                } else {
                    return StepResult::Invalid;
                }
            } // No path back to start
        };
        let min_complete_dist =
            self.metrics.common.dist + edata.distance + dist_to_start;

        // Check that it's Possible to get back without going over max
        // distance. Note that a more robust solution would be to use shortest
        // path on a filtered view of the graph, but it has been determined
        // that the performance impact is too high for the limited improvement
        // in completion rates it provides
        if min_complete_dist > self.config.max_distance {
            return StepResult::Invalid;
        }

        // Step ---------------------------------------------------------------

        self.metrics.take_step(edata);
        self.geometry.take_step(edata);
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

/// Minimal container for a completed route. This holds only the information
/// required by the webapp in order to render it.
#[derive(Debug, Serialize)]
pub struct Route {
    geometry: RouteGeometry,
    pub metrics: RouteMetrics,
    pub id: u64,
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::common::config::{RouteMode, SurfaceRestriction};
    use petgraph::graph::EdgeIndex;
    use rustc_hash::FxHashMap;

    /// Generate a basic RouteConfig which can be used for testing
    fn get_test_config() -> RouteConfig {
        RouteConfig {
            centre: (0.0, 0.0).into(),
            route_mode: RouteMode::Hilly,
            max_candidates: 1,
            min_distance: 2.0,
            max_distance: 3.0,
            highways: vec!["highway".to_string()],
            surfaces: vec!["surface".to_string()],
            surface_restriction: None,
        }
    }

    /// Generate a basic Candidate which can be used for testing. Visited
    /// points are all integers between 0 and 9 inclusive.
    fn get_test_candidate(config: Option<RouteConfig>) -> Candidate {
        let test_points = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let mut test_visited = FxHashSet::<i64>::default();
        for point in test_points.iter() {
            test_visited.insert(point.clone());
        }

        let test_config = match config {
            Some(usrconf) => Arc::new(usrconf),
            None => Arc::new(get_test_config()),
        };

        Candidate {
            points: test_points,
            visited: test_visited,
            config: test_config,
            geometry: CandidateGeometry::new(),
            metrics: CandidateMetrics::new(),
            cur_inx: NodeIndex::new(0),
        }
    }

    /// Generate a very basic graph with a single edge, which can be used
    /// to generate an EdgeReference. This seems to be necessary as it's
    /// not possible to directly instantiate an EdgeReference due to
    /// private attributes, and no constructor methods are provided.
    fn get_test_graph_for_edge(
        src: i64,
        dst: i64,
        edge_dist: Option<f64>,
        dist_to_start: Option<f64>,
        is_start: bool,
    ) -> (
        NodeIndex<u32>,
        NodeIndex<u32>,
        EdgeIndex<u32>,
        Graph<NodeData, EdgeData, Directed, u32>,
    ) {
        let edge_dist = match edge_dist {
            Some(usrdist) => usrdist,
            None => 2.0,
        };

        let edge_data = EdgeData {
            // Dummy data with correct src/dst
            src: src,
            dst: dst,
            highway: "highway".to_string(),
            surface: "surface".to_string(),
            elevation_gain: 0.0,
            elevation_loss: 1.0,
            distance: edge_dist,
            lats: vec![3.0, 4.0],
            lons: vec![5.0, 6.0],
            eles: vec![7.0, 8.0],
            dists: vec![9.0, 10.0],
        };

        // Sample nodes for provided src/dst
        let src_data = NodeData {
            id: src,
            lat: 3.0,
            lon: 5.0,
            elevation: 7.0,
            is_start: true,
            dist_to_start: None,
        };
        let dst_data = NodeData {
            id: dst,
            lat: 4.0,
            lon: 6.0,
            elevation: 8.0,
            is_start: is_start,
            dist_to_start: dist_to_start,
        };

        // Generate the graph
        let mut graph = Graph::<NodeData, EdgeData, Directed, u32>::new();

        // Add the nodes & edge
        let src = graph.add_node(src_data);
        let dst = graph.add_node(dst_data);
        let edge = graph.add_edge(src, dst, edge_data);

        (src, dst, edge, graph)
    }

    /// Check that when a new candidate is created, the route is correctly
    /// seeded with details of the provided start point
    #[test]
    fn test_new_candidate() {
        // Arrange
        // Test data
        let mut test_graph = Graph::<NodeData, EdgeData, Directed, u32>::new();

        let test_ndata1 = NodeData {
            id: 0,
            lat: 1.0,
            lon: 2.0,
            elevation: 3.0,
            is_start: true,
            dist_to_start: None,
        };
        let test_ndata2 = NodeData {
            id: 4,
            lat: 5.0,
            lon: 6.0,
            elevation: 7.0,
            is_start: false,
            dist_to_start: Some(8.0),
        };

        let test_index = test_graph.add_node(test_ndata1);
        let _ = test_graph.add_node(test_ndata2);

        let test_config = Arc::new(get_test_config());

        // Target data

        let mut target_visited = FxHashSet::<i64>::default();
        target_visited.insert(0);

        let target = Candidate {
            points: vec![0],
            visited: target_visited,
            geometry: CandidateGeometry::new(),
            metrics: CandidateMetrics::new(),
            config: test_config.clone(),
            cur_inx: test_index.clone(),
        };

        // Act
        let result =
            Candidate::new(&test_graph, test_config.clone(), &test_index);

        // Assert
        assert_eq!(result, target);
    }

    #[cfg(test)]
    mod test_check_for_overlap {

        use super::*;

        /// Check that attempting to check for overlapping points on an empty
        /// candidate causes the programme to panic. In future this will be
        /// updated to return a descriptive error to the API endpoint.
        #[test]
        #[should_panic(expected = "this candidate has no points")]
        fn test_panic() {
            let test_config = Arc::new(get_test_config());
            let test_candidate = Candidate {
                points: Vec::<i64>::new(),
                visited: FxHashSet::<i64>::default(),
                config: test_config,
                geometry: CandidateGeometry::new(),
                metrics: CandidateMetrics::new(),
                cur_inx: NodeIndex::new(0),
            };
            let (test_src, test_dst) = (20, 21);

            let (src, _, _, graph) =
                get_test_graph_for_edge(test_src, test_dst, None, None, false);

            let erefs: Vec<EdgeReference<EdgeData>> =
                graph.edges(src).collect();
            let eref = erefs.get(0).unwrap();

            let _ = test_candidate.check_for_overlap(eref);
        }

        /// Check that attempting to step back to a previously visited node
        /// trips the overlap check
        #[test]
        fn test_true() {
            let test_candidate = get_test_candidate(None);

            // Current pos is 9, this would be a step backwards
            let (test_src, test_dst) = (9, 8);

            let (src, _, _, graph) =
                get_test_graph_for_edge(test_src, test_dst, None, None, false);

            let erefs: Vec<EdgeReference<EdgeData>> =
                graph.edges(src).collect();
            let eref = erefs.get(0).unwrap();

            assert!(test_candidate.check_for_overlap(eref))
        }

        /// Check that attempting to step to an unvisited node does not trip
        /// the overlap check
        #[test]
        fn test_false_std() {
            let test_candidate = get_test_candidate(None);

            // Current pos is 9, this would be a step onwards
            let (test_src, test_dst) = (9, 10);

            let (src, _, _, graph) =
                get_test_graph_for_edge(test_src, test_dst, None, None, false);

            let erefs: Vec<EdgeReference<EdgeData>> =
                graph.edges(src).collect();
            let eref = erefs.get(0).unwrap();

            assert!(!test_candidate.check_for_overlap(eref))
        }

        /// Check that attempting to step back to a visited node, which is
        /// one of the first 3 nodes in the route does not trip the overlap
        /// check, so long as the route is sufficiently long
        #[test]
        fn test_false_near_end() {
            let mut test_candidate = get_test_candidate(None);

            // Min distance is 2.0, max is 3.0
            test_candidate.metrics.common.dist = 2.5;

            // Step back to one of first 3 nodes should not be flagged
            let (test_src, test_dst) = (9, 1);

            let (src, _, _, graph) =
                get_test_graph_for_edge(test_src, test_dst, None, None, false);

            let erefs: Vec<EdgeReference<EdgeData>> =
                graph.edges(src).collect();
            let eref = erefs.get(0).unwrap();

            assert!(!test_candidate.check_for_overlap(eref))
        }

        /// Check that stepping back to the start node does not trip the
        /// overlap check
        #[test]
        fn test_false_at_end() {
            let test_candidate = get_test_candidate(None);

            // Step back to one of first 3 nodes should not be flagged
            let (test_src, test_dst) = (9, 0);

            let (src, _, _, graph) =
                get_test_graph_for_edge(test_src, test_dst, None, None, false);

            let erefs: Vec<EdgeReference<EdgeData>> =
                graph.edges(src).collect();
            let eref = erefs.get(0).unwrap();

            assert!(!test_candidate.check_for_overlap(eref))
        }
    }

    mod test_sum_surfaces_distance {

        use super::*;

        /// Check that summation works when requested surfaces are present
        #[test]
        fn test_non_zero_distance() {
            let mut test_candidate = get_test_candidate(None);

            let mut test_surfaces = FxHashMap::<String, f64>::default();
            test_surfaces.insert("surface_1".to_string(), 25.0);
            test_surfaces.insert("surface_2".to_string(), 50.0);
            test_surfaces.insert("surface_3".to_string(), 100.0);

            test_candidate.metrics.common.s_dists = test_surfaces;

            let result = test_candidate.sum_surfaces_distance(&vec![
                "surface_1".to_string(),
                "surface_2".to_string(),
            ]);

            let target = 75.0;

            assert_eq!(result, target);
        }

        /// Check that zero is returned when requested surfaces are not present
        #[test]
        fn test_zero_distance() {
            let mut test_candidate = get_test_candidate(None);

            let mut test_surfaces = FxHashMap::<String, f64>::default();
            test_surfaces.insert("surface_1".to_string(), 25.0);
            test_surfaces.insert("surface_2".to_string(), 50.0);
            test_surfaces.insert("surface_3".to_string(), 100.0);

            test_candidate.metrics.common.s_dists = test_surfaces;

            let result = test_candidate
                .sum_surfaces_distance(&vec!["surface_4".to_string()]);

            let target = 0.0;

            assert_eq!(result, target);
        }
    }

    mod test_validate_route_completion {
        use super::*;

        /// Going over the max configured distance should invalidate the route,
        /// even if the total is within the target range
        #[test]
        fn test_with_surface_restriction_false() {
            // Create candidate with route restriction, set target dist
            let mut test_config = get_test_config();
            test_config.surface_restriction = SurfaceRestriction::new(
                Some(vec!["surface_1".to_string()]),
                Some(0.5),
            );
            test_config.min_distance = 900.0;
            test_config.max_distance = 1000.0;

            // Set total dist inside target dist
            let mut test_candidate = get_test_candidate(Some(test_config));
            test_candidate.metrics.common.dist = 950.0;

            // Set distance on surface > configured max
            let mut test_surfaces = FxHashMap::<String, f64>::default();
            test_surfaces.insert("surface_1".to_string(), 700.0);
            test_candidate.metrics.common.s_dists = test_surfaces;

            // Check that going over surface max invalidates the route
            assert!(!test_candidate.validate_route_completion());
        }

        /// If both total and surface distances are within configured bounds,
        /// the route should be accepted
        #[test]
        fn test_with_surface_restriction_true() {
            // Create candidate with route restriction, set target dist
            let mut test_config = get_test_config();
            test_config.surface_restriction = SurfaceRestriction::new(
                Some(vec!["surface_1".to_string()]),
                Some(0.5),
            );
            test_config.min_distance = 900.0;
            test_config.max_distance = 1000.0;

            // Set total dist inside target dist
            let mut test_candidate = get_test_candidate(Some(test_config));
            test_candidate.metrics.common.dist = 950.0;

            // Set distance on surface < configured max
            let mut test_surfaces = FxHashMap::<String, f64>::default();
            test_surfaces.insert("surface_1".to_string(), 100.0);
            test_candidate.metrics.common.s_dists = test_surfaces;

            // Check that going over surface max invalidates the route
            assert!(test_candidate.validate_route_completion());
        }

        /// If no surface restriction is set, only total distance should be
        /// checked
        #[test]
        fn test_true() {
            // Create candidate with route restriction, set target dist
            let mut test_config = get_test_config();
            test_config.min_distance = 900.0;
            test_config.max_distance = 1000.0;

            // Set total dist inside target dist
            let mut test_candidate = get_test_candidate(Some(test_config));
            test_candidate.metrics.common.dist = 950.0;

            // Check that going over surface max invalidates the route
            assert!(test_candidate.validate_route_completion());
        }

        /// Going below min distance should invalidate the route
        #[test]
        fn test_false_below_min() {
            // Create candidate with route restriction, set target dist
            let mut test_config = get_test_config();
            test_config.min_distance = 900.0;
            test_config.max_distance = 1000.0;

            // Set total dist inside target dist
            let mut test_candidate = get_test_candidate(Some(test_config));
            test_candidate.metrics.common.dist = 850.0;

            // Check that going over surface max invalidates the route
            assert!(!test_candidate.validate_route_completion());
        }

        /// Going over max distance should invalidate the route
        #[test]
        fn test_false_above_max() {
            // Create candidate with route restriction, set target dist
            let mut test_config = get_test_config();
            test_config.min_distance = 900.0;
            test_config.max_distance = 1000.0;

            // Set total dist inside target dist
            let mut test_candidate = get_test_candidate(Some(test_config));
            test_candidate.metrics.common.dist = 1050.0;

            // Check that going over surface max invalidates the route
            assert!(!test_candidate.validate_route_completion());
        }
    }

    mod test_take_step {

        use super::*;

        /// Taking a step which generates a valid route should return
        /// StepResult::Complete
        #[test]
        fn test_complete() {
            // Get an edge reference stepping from 9 to 0, distance 50m
            let (src, dst, _, graph) =
                get_test_graph_for_edge(9, 0, Some(50.0), None, true);
            let erefs: Vec<EdgeReference<EdgeData>> =
                graph.edges(src).collect();
            let test_eref = erefs.get(0).unwrap();
            let test_ddata = graph.node_weight(dst).unwrap();

            // Create a candidate with configured min/max distance
            let mut test_config = get_test_config();
            test_config.min_distance = 900.0;
            test_config.max_distance = 1000.0;
            let mut test_candidate = get_test_candidate(Some(test_config));

            // Set current distance travelled
            test_candidate.metrics.common.dist = 875.0;

            // Take the step
            let result = test_candidate.take_step(test_eref, test_ddata);

            // Check the output type
            match result {
                StepResult::Complete(_) => (),
                _ => panic!("Got the wrong result type"),
            }
        }

        /// Valid steps which do not complete the route should return
        /// StepResult::Valid
        #[test]
        fn test_valid() {
            // Get an edge reference stepping from 9 to 10, distance 50m, dist to start 50m
            let (src, dst, _, graph) =
                get_test_graph_for_edge(9, 10, Some(50.0), Some(50.0), false);
            let erefs: Vec<EdgeReference<EdgeData>> =
                graph.edges(src).collect();
            let test_eref = erefs.get(0).unwrap();
            let test_ddata = graph.node_weight(dst).unwrap();

            // Create a candidate with configured min/max distance
            let mut test_config = get_test_config();
            test_config.min_distance = 900.0;
            test_config.max_distance = 1000.0;
            let mut test_candidate = get_test_candidate(Some(test_config));

            // Set current distance travelled
            test_candidate.metrics.common.dist = 875.0;

            // Take the step
            let result = test_candidate.take_step(test_eref, test_ddata);

            // Check the output type
            match result {
                StepResult::Valid(_) => (),
                _ => panic!("Got the wrong result type"),
            }
        }

        /// If the step would overlap with nodes already visited, then the
        /// step should return StepResult::Invalid
        #[test]
        fn test_invalid_overlap() {
            // Get an edge reference stepping from 9 to 5, distance 50m, dist to start 50m
            let (src, dst, _, graph) =
                get_test_graph_for_edge(9, 5, Some(50.0), Some(50.0), false);
            let erefs: Vec<EdgeReference<EdgeData>> =
                graph.edges(src).collect();
            let test_eref = erefs.get(0).unwrap();
            let test_ddata = graph.node_weight(dst).unwrap();

            // Create a candidate with configured min/max distance
            let mut test_config = get_test_config();
            test_config.min_distance = 900.0;
            test_config.max_distance = 1000.0;
            let mut test_candidate = get_test_candidate(Some(test_config));

            // Set current distance travelled
            test_candidate.metrics.common.dist = 875.0;

            // Take the step
            let result = test_candidate.take_step(test_eref, test_ddata);

            // Check the output type
            match result {
                StepResult::Invalid => (),
                _ => panic!("Got the wrong result type"),
            }
        }

        /// If the step would put the total distance over the configured
        /// max, then the step should return StepResult::Invalid
        #[test]
        fn test_invalid_dist() {
            // Get an edge reference stepping from 9 to 10, distance 200m, dist_to_start 50m
            let (src, dst, _, graph) =
                get_test_graph_for_edge(9, 10, Some(200.0), Some(50.0), false);
            let erefs: Vec<EdgeReference<EdgeData>> =
                graph.edges(src).collect();
            let test_eref = erefs.get(0).unwrap();
            let test_ddata = graph.node_weight(dst).unwrap();

            // Create a candidate with configured min/max distance
            let mut test_config = get_test_config();
            test_config.min_distance = 900.0;
            test_config.max_distance = 1000.0;
            let mut test_candidate = get_test_candidate(Some(test_config));

            // Set current distance travelled
            test_candidate.metrics.common.dist = 875.0;

            // Take the step
            let result = test_candidate.take_step(test_eref, test_ddata);

            // Check the output type
            match result {
                StepResult::Invalid => (),
                _ => panic!("Got the wrong result type"),
            }
        }

        /// If the step would result in being unable to get back to the start
        /// within the configured max distance, then the step should
        /// return StepResult::Invalid
        #[test]
        fn test_invalid_return_dist() {
            // Get an edge reference stepping from 9 to 10, distance 200m, dist_to_start 50m
            let (src, dst, _, graph) =
                get_test_graph_for_edge(9, 10, Some(50.0), Some(200.0), false);
            let erefs: Vec<EdgeReference<EdgeData>> =
                graph.edges(src).collect();
            let test_eref = erefs.get(0).unwrap();
            let test_ddata = graph.node_weight(dst).unwrap();

            // Create a candidate with configured min/max distance
            let mut test_config = get_test_config();
            test_config.min_distance = 900.0;
            test_config.max_distance = 1000.0;
            let mut test_candidate = get_test_candidate(Some(test_config));

            // Set current distance travelled
            test_candidate.metrics.common.dist = 875.0;

            // Take the step
            let result = test_candidate.take_step(test_eref, test_ddata);

            // Check the output type
            match result {
                StepResult::Invalid => (),
                _ => panic!("Got the wrong result type"),
            }
        }

        /// If the completed route does not match total distance requirements,
        /// then the step should return StepResult::Invalid
        #[test]
        fn test_invalid_on_completion() {
            // Get an edge reference stepping from 9 to 0, distance 5m
            let (src, dst, _, graph) =
                get_test_graph_for_edge(9, 0, Some(5.0), None, false);
            let erefs: Vec<EdgeReference<EdgeData>> =
                graph.edges(src).collect();
            let test_eref = erefs.get(0).unwrap();
            let test_ddata = graph.node_weight(dst).unwrap();

            // Create a candidate with configured min/max distance
            let mut test_config = get_test_config();
            test_config.min_distance = 900.0;
            test_config.max_distance = 1000.0;
            let mut test_candidate = get_test_candidate(Some(test_config));

            // Set current distance travelled
            test_candidate.metrics.common.dist = 875.0;

            // Take the step
            let result = test_candidate.take_step(test_eref, test_ddata);

            // Check the output type
            match result {
                StepResult::Invalid => (),
                _ => panic!("Got the wrong result type"),
            }
        }
    }
}
