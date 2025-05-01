use crate::common::config::RouteConfig;
use crate::common::graph_data::{EdgeData, NodeData};
use crate::loading::postgres::{EdgeRow, NodeRow};
use core::f64;
use geo::{Distance, Haversine, Point};
use petgraph::algo::dijkstra;
use petgraph::graph::NodeIndex;
use petgraph::visit::IntoNodeReferences;
use petgraph::{Directed, Graph};
use rustc_hash::{FxHashMap, FxHashSet};

/// Nodes in the graph need to have associated lat/lon data. To achieve this,
/// we create a mapping for source IDs as they appear in the OSM data to
/// NodeData structs. As edges are added, these can then be retrieved and added
/// to the graph. Edges must also be provided here, as we only want to add the
/// nodes which are used in one of the edges.
pub fn generate_node_map(
    nodes: Vec<NodeRow>,
    edges: &Vec<EdgeRow>,
) -> FxHashMap<i64, NodeData> {
    let mut node_map = FxHashMap::<i64, NodeData>::default();
    let mut used_nodes = FxHashSet::<i64>::default();

    for edge in edges {
        used_nodes.insert(edge.src.clone());
        used_nodes.insert(edge.dst.clone());
    }

    for node in nodes {
        if used_nodes.contains(&node.id) {
            node_map.insert(node.id, node.into());
        }
    }

    node_map
}

/// Based on the edge data which has been loaded in from PostgreSQL, generate
/// a petgraph graph which can be used for route plotting
pub fn create_graph(
    nodes: Vec<NodeRow>,
    edges: Vec<EdgeRow>,
) -> Graph<NodeData, EdgeData, Directed, u32> {
    // Set up empty graph
    let mut graph = Graph::<NodeData, EdgeData, Directed, u32>::new();

    // Unpack node data
    let node_weights_by_id = generate_node_map(nodes, &edges);

    // Add all nodes to the graph, create mapping from OSM IDs to node indexes
    // This consumes the data in node_weights_by_id
    let mut node_id_inx_map = FxHashMap::<i64, NodeIndex>::default();
    for (id, weight) in node_weights_by_id.iter() {
        let node_inx = graph.add_node(*weight);
        node_id_inx_map.insert(*id, node_inx);
    }

    for edge in edges {
        // Prepare for loading, src and dst are as provided in the OSM data
        let edge_data: EdgeData = edge.into();

        // Fetch indexes for src and dst as they appear in the graph
        let maybe_src_inx = node_id_inx_map.get(&edge_data.src);
        let src_inx = match maybe_src_inx {
            Some(src_inx) => src_inx,
            None => continue,
        };

        let maybe_dst_inx = node_id_inx_map.get(&edge_data.dst);
        let dst_inx = match maybe_dst_inx {
            Some(dst_inx) => dst_inx,
            None => continue,
        };

        graph.update_edge(*src_inx, *dst_inx, edge_data);
    }

    graph
}

/// Based on the user's selected start point, determine the closest available
/// node to it. While the index of this node is not fixed, the is_start
/// attribute will also be set to true for easier detection
pub fn tag_start_node(
    config: &RouteConfig,
    mut graph: Graph<NodeData, EdgeData, Directed, u32>,
) -> (NodeIndex<u32>, Graph<NodeData, EdgeData, Directed, u32>) {
    // Set variables to keep track of the current closest node
    let mut smallest_dist = f64::MAX;
    let mut closest_inx: Option<NodeIndex<u32>> = None;

    for (node_index, node_weight) in graph.node_references() {
        // Get distance for the current node from the requested start point.
        let node_lat = node_weight.lat;
        let node_lon = node_weight.lon;
        let node_coords: Point = (node_lon, node_lat).into();

        let dist_to_start = Haversine::distance(config.centre, node_coords);

        // Store details of new closest node if applicable
        if dist_to_start < smallest_dist {
            smallest_dist = dist_to_start;
            closest_inx = Some(node_index.clone());
        }
    }

    // Either return the closest node, or panic
    // TODO: Propagate Option return type for cleaner error handling
    match closest_inx {
        Some(inx) => match graph.node_weight_mut(inx) {
            Some(weight) => {
                weight.is_start = true;
                return (inx, graph);
            }
            None => panic!("Unable to find the start node!"),
        },
        None => panic!("Never updated closest_inx!"),
    }
}

/// Use Dijkstra's algorithm on reversed graph to calculate distance of each
/// node from the start point
pub fn tag_dists_to_start(
    start_node: &NodeIndex<u32>,
    mut graph: Graph<NodeData, EdgeData, Directed, u32>,
) -> Graph<NodeData, EdgeData, Directed, u32> {
    graph.reverse();

    let dists =
        dijkstra(&graph, *start_node, None, |edge| edge.weight().distance);

    graph.reverse();

    for (node_inx, dist) in dists.iter() {
        if let Some(weight) = graph.node_weight_mut(*node_inx) {
            weight.dist_to_start = Some(*dist);
        } else {
            ()
        }
    }

    graph
}

/// Determine whether a node should be mapped across to the new graph. This is
/// based on whether a valid path back to the start point exists, and whether
/// the distance of the shortest path is within the bounds of the requested
/// route
fn node_map(node_data: &NodeData, config: &RouteConfig) -> Option<NodeData> {
    match node_data.dist_to_start {
        Some(dist) => {
            if dist <= config.max_distance / 2.0 {
                Some(node_data.clone())
            } else {
                None
            }
        }
        None => None,
    }
}

/// Once the start node has been properly tagged (with tag_start_node), this
/// can be used to retrieve it based on the is_start attribute
pub fn get_start_node(
    graph: &Graph<NodeData, EdgeData, Directed, u32>,
) -> Option<NodeIndex> {
    for (node_inx, node_data) in graph.node_references() {
        match node_data.is_start {
            true => return Some(node_inx),
            false => (),
        }
    }
    return None;
}

pub fn get_start_and_end_nodes(
    graph: &Graph<NodeData, EdgeData, Directed, u32>,
    edge: &EdgeData,
) -> Option<(NodeIndex, NodeIndex)> {
    let mut start_node: Option<NodeIndex> = None;
    let mut end_node: Option<NodeIndex> = None;
    for (node_inx, node_data) in graph.node_references() {
        match node_data.is_start {
            true => start_node = Some(node_inx),
            false => (),
        }
        if node_data.id == edge.dst {
            end_node = Some(node_inx)
        }
    }

    match start_node {
        None => None,
        Some(start_inx) => match end_node {
            None => None,
            Some(end_inx) => Some((start_inx, end_inx)),
        },
    }
}

/// Construct a new graph which contains only those nodes which are reachable
/// from the start node. This includes both nodes for which no route back to
/// the start exist, and nodes which are too far from the start point to be
/// reached by any valid route. As node indices are not static, the start node
/// must be redetermined after this operation.
pub fn drop_unreachable_nodes(
    graph: Graph<NodeData, EdgeData, Directed, u32>,
    config: &RouteConfig,
) -> (NodeIndex, Graph<NodeData, EdgeData, Directed, u32>) {
    let new_graph = graph.filter_map(
        |_, node_data| node_map(node_data, config),
        |_, edge_data| Some(edge_data.clone()),
    );

    let new_start = get_start_node(&new_graph)
        .expect("The start node was marked as unreachable!");

    (new_start, new_graph)
}
