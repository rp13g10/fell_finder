//! The functions defined here can be used to read in details of all of the
//! nodes and edges required to represent a map of the area the user has
//! selected for route creation
use crate::common::config::RouteConfig;
use crate::common::graph_data::{EdgeData, NodeData};
use aho_corasick::AhoCorasick;
use sqlx;
use sqlx::PgPool;

/// Container for the raw output of the nodes SQL query
#[derive(sqlx::FromRow, Debug, Clone, Copy)]
pub struct NodeRow {
    pub id: i64,
    lat: f64,
    lon: f64,
    elevation: f64,
}

impl Into<NodeData> for NodeRow {
    fn into(self) -> NodeData {
        NodeData {
            id: self.id,
            lat: self.lat,
            lon: self.lon,
            elevation: self.elevation,
            is_start: false, // default, will be overwritten``
            dist_to_start: None, // default, will be overwritten
        }
    }
}

/// Container for the raw output of the edges SQL query
#[derive(sqlx::FromRow, Debug)]
pub struct EdgeRow {
    pub src: i64,
    pub dst: i64,
    highway: String,
    surface: String,
    elevation_gain: f64,
    elevation_loss: f64,
    distance: f64,
    lats: Vec<f64>,
    lons: Vec<f64>,
    eles: Vec<f64>,
    dists: Vec<f64>,
}

impl Into<EdgeData> for EdgeRow {
    /// Unpack the raw edge data into a format which can be loaded into the
    /// graph
    fn into(self) -> EdgeData {
        EdgeData {
            src: self.src,
            dst: self.dst,
            highway: self.highway,
            surface: self.surface,
            elevation_gain: self.elevation_gain,
            elevation_loss: self.elevation_loss,
            distance: self.distance,
            lats: self.lats,
            lons: self.lons,
            eles: self.eles,
            dists: self.dists,
        }
    }
}

/// Generate a SQL query to read in the nodes for a route based on the
/// provided user configuration
/// /// TODO: Swap over to using aho-corasick, all subs in single iteration
pub fn generate_nodes_query(config: &RouteConfig) -> String {
    let bbox = config.get_bounding_box();

    let ptn_list = bbox.get_partition_list();
    let ptn_str = ptn_list.join(", ");

    // This brings the query into the compiled code
    let nodes_base = include_str!("get_nodes.sql");

    let patterns = [
        "< ptn_str >".to_string(),
        "< min_lat >".to_string(),
        "< min_lon >".to_string(),
        "< max_lat >".to_string(),
        "< max_lon >".to_string(),
    ];
    let replace_with = [
        ptn_str,
        bbox.min_lat.to_string(),
        bbox.min_lon.to_string(),
        bbox.max_lat.to_string(),
        bbox.max_lon.to_string(),
    ];

    let ac = AhoCorasick::new(patterns)
        .expect("Something went wrong while setting up aho-corasick");

    let nodes_query = ac.replace_all(nodes_base, &replace_with);

    nodes_query
}

/// Generate a SQL query to read in the edges for a route based on the
/// provided user configuration
pub fn generate_edges_query(config: &RouteConfig) -> String {
    let bbox = config.get_bounding_box();

    let ptn_list = bbox.get_partition_list();
    let ptn_str = ptn_list.join(", ");

    let surface_str = config.get_surface_str();
    let highway_str = config.get_highway_str();

    let edges_base = include_str!("get_edges.sql");

    let patterns = [
        "< ptn_str >".to_string(),
        "< highway_str >".to_string(),
        "< surface_str >".to_string(),
        "< min_lat >".to_string(),
        "< min_lon >".to_string(),
        "< max_lat >".to_string(),
        "< max_lon >".to_string(),
    ];
    let replace_with = [
        ptn_str,
        highway_str,
        surface_str,
        bbox.min_lat.to_string(),
        bbox.min_lon.to_string(),
        bbox.max_lat.to_string(),
        bbox.max_lon.to_string(),
    ];

    let ac = AhoCorasick::new(patterns)
        .expect("Something went wrong while setting up aho-corasick");

    let edges_query = ac.replace_all(edges_base, &replace_with);

    edges_query
}

/// Executes the nodes SQL query and returns a vector of NodeRow
pub async fn load_nodes(pool: &PgPool, config: &RouteConfig) -> Vec<NodeRow> {
    let query = generate_nodes_query(config);
    let rows: Vec<NodeRow> =
        sqlx::query_as(&query).fetch_all(pool).await.unwrap();
    rows
}

/// Executes the edges SQL query and returns a vector of NodeRow
pub async fn load_edges(pool: &PgPool, config: &RouteConfig) -> Vec<EdgeRow> {
    let query = generate_edges_query(config);
    let rows: Vec<EdgeRow> =
        sqlx::query_as(&query).fetch_all(pool).await.unwrap();
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::config::RouteMode;

    #[test]
    fn test_node_row_to_data() {
        let test_row = NodeRow {
            id: 0,
            lat: 1.0,
            lon: 2.0,
            elevation: 3.0,
        };

        let target = NodeData {
            id: 0,
            lat: 1.0,
            lon: 2.0,
            elevation: 3.0,
            is_start: false,
            dist_to_start: None,
        };

        let result: NodeData = test_row.into();

        assert_eq!(result, target)
    }

    #[test]
    fn test_edge_row_to_edge() {
        let test_row = EdgeRow {
            src: 0,
            dst: 1,
            highway: "highway".to_string(),
            surface: "surface".to_string(),
            elevation_gain: 2.0,
            elevation_loss: 3.0,
            distance: 4.0,
            lats: vec![5.0],
            lons: vec![6.0],
            eles: vec![7.0],
            dists: vec![8.0],
        };

        let target = EdgeData {
            src: 0,
            dst: 1,
            highway: "highway".to_string(),
            surface: "surface".to_string(),
            elevation_gain: 2.0,
            elevation_loss: 3.0,
            distance: 4.0,
            lats: vec![5.0],
            lons: vec![6.0],
            eles: vec![7.0],
            dists: vec![8.0],
        };

        let result: EdgeData = test_row.into();

        assert_eq!(result, target);
    }

    #[test]
    fn test_gen_nodes_query() {
        let test_config = RouteConfig {
            centre: (0.0, 0.0).into(),
            route_mode: RouteMode::Hilly,
            max_candidates: 1,
            min_distance: 9000.0,
            max_distance: 10000.0,
            highways: vec!["highway_1".to_string()],
            surfaces: vec!["surface_1".to_string(), "surface_2".to_string()],
            surface_restriction: None,
        };

        let target = include_str!("test_data/nodes.sql");

        let result = generate_nodes_query(&test_config);
        assert_eq!(result, target);
    }

    #[test]
    fn test_gen_edges_query() {
        let test_config = RouteConfig {
            centre: (0.0, 0.0).into(),
            route_mode: RouteMode::Hilly,
            max_candidates: 1,
            min_distance: 9000.0,
            max_distance: 10000.0,
            highways: vec!["highway_1".to_string()],
            surfaces: vec!["surface_1".to_string(), "surface_2".to_string()],
            surface_restriction: None,
        };

        let target = include_str!("test_data/edges.sql");

        let result = generate_edges_query(&test_config);
        assert_eq!(result, target);
    }
}
