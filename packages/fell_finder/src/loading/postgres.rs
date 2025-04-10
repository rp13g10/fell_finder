use crate::common::config::RouteConfig;
use crate::common::graph_data::{EdgeData, NodeData};
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

impl NodeRow {
    /// Unpack the raw node data into a format which can be loaded into the
    /// graph
    pub fn prepare(self) -> NodeData {
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

impl EdgeRow {
    /// Unpack the raw edge data into a format which can be loaded into the
    /// graph
    pub fn prepare(self) -> (i64, i64, EdgeData) {
        let data = EdgeData {
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
        };

        (self.src, self.dst, data)
    }
}

/// Generate a SQL query to read in the nodes for a route based on the
/// provided user configuration
pub fn generate_nodes_query(config: &RouteConfig) -> String {
    let bbox = config.get_bounding_box();

    let ptn_list = bbox.get_partition_list();
    let ptn_str = ptn_list.join(", ");

    // This brings the query into the compiled code
    let nodes_base = include_str!("get_nodes.sql");

    let nodes_query = nodes_base
        .replace("< ptn_str >", &ptn_str)
        .replace("< min_lat >", &bbox.min_lat.to_string())
        .replace("< min_lon >", &bbox.min_lon.to_string())
        .replace("< max_lat >", &bbox.max_lat.to_string())
        .replace("< max_lon >", &bbox.max_lon.to_string());

    nodes_query
}

/// Generate a SQL query to read in the edges for a route based on the
/// provided user configuration
/// TODO: Check if there's a more elegant way to perform string replacement
pub fn generate_edges_query(config: &RouteConfig) -> String {
    let bbox = config.get_bounding_box();

    let ptn_list = bbox.get_partition_list();
    let ptn_str = ptn_list.join(", ");

    let surface_str = config.get_surface_str();
    let highway_str = config.get_highway_str();

    let edges_base = include_str!("get_edges.sql");

    let edges_query = edges_base
        .replace("< ptn_str >", &ptn_str)
        .replace("< highway_str >", &highway_str)
        .replace("< surface_str >", &surface_str)
        .replace("< min_lat >", &bbox.min_lat.to_string())
        .replace("< min_lon >", &bbox.min_lon.to_string())
        .replace("< max_lat >", &bbox.max_lat.to_string())
        .replace("< max_lon >", &bbox.max_lon.to_string());

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
