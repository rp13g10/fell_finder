use crate::config::route::RouteConfig;
use crate::loading::structs::{EdgeRow, NodeRow};
use sqlx;
use sqlx::PgPool;

/// Generate a SQL query to read in the nodes for a route based on the
/// provided user configuration
pub fn generate_nodes_query(config: &RouteConfig) -> String {
    let bbox = config.get_bounding_box();

    let ptn_list = bbox.get_partition_list();
    let ptn_str = ptn_list.join(", ");

    let min_lat = &bbox.sw.y().to_string();
    let min_lon = &bbox.sw.x().to_string();
    let max_lat = &bbox.ne.y().to_string();
    let max_lon = &bbox.ne.x().to_string();

    // This brings the query into the compiled code
    let nodes_base = include_str!("get_nodes.sql");

    let nodes_query = nodes_base
        .replace("< ptn_str >", &ptn_str)
        .replace("< min_lat >", &min_lat)
        .replace("< min_lon >", &min_lon)
        .replace("< max_lat >", &max_lat)
        .replace("< max_lon >", &max_lon);

    nodes_query
}

/// Generate a SQL query to read in the edges for a route based on the
/// provided user configuration
/// TODO: Check if there's a more elegant way to perform string replacement
pub fn generate_edges_query(config: &RouteConfig) -> String {
    let bbox = config.get_bounding_box();

    // TODO: Set BBox class to call this once at instantiation
    let ptn_list = bbox.get_partition_list();
    let ptn_str = ptn_list.join(", ");

    let coords = bbox.get_min_max_coords();
    let surface_str = config.get_surface_str();
    let highway_str = config.get_highway_str();

    let edges_base = include_str!("get_edges.sql");

    let edges_query = edges_base
        .replace("< ptn_str >", &ptn_str)
        .replace("< highway_str >", &highway_str)
        .replace("< surface_str >", &surface_str)
        .replace("< min_lat >", &coords.min_lat.to_string())
        .replace("< min_lon >", &coords.min_lon.to_string())
        .replace("< max_lat >", &coords.max_lat.to_string())
        .replace("< max_lon >", &coords.max_lon.to_string());

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
