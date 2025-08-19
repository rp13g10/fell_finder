//! TThis stripped-back version of the main script is used for profiling
//! purposes only. Further investigation is required in order to run it
//! directly on the Axum webserver, as a clean shutdown seems to be required.
use fell_finder::common::config::RouteConfig;
use fell_finder::loading::petgraph::{
    create_graph, drop_unreachable_nodes, tag_dists_to_start, tag_start_node,
};
use fell_finder::loading::postgres::{load_edges, load_nodes};
use fell_finder::routing::common::RoutegenResponse;
use fell_finder::routing::zimmer::generate_routes;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use std::time::Instant;

/// Generate a route using static config for profiling purposes
async fn get_routes() -> RoutegenResponse {
    // Capture stats for param optimisation
    let start_time = Instant::now();

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://postgres:postgres@localhost:5432/fell_finder")
        .await
        .expect("Error connecting to postgres!");

    // Config needs to be shared across all candidates
    let route_config = RouteConfig {
        centre: (-1.3476727, 51.0428188).into(),
        route_mode: fell_finder::common::config::RouteMode::Hilly,
        max_candidates: 1024,
        min_distance: 4500.0,
        max_distance: 5500.0,
        highways: vec![
            "motorway".to_string(),
            "motorway_link".to_string(),
            "trunk".to_string(),
            "trunk_link".to_string(),
            "primary_link".to_string(),
            "secondary".to_string(),
            "secondary_link".to_string(),
            "tertiary".to_string(),
            "tertiary_link".to_string(),
            "residential".to_string(),
            "living_street".to_string(),
            "track".to_string(),
            "road".to_string(),
            "pedestrian".to_string(),
            "footway".to_string(),
            "bridleway".to_string(),
            "steps".to_string(),
            "corridor".to_string(),
            "path".to_string(),
            "sidewalk".to_string(),
            "crossing".to_string(),
            "traffic_island".to_string(),
            "cycleway".to_string(),
            "lane".to_string(),
            "shared_busway".to_string(),
            "shared_lane".to_string(),
            "unclassified".to_string(),
        ],
        surfaces: vec![
            "paved".to_string(),
            "asphalt".to_string(),
            "chipseal".to_string(),
            "concrete".to_string(),
            "concrete:lanes".to_string(),
            "concrete:plates".to_string(),
            "paving_stones".to_string(),
            "sett".to_string(),
            "unhewn_cobblestone".to_string(),
            "cobblestone".to_string(),
            "unpaved".to_string(),
            "compacted".to_string(),
            "fine_gravel".to_string(),
            "shells".to_string(),
            "rock".to_string(),
            "pebblestone".to_string(),
            "grass_paver".to_string(),
            "woodchips".to_string(),
            "ground".to_string(),
            "dirt".to_string(),
            "earth".to_string(),
            "grass".to_string(),
            "mud".to_string(),
            "snow".to_string(),
            "ice".to_string(),
            "unclassified".to_string(),
        ],
        surface_restriction: None,
    };
    let shared_config = Arc::new(route_config);

    let nodes = load_nodes(&pool, Arc::clone(&shared_config)).await;
    let edges = load_edges(&pool, Arc::clone(&shared_config)).await;

    let graph = create_graph(nodes, edges);

    let (mut start_inx, mut graph) =
        tag_start_node(Arc::clone(&shared_config), graph);
    graph = tag_dists_to_start(&start_inx, graph);

    (start_inx, graph) =
        drop_unreachable_nodes(graph, Arc::clone(&shared_config));

    let routes = generate_routes(graph, shared_config, start_inx, 0);

    // Log completion time
    let duration = start_time.elapsed().as_secs();
    routes.metrics.set_duration(duration.try_into().unwrap());

    routes
}

/// Entry point for the fell_finder API
#[tokio::main]
async fn main() {
    get_routes().await;
}
