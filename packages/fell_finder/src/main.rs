//! This is the main entry point for the fell_finder crate. It spawns a basic
//! Axum webserver which accepts requests to generate routes based on user
//! inputs. The webserver configuration is currently in MVP state, and is due
//! to be improved in a future release.
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::serve;
use axum::{Json, Router};
use fell_finder::common::config::{RouteConfig, UserRouteConfig};
use fell_finder::loading::petgraph::{
    create_graph, drop_unreachable_nodes, tag_dists_to_start, tag_start_node,
};
use fell_finder::loading::postgres::{load_edges, load_nodes};
use fell_finder::routing::zimmer::generate_routes;
use serde_json::json;
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::sync::Arc;
use std::time::Instant;

/// Allow passing of the database connection pool into request handlers
#[derive(Clone)]
struct AppState {
    db: PgPool,
}

// TODO: Allow configuration of backend systems via API params, pull from env
//       vars if not provided. Easier for experimentation.

/// Based on the user provided configuration options, attempt to generate
/// routes which satisfy the requirements. Currently only success status codes
/// are returned. More graceful error handling is planned for a future
/// release.
async fn get_routes(
    State(state): State<AppState>,
    Query(query): Query<UserRouteConfig>,
) -> impl IntoResponse {
    // Capture stats for param optimisation
    let start_time = Instant::now();

    // Config needs to be shared across all candidates
    let route_config: RouteConfig = query.into();
    let shared_config = Arc::new(route_config);

    let nodes = load_nodes(&state.db, Arc::clone(&shared_config)).await;
    let edges = load_edges(&state.db, Arc::clone(&shared_config)).await;

    let graph = create_graph(nodes, edges);

    let (mut start_inx, mut graph) =
        tag_start_node(Arc::clone(&shared_config), graph);
    graph = tag_dists_to_start(&start_inx, graph);

    (start_inx, graph) =
        drop_unreachable_nodes(graph, Arc::clone(&shared_config));

    let mut routes = generate_routes(graph, shared_config, start_inx, 1);

    // Log completion time
    // TODO: Figure out why mutating metrics directly doesn't seem to stick
    let duration = start_time.elapsed().as_secs_f64();
    routes.metrics.duration = duration.try_into().unwrap();

    (axum::http::StatusCode::OK, Json(routes)).into_response()
}

/// Very basic health checker, verify that the app is running and reachable
async fn health_check() -> impl IntoResponse {
    let msg = "Hello World!";

    let json_response = json!({
        "status": "success",
        "message": msg
    });

    Json(json_response)
}

/// Entry point for the fell_finder API
#[tokio::main]
async fn main() {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://postgres:postgres@localhost:5432/fell_finder")
        .await
        .expect("Error connecting to postgres!");

    let state = AppState { db: pool };

    let router = Router::new()
        .route("/healthcheck", get(health_check))
        .route("/loop", get(get_routes))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000")
        .await
        .expect("Error binding to localhost:8000!");
    serve(listener, router).await.expect("Error serving API!");
}
