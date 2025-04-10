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
use std::time::Instant;

#[derive(Clone)]
struct AppState {
    db: PgPool,
}

async fn get_routes(
    State(state): State<AppState>,
    Query(query): Query<UserRouteConfig>,
) -> impl IntoResponse {
    let now = Instant::now();

    let route_config: RouteConfig = query.into();

    let nodes = load_nodes(&state.db, &route_config).await;
    let edges = load_edges(&state.db, &route_config).await;

    let graph = create_graph(nodes, edges);

    let (mut start_inx, mut graph) = tag_start_node(&route_config, graph);

    graph = tag_dists_to_start(&start_inx, graph);

    (start_inx, graph) = drop_unreachable_nodes(graph, &route_config);

    let routes = generate_routes(graph, route_config, start_inx);
    println!("{:?} routes generated", routes.len());

    for route in routes.iter() {
        println!("{:?}: {:?}", &route.id, &route.metrics)
    }

    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);

    (axum::http::StatusCode::OK, Json(routes)).into_response()
}

async fn health_check() -> impl IntoResponse {
    let msg = "Hello World!";

    let json_response = json!({
        "status": "success",
        "message": msg
    });

    Json(json_response)
}

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
