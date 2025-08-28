//! This is the main entry point for the fell_finder crate. It spawns a basic
//! Axum webserver which accepts requests to generate routes based on user
//! inputs. The webserver configuration is currently in MVP state, and is due
//! to be improved in a future release.
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::serve;
use axum::{Json, Router};
use fell_finder::common::config::{
    BackendConfig, RouteConfig, UserRouteConfig,
};
use fell_finder::common::exceptions::{ConfigError, RoutingError};
use fell_finder::common::graph_data::TaggedGraph;
use fell_finder::loading::petgraph::{
    create_graph, drop_unreachable_nodes, tag_dists_to_start, tag_start_node,
};
use fell_finder::loading::postgres::{load_edges, load_nodes};
use fell_finder::routing::zimmer::generate_routes;
use serde_json::json;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{Pool, Postgres};
use std::sync::Arc;

/// Allow passing of the database connection pool into request handlers
#[derive(Debug, Clone)]
struct AppState {
    db: PgPool,
    cfg: BackendConfig,
}

async fn get_tagged_graph_for_routing(
    db: Pool<Postgres>,
    route_config: Arc<RouteConfig>,
) -> Result<TaggedGraph, RoutingError> {
    let nodes = load_nodes(&db, Arc::clone(&route_config)).await?;
    let edges = load_edges(&db, Arc::clone(&route_config)).await?;

    let graph = create_graph(nodes, edges);

    let mut tagged_graph = tag_start_node(Arc::clone(&route_config), graph)?;

    tagged_graph = tag_dists_to_start(tagged_graph);

    tagged_graph =
        drop_unreachable_nodes(tagged_graph, Arc::clone(&route_config))?;
    Ok(tagged_graph)
}
/// Based on the user provided configuration options, attempt to generate
/// routes which satisfy the requirements. Currently only success status codes
/// are returned. More graceful error handling is planned for a future
/// release.
async fn get_routes(
    State(state): State<AppState>,
    Query(query): Query<UserRouteConfig>,
) -> impl IntoResponse {
    // Attempt to parse user provided route config
    let maybe_route_config: Result<RouteConfig, ConfigError> =
        query.try_into();
    let route_config = match maybe_route_config {
        Ok(config) => config,
        Err(err) => {
            return (axum::http::StatusCode::BAD_REQUEST, format!("{}", err))
                .into_response();
        }
    };
    let route_config = Arc::new(route_config);
    let backend_config = Arc::new(state.cfg);
    let db = state.db;

    // Attempt to fetch data for route generation
    let maybe_tagged_graph =
        get_tagged_graph_for_routing(db, Arc::clone(&route_config)).await;
    let tagged_graph = match maybe_tagged_graph {
        Ok(tagged_graph) => tagged_graph,
        Err(err) => {
            return (axum::http::StatusCode::INTERNAL_SERVER_ERROR, Json(err))
                .into_response();
        }
    };

    // Attempt to create the route
    let maybe_routes =
        generate_routes(tagged_graph, route_config, backend_config, 1);

    match maybe_routes {
        Ok(routes) => {
            (axum::http::StatusCode::OK, Json(routes)).into_response()
        }
        Err(err) => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, Json(err))
            .into_response(),
    }
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
    let backend_config = BackendConfig::new()
        .expect("Critical error retrieving backend config!");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&format!(
            "postgres://{}:{}@localhost:5432/fell_finder",
            backend_config.db_user, backend_config.db_pass
        ))
        .await
        .expect("Error connecting to postgres!");

    let state = AppState {
        db: pool,
        cfg: backend_config,
    };

    let router = Router::new()
        .route("/healthcheck", get(health_check))
        .route("/loop", get(get_routes))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000")
        .await
        .expect("Error binding to localhost:8000!");
    serve(listener, router).await.expect("Error serving API!");
}
