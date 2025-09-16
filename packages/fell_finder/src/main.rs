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
use fell_finder::common::messages::{
    JobId, JobStatus, content_from_redis, content_to_redis,
};
use fell_finder::loading::petgraph::{
    create_graph, drop_unreachable_nodes, tag_dists_to_start, tag_start_node,
};
use fell_finder::loading::postgres::{load_edges, load_nodes};
use fell_finder::routing::zimmer::generate_routes;
use redis;
use redis::aio::MultiplexedConnection;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{Pool, Postgres};
use std::sync::Arc;

// NOTE: Need to knock out any remaining compiler issues, then test that
//       it's possible to track route creating progress using python. If it
//       works, update webapp.

/// Allow passing of the database connection pool into request handlers
#[derive(Debug, Clone)]
struct AppState {
    db: PgPool,
    redis: MultiplexedConnection,
    cfg: BackendConfig,
}

/// Verify that the user provided query can be processed into a valid route
/// configuration
fn parse_user_params_and_set_job_id(
    query: UserRouteConfig,
) -> Result<RouteConfig, RoutingError> {
    // Attempt to parse user provided route config
    let maybe_route_config: Result<RouteConfig, ConfigError> =
        query.try_into();
    let route_config = match maybe_route_config {
        Ok(config) => config,
        Err(err) => return Err(RoutingError::ConfigError(err)),
    };

    Ok(route_config)
}

/// Based on the provided user config, fetch all corresponding nodes and edges
/// for the route area. Load these into petgraph.
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
/// routes which satisfy the requirements.
async fn generate_request_future(state: AppState, route_config: RouteConfig) {
    let mut redis_conn = state.redis;
    let job_id = route_config.job_id.clone();

    content_to_redis(&job_id, "status", JobStatus::Started, &mut redis_conn)
        .await;

    // Create shareable references to configs
    let route_config = Arc::new(route_config);
    let backend_config = Arc::new(state.cfg);
    let db = state.db;

    // Attempt to fetch data for route generation
    let maybe_tagged_graph =
        get_tagged_graph_for_routing(db, Arc::clone(&route_config)).await;
    let tagged_graph = match maybe_tagged_graph {
        Ok(tagged_graph) => tagged_graph,
        Err(err) => {
            content_to_redis(
                &job_id,
                "status",
                JobStatus::Error(err),
                &mut redis_conn,
            )
            .await;
            return;
        }
    };

    // Attempt to create the route
    let maybe_routes = generate_routes(
        tagged_graph,
        route_config,
        backend_config,
        1,
        &mut redis_conn,
    )
    .await;

    match maybe_routes {
        Ok(routes) => {
            content_to_redis(&job_id, "result", routes, &mut redis_conn).await;
        }
        Err(err) => {
            content_to_redis(
                &job_id,
                "status",
                JobStatus::Error(err),
                &mut redis_conn,
            )
            .await;
        }
    };
}

/// Triggers route creation as a background task, returns a job ID to the user
/// which can be used to poll for status updates & retrieve completed routes
#[axum::debug_handler]
async fn route_request(
    State(state): State<AppState>,
    Query(query): Query<UserRouteConfig>,
) -> impl IntoResponse {
    let mut redis_conn = state.redis.clone();

    // Make sure user has provided a valid configuration
    let route_config = match parse_user_params_and_set_job_id(query) {
        Ok(config) => config,
        Err(err) => {
            return (axum::http::StatusCode::BAD_REQUEST, Json(err))
                .into_response();
        }
    };

    // Prepare route response
    let job_id = route_config.job_id.clone();
    let response_contents = JobId {
        job_id: route_config.job_id.clone(),
    };

    // Trigger route generation as background task
    let request_future = generate_request_future(state, route_config);
    content_to_redis(&job_id, "status", JobStatus::Queued, &mut redis_conn)
        .await;
    tokio::spawn(request_future);

    (axum::http::StatusCode::ACCEPTED, Json(response_contents)).into_response()
}

/// Allows a user to check the status of a job
async fn route_status(
    State(state): State<AppState>,
    Query(job): Query<JobId>,
) -> impl IntoResponse {
    let mut redis_conn = state.redis;
    let job_id = job.job_id;

    let content = content_from_redis(&job_id, "status", &mut redis_conn).await;

    match content {
        Some(status) => (axum::http::StatusCode::OK, status).into_response(),
        None => (
            axum::http::StatusCode::NO_CONTENT,
            format!("Requested job_id {} not found", job_id),
        )
            .into_response(),
    }
}

/// Allows a user to retrieve the results of a completed job
async fn route_retrieve(
    State(state): State<AppState>,
    Query(job): Query<JobId>,
) -> impl IntoResponse {
    let mut redis_conn = state.redis;
    let job_id = job.job_id;

    let content = content_from_redis(&job_id, "result", &mut redis_conn).await;

    match content {
        Some(routes) => (axum::http::StatusCode::OK, routes).into_response(),
        None => (
            axum::http::StatusCode::NO_CONTENT,
            format!("Requested job_id {} has not completed yet", job_id),
        )
            .into_response(),
    }
}

/// Very basic health checker, verify that the app is running and reachable
async fn health_check() -> impl IntoResponse {
    let msg = "Hello World!";

    (axum::http::StatusCode::OK, msg).into_response()
}

/// Entry point for the fell_finder API
#[tokio::main]
async fn main() {
    let backend_config = BackendConfig::new()
        .expect("Critical error retrieving backend config!");

    let db_pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&format!(
            "postgres://{}:{}@localhost:5432/fell_finder",
            backend_config.db_user, backend_config.db_pass
        ))
        .await
        .expect("Error connecting to postgres!");

    // NOTE: Suggestion from redis docs is to clone the redis_conn into each
    // request, operation should be cheap
    let client = redis::Client::open("redis://127.0.0.1/")
        .expect("Error connecting to redis!");
    let redis_conn = client.get_multiplexed_tokio_connection().await.unwrap();

    let state = AppState {
        db: db_pool,
        redis: redis_conn,
        cfg: backend_config,
    };

    // TODO: Add an endpoint which allows users to request a route and get the
    //       response directly, without having to generate a separate retrieve
    //       call

    let router = Router::new()
        .route("/healthcheck", get(health_check))
        // .route("/loop", get(get_routes))
        .route("/route/request", get(route_request))
        .route("/route/status", get(route_status))
        .route("/route/retrieve", get(route_retrieve))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8000")
        .await
        .expect("Error binding to localhost:8000!");
    serve(listener, router).await.expect("Error serving API!");
}
