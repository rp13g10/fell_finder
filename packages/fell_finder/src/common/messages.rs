//! This defines the structures which are used to send/receive data to the
//! user. In addition to those defined here, the Route struct itself can be
//! sent in an API response. To avoid circular dependencies, this is defined
//! directly in the routing module.

use crate::common::exceptions::RoutingError;
use redis;
use redis::RedisError;
use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};
use serde_json::json;

// MARK: Jobs

/// Container for receiving/sending details of a route job between API calls
#[derive(Serialize, Deserialize, Debug)]
pub struct JobId {
    pub job_id: String,
}

#[derive(Serialize, Debug, Clone)]
pub struct JobProgress {
    current: f64,
    target: f64,
    attempt: usize,
    duration: f64,
}

impl JobProgress {
    pub fn new(target: &f64, attempt: &usize) -> JobProgress {
        JobProgress {
            current: 0.0,
            target: target.clone(),
            attempt: attempt.clone(),
            duration: 0.0,
        }
    }

    pub fn update_progress(&mut self, current: f64, duration: f64) {
        self.current = current;
        self.duration = duration;
    }

    pub fn finalize(&mut self) {
        self.current = self.target.clone();
    }
}

#[derive(Serialize, Debug)]
pub enum JobStatus {
    Queued,
    Started,
    Calculating(JobProgress),
    Success,
    Error(RoutingError),
}

#[derive(Serialize, Debug)]
pub struct JobDetails {
    pub job_id: String,
    pub status: JobStatus,
}

// MARK: Redis

pub async fn content_to_redis(
    job_id: &str,
    content_type: &str,
    content: impl Serialize,
    conn: &mut MultiplexedConnection,
) {
    let key = format!("{}_{}", content_type, job_id);
    redis::cmd("SET")
        .arg(&[key, json!(content).to_string()])
        .exec_async(conn)
        .await
        .expect(
            "Unable to communicate with Redis, programme will now terminate.",
        )
}

pub async fn content_from_redis(
    job_id: &str,
    content_type: &str,
    conn: &mut MultiplexedConnection,
) -> Option<String> {
    // TODO: Validate behaviour when querying a key which does not exist, seems
    //       likely that calling expect will cause a panic if the key isn't
    //       there?

    let key = format!("{}_{}", content_type, job_id);
    let content: Result<String, RedisError> =
        redis::cmd("GET").arg(&[key]).query_async(conn).await;

    match content {
        Ok(value) => Some(value),
        Err(_) => None,
    }
}
