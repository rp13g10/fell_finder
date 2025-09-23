//! This defines the structures which are used to send/receive data to the
//! user. In addition to those defined here, the Route struct itself can be
//! sent in an API response. To avoid circular dependencies, this is defined
//! directly in the routing module.

use crate::common::exceptions::RoutingError;
use redis;
use redis::RedisError;
use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize, ser::SerializeMap};
use serde_json::json;

// MARK: Jobs

/// Container for receiving/sending details of a route job between API calls
#[derive(Serialize, Deserialize, Debug)]
pub struct JobId {
    pub job_id: String,
}

/// Container for job progress, contains the information required to display a
/// progress bar for a job which has been started
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
            target: *target,
            attempt: *attempt,
            duration: 0.0,
        }
    }

    pub fn update_progress(&mut self, current: f64, duration: f64) {
        self.current = current;
        self.duration = duration;
    }

    pub fn finalize(&mut self) {
        self.current = self.target;
    }
}

/// Contains details of the status of a current job
#[derive(Debug)]
pub enum JobStatus {
    Queued,
    Started,
    Calculating(JobProgress),
    Success,
    Error(RoutingError),
}

// Define custom serialization options, this ensures a consistent format
// is used when returning status information to users
impl Serialize for JobStatus {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_map(None)?;

        // TODO: For progress & err, values are currently being quoted. need to
        //       prevent this, or cut off the first & last chars

        match self {
            Self::Queued => {
                state.serialize_entry("status", "queued")?;
                state.serialize_entry("detail", "")?;
            }
            Self::Started => {
                state.serialize_entry("status", "started")?;
                state.serialize_entry("detail", "")?;
            }
            Self::Calculating(progress) => {
                state.serialize_entry("status", "calculating")?;
                state.serialize_entry("detail", progress)?;
            }
            Self::Success => {
                state.serialize_entry("status", "success")?;
                state.serialize_entry("detail", "")?;
            }
            Self::Error(err) => {
                state.serialize_entry("status", "error")?;
                state.serialize_entry("detail", err)?;
            }
        };
        state.end()
    }
}

/// Combines the details of a job with a job ID, ready to be sent back to the
/// user in response to an API request
#[derive(Serialize, Debug)]
pub struct JobDetails {
    pub job_id: String,
    pub status: JobStatus,
}

// MARK: Redis

/// Convenience function which writes content to Redis, such as job status or
/// job results. Keys are set based on the job ID and the content type.
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

/// Convenience function which retrieves content from Redis, such as job status
/// or job results. Keys are set based on the job ID and the content type.
pub async fn content_from_redis(
    job_id: &str,
    content_type: &str,
    conn: &mut MultiplexedConnection,
) -> Option<String> {
    let key = format!("{}_{}", content_type, job_id);
    let content: Result<String, RedisError> =
        redis::cmd("GET").arg(&[key]).query_async(conn).await;

    content.ok()
}
