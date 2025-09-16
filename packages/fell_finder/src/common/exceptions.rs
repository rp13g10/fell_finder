//! This crate defines the various error types which may be raised during
//! the route creation process

use serde::Serialize;
use std::fmt;

#[derive(Debug, Clone, Serialize)]
pub enum BackendError {
    MissingEvarError(String),
    InvalidEvarError(String),
}

impl fmt::Display for BackendError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let msg = match self {
            BackendError::MissingEvarError(evar) => {
                format!("Missing environment variable {}", evar)
            }
            BackendError::InvalidEvarError(evar) => {
                format!("Invalid environment variable {}", evar)
            }
        };
        write!(f, "Error while setting up webapp: {}", msg)
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum ConfigError {
    MissingParamError(String),
    InvalidParamError(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let msg = match self {
            ConfigError::MissingParamError(param) => {
                format!("Missing parameter {}", param)
            }
            ConfigError::InvalidParamError(param) => {
                format!("Invalid parameter {}", param)
            }
        };
        write!(f, "Error parsing route config: {}", msg)
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum RoutingError {
    NoMapDataError,
    DatabaseError(String),
    RedisError(String),
    UserError(String),
    DeveloperError(String),
    BinningError,
    GeometryError,
    NoRoutesError,
    InvalidCandidateError,
    ConfigError(ConfigError),
    TimeoutError,
}
