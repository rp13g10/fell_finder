//! This crate defines the various error types which may be raised during
//! the route creation process

use serde::Serialize;
use std::fmt;

#[derive(Debug, Clone, Serialize)]
pub enum BackendError {
    MissingEvarError(String),
    InvalidEvarError(String),
}

// Make tracebacks a bit more helpful if backend configuration options are not
// set properly
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

#[derive(Clone, Debug)]
pub enum ConfigError {
    MissingParamError(String),
    InvalidParamError(String),
}

impl ConfigError {
    // Return a more user-friendly string. This is a tactical fix until a
    // serde-based solution can be set up
    fn to_user_facing_string(&self) -> String {
        match self {
            Self::MissingParamError(param) => {
                format!("Missing parameter: {}", param)
            }
            Self::InvalidParamError(param) => {
                format!("Invalid parameter: {}", param)
            }
        }
    }
}

// Make tracebacks a bit more helpful if route configuration options are not
// set properly
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

#[derive(Debug, Clone)]
pub enum RoutingError {
    NoMapDataError,
    DatabaseError(String),
    RedisError(String),
    UserError(String),
    DeveloperError(String),
    GeometryError,
    NoRoutesError,
    InvalidCandidateError,
    ConfigError(ConfigError),
    TimeoutError,
}

// Custom serialization for routing errors, this content is displayed in the
// popup messages displayed to users if route creation fails for any reason.
impl Serialize for RoutingError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let msg: String = match self {
            Self::NoMapDataError => {
                "No map data is available for the selected area".to_string()
            }
            Self::DatabaseError(err) => {
                format!("An error was encountered while loading data: {}", err)
            }
            Self::DeveloperError(err) => {
                format!(
                    "Oops! There seems to be an issue with the server: {}",
                    err
                )
            }
            Self::GeometryError => {
                "A route with impossible geometry was generated".to_string()
            }
            Self::NoRoutesError => {
                // TODO: Figure out a tidy syntax for multi-line strings
                "No routes could be generated for the current settings. Please try a different start point, or changing some settings.".to_string()
            }
            Self::InvalidCandidateError => {
                "A route with no data was generated".to_string()
            }
            Self::ConfigError(err) => {
                // TODO: Check to see if there's a way to have Serde convert
                //       the config error directly to string
                let cfg_msg = err.to_user_facing_string();
                format!(
                    "An error with the provided route configuration was encountered: {}",
                    cfg_msg
                )
            }
            Self::TimeoutError => {
                "Route calculation timed out. Please try a shorter distance."
                    .to_string()
            }
        };
        serializer.serialize_str(&msg)
    }
}
