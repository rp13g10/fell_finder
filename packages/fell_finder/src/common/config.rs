//! This crate contains structs which represent the route configuration options
//! selected by the end user. In particular, the RouteConfig struct is used
//! widely across this package to inform the route creation process.

use crate::common::bbox::BBox;
use geo::Point;
use geo::{Destination, Haversine};
use serde::Deserialize;
use std::env;
use std::str::FromStr;
use uuid::Uuid;

use crate::common::exceptions::{BackendError, ConfigError};

// MARK: Route Config

/// Sets the type of route being created (optimise for max elevation
/// gain if Hilly, min elevation gain if Flat)
#[derive(Debug, Clone, PartialEq)]
pub enum RouteMode {
    Hilly,
    Flat,
}

// Enable conversion from string input, used when parsing API requests
impl FromStr for RouteMode {
    type Err = ConfigError;

    fn from_str(input: &str) -> Result<RouteMode, Self::Err> {
        match input {
            "hilly" => Ok(RouteMode::Hilly),
            "flat" => Ok(RouteMode::Flat),
            _ => Err(ConfigError::InvalidParamError("route_mode".to_string())),
        }
    }
}

/// Contains details of any surface restrictions which the user has applied,
/// e.g. no more than 10% of the max distance to be spent on unmaintained
/// trails
#[derive(Debug, Clone, PartialEq)]
pub struct SurfaceRestriction {
    pub restricted_surfaces: Vec<String>,
    pub restricted_surfaces_perc: f64,
}

impl SurfaceRestriction {
    /// Create a new surface restriction based on user provided input. If
    /// either argument is None, this will also return None
    pub fn new(
        restricted_surfaces: Option<String>,
        restricted_surfaces_perc: Option<f64>,
    ) -> Option<SurfaceRestriction> {
        match restricted_surfaces {
            Some(surfaces) => match restricted_surfaces_perc {
                Some(perc) => {
                    let surfaces_vec: Vec<String> =
                        surfaces.split(',').map(String::from).collect();

                    Some(SurfaceRestriction {
                        restricted_surfaces: surfaces_vec,
                        restricted_surfaces_perc: perc,
                    })
                }
                None => None,
            },
            None => None,
        }
    }
}

/// Stores the user's requested route configuration exactly as it is received
/// from the API
#[derive(Deserialize, Debug, PartialEq)]
pub struct UserRouteConfig {
    pub start_lat: f64,
    pub start_lon: f64,
    pub route_mode: String,
    pub target_distance: f64,
    pub highway_types: String,
    pub surface_types: String,
    pub restricted_surfaces: Option<String>,
    pub restricted_surfaces_perc: Option<f64>,
    pub distance_tolerance: f64,
}

impl TryInto<RouteConfig> for UserRouteConfig {
    type Error = ConfigError;

    fn try_into(self) -> Result<RouteConfig, ConfigError> {
        let centre = Point::new(self.start_lon, self.start_lat);

        let surface_restriction = SurfaceRestriction::new(
            self.restricted_surfaces,
            self.restricted_surfaces_perc,
        );

        let tol_mult = 1.0 + self.distance_tolerance;

        let min_distance = self.target_distance / tol_mult;
        let max_distance = self.target_distance * tol_mult;

        let route_mode = RouteMode::from_str(&self.route_mode)?;

        let highways: Vec<String> =
            self.highway_types.split(',').map(String::from).collect();

        let surfaces: Vec<String> =
            self.surface_types.split(',').map(String::from).collect();

        let job_id = Uuid::new_v4().to_string();

        Ok(RouteConfig {
            centre,
            route_mode,
            min_distance,
            max_distance,
            highways,
            surfaces,
            surface_restriction,
            job_id,
        })
    }
}

/// Stores the user's requested route configuration in a format which can be
/// used in the rest of this package. It is not expected that this struct will
/// be directly instantiated; users should first create a UserRouteConfig and
/// use .into() to convert to RouteConfig
#[derive(Debug, Clone, PartialEq)]
pub struct RouteConfig {
    pub centre: Point,
    pub route_mode: RouteMode,
    pub min_distance: f64,
    pub max_distance: f64,
    pub highways: Vec<String>,
    pub surfaces: Vec<String>,
    pub surface_restriction: Option<SurfaceRestriction>,
    pub job_id: String,
}

impl RouteConfig {
    /// Generate a bounding box which contains the entire area which can be
    /// reached without going over the max distance from the start point
    pub fn get_bounding_box(&self) -> BBox {
        let dist_to_corner: f64 = (self.max_distance / 2.0) * (2.0_f64.sqrt());

        let ne = Haversine::destination(self.centre, 45.0, dist_to_corner);
        let sw = Haversine::destination(self.centre, 225.0, dist_to_corner);

        BBox::from_points(&ne, &sw)
    }

    /// Generate a comma-separated, quoted string containing all requested
    /// highway types. Used for SQL query preparation.
    pub fn get_highway_str(&self) -> String {
        let highways: Vec<String> = self
            .highways
            .iter()
            .map(|highway| format!("'{highway}'"))
            .collect();
        highways.join(", ")
    }

    /// Generate a comma-separated, quoted string containing all requested
    /// surface types. Used for SQL query preparation.
    pub fn get_surface_str(&self) -> String {
        let surfaces: Vec<String> = self
            .surfaces
            .iter()
            .map(|surface| format!("'{surface}'"))
            .collect();
        surfaces.join(", ")
    }
}

// MARK: Backend Config

/// Fetches the specified environment variable as usize, returns an error
/// if the environment variable has not been set, or cannot be converted
fn get_evar_as_int(evar: &str) -> Result<usize, BackendError> {
    let maybe_usr_pref = env::var(evar);
    match maybe_usr_pref {
        Ok(str) => match str.parse() {
            Ok(int) => Ok(int),
            Err(_) => Err(BackendError::InvalidEvarError(evar.to_string())),
        },
        Err(_) => Err(BackendError::MissingEvarError(evar.to_string())),
    }
}

/// Fetches the specified environment variable as f64, returns an error
/// if the environment variable has not been set, or cannot be converted
fn get_evar_as_float(evar: &str) -> Result<f64, BackendError> {
    let maybe_usr_pref = env::var(evar);
    match maybe_usr_pref {
        Ok(str) => match str.parse() {
            Ok(float) => Ok(float),
            Err(_) => Err(BackendError::InvalidEvarError(evar.to_string())),
        },
        Err(_) => Err(BackendError::MissingEvarError(evar.to_string())),
    }
}

/// Fetches the specified environment variable as String, returns an error
/// if the environment variable has not been set
fn get_evar_as_string(evar: &str) -> Result<String, BackendError> {
    let maybe_usr_pref = env::var(evar);
    match maybe_usr_pref {
        Ok(str) => Ok(str),
        Err(_) => Err(BackendError::MissingEvarError(evar.to_string())),
    }
}

/// Fetches the specified environment variable as bool, returns an error
/// if the environment variable has not been set, or cannot be converted
fn get_evar_as_bool(evar: &str) -> Result<bool, BackendError> {
    let maybe_usr_pref = env::var(evar);
    match maybe_usr_pref {
        Ok(str) => match str.parse() {
            Ok(bool) => Ok(bool),
            Err(_) => Err(BackendError::InvalidEvarError(evar.to_string())),
        },
        Err(_) => Err(BackendError::MissingEvarError(evar.to_string())),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PruningStrategy {
    Naive,
    Fuzzy,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinStrategy {
    Last,
    Centre,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PostgresDetails {
    pub user: String,
    pub pass: String,
    pub host: String,
    pub port: String,
}

impl PostgresDetails {
    pub fn get_uri(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/fell_finder",
            self.user, self.pass, self.host, self.port
        )
    }

    pub fn new() -> Result<PostgresDetails, BackendError> {
        Ok(PostgresDetails {
            user: get_evar_as_string("FF_DB_USER")?,
            pass: get_evar_as_string("FF_DB_PASS")?,
            host: get_evar_as_string("FF_DB_HOST")?,
            port: get_evar_as_string("FF_DB_PORT")?,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RedisDetails {
    pub host: String,
    pub port: String,
}

impl RedisDetails {
    pub fn get_uri(&self) -> String {
        format!("redis://{}:{}/", self.host, self.port)
    }

    pub fn new() -> Result<RedisDetails, BackendError> {
        Ok(RedisDetails {
            host: get_evar_as_string("FF_REDIS_HOST")?,
            port: get_evar_as_string("FF_REDIS_PORT")?,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BackendConfig {
    pub max_candidates: usize,
    pub max_routes: usize,
    pub pruning_threshold: f64,
    pub pruning_strategy: PruningStrategy,
    pub dijkstra_validation: bool,
    pub display_threshold: f64,
    pub bin_size: usize,
    pub bin_strategy: BinStrategy,
    pub db_details: PostgresDetails,
    pub redis_details: RedisDetails,
    pub finishing_overlaps: usize,
    pub max_job_seconds: f64,
    pub progress_update_seconds: f64,
}

impl BackendConfig {
    /// Create a new BackendConfig object, pulling all of the required
    /// information from environment variables. If any variables cannot
    /// be read/parsed, the programme will panic
    pub fn new() -> Result<BackendConfig, BackendError> {
        let pruning_strategy_in = get_evar_as_string("FF_PRUNING_STRATEGY")?;

        let pruning_strategy = match pruning_strategy_in.to_uppercase().as_str() {
            "NAIVE" => Ok(PruningStrategy::Naive),
            "FUZZY" => Ok(PruningStrategy::Fuzzy),
            _ => Err(BackendError::InvalidEvarError(
                "Got invalid option for FF_PRUNING_STRATEGY {:?}, must be one of 'Naive', 'Fuzzy'.".to_string(),
            )),
        }?;

        let bin_strategy_in = get_evar_as_string("FF_BIN_STRATEGY")?;

        let bin_strategy = match bin_strategy_in.to_uppercase().as_str() {
            "CENTRE" => Ok(BinStrategy::Centre),
            "LAST" => Ok(BinStrategy::Last),
            _ => Err(BackendError::InvalidEvarError(
                "Got invalid option for FF_BIN_STRATEGY {:?}, must be one of 'Centre', 'Last'.".to_string()
            ))
        }?;

        Ok(BackendConfig {
            max_candidates: get_evar_as_int("FF_MAX_CANDS")?,
            max_routes: get_evar_as_int("FF_MAX_ROUTES")?,
            bin_size: get_evar_as_int("FF_BIN_SIZE")?,
            bin_strategy: bin_strategy,
            pruning_threshold: get_evar_as_float("FF_PRUNING_THRESHOLD")?,
            pruning_strategy: pruning_strategy,
            dijkstra_validation: get_evar_as_bool("FF_DIJKSTRA_VALIDATION")?,
            display_threshold: get_evar_as_float("FF_DISPLAY_THRESHOLD")?,
            db_details: PostgresDetails::new()?,
            redis_details: RedisDetails::new()?,
            finishing_overlaps: get_evar_as_int("FF_FINISHING_OVERLAPS")?,
            max_job_seconds: get_evar_as_float("FF_MAX_JOB_SECONDS")?,
            progress_update_seconds: get_evar_as_float("FF_UPDATE_FREQUENCY")?,
        })
    }

    /// Returns a 'default' configuration object with sensible defaults. This is
    /// not used while hosting the API, but is called frequently during unit tests
    pub fn default() -> BackendConfig {
        BackendConfig {
            max_candidates: 1024,
            max_routes: 32,
            bin_size: 64,
            bin_strategy: BinStrategy::Last,
            pruning_threshold: 0.95,
            pruning_strategy: PruningStrategy::Naive,
            dijkstra_validation: false,
            display_threshold: 0.9,
            db_details: PostgresDetails {
                host: "localhost".to_string(),
                port: "5432".to_string(),
                user: "postgres".to_string(),
                pass: "postgres".to_string(),
            },
            redis_details: RedisDetails {
                host: "localhost".to_string(),
                port: "6379".to_string(),
            },
            finishing_overlaps: 3,
            max_job_seconds: 300.0,
            progress_update_seconds: 1.0,
        }
    }
}

// MARK: Tests
#[cfg(test)]
mod tests {

    use approx::assert_abs_diff_eq;

    use super::*;

    /// Success case for creation of a new RouteMode
    #[test]
    fn test_new_route_type_ok() {
        let maybe_result = RouteMode::from_str("hilly");
        let target = RouteMode::Hilly;

        match maybe_result {
            Ok(result) => {
                assert_eq!(result, target)
            }
            Err(_) => panic!("Should have received a value!"),
        }
    }

    /// Failure case for creation of a new RouteMode
    #[test]
    fn test_new_route_type_err() -> Result<(), String> {
        let maybe_result = RouteMode::from_str("other");

        match maybe_result {
            Ok(_) => Err("Should not have received a value!".to_string()),
            Err(_) => Ok(()),
        }
    }

    /// Creation of surface restriction, all args provided
    #[test]
    fn test_new_restriction_some() {
        let maybe_result =
            SurfaceRestriction::new(Some("surface".to_string()), Some(0.5));

        let target = SurfaceRestriction {
            restricted_surfaces: vec!["surface".to_string()],
            restricted_surfaces_perc: 0.5,
        };

        match maybe_result {
            Some(result) => assert_eq!(result, target),
            None => panic!("Should have received a value"),
        }
    }

    /// Creation of surface restriction, surface arg not provided
    #[test]
    fn test_new_restriction_no_surface() -> Result<(), String> {
        let maybe_result = SurfaceRestriction::new(None, Some(0.5));

        match maybe_result {
            Some(_) => Err("Should not have received a value".to_string()),
            None => Ok(()),
        }
    }

    /// Creation of surface restriction, surface percentage arg not provided
    #[test]
    fn test_new_restriction_no_perc() -> Result<(), String> {
        let maybe_result =
            SurfaceRestriction::new(Some("surface".to_string()), None);

        match maybe_result {
            Some(_) => Err("Should not have received a value".to_string()),
            None => Ok(()),
        }
    }

    /// Check conversion from UserRouteConfig to RouteConfig retains all of
    /// the necessary information
    #[test]
    fn test_user_config_to_route_config_ok() {
        let test_user_config = UserRouteConfig {
            start_lat: 0.1,
            start_lon: 0.2,
            route_mode: "hilly".to_string(),
            target_distance: 0.5,
            highway_types: "highway_1,highway_2".to_string(),
            surface_types: "surface_1,surface_2".to_string(),
            restricted_surfaces: Some("restriction".to_string()),
            restricted_surfaces_perc: Some(0.6),
            distance_tolerance: 0.05,
        };

        let target = RouteConfig {
            centre: (0.2, 0.1).into(),
            route_mode: RouteMode::Hilly,
            min_distance: 0.5 / 1.05,
            max_distance: 0.5 * 1.05,
            highways: vec!["highway_1".to_string(), "highway_2".to_string()],
            surfaces: vec!["surface_1".to_string(), "surface_2".to_string()],
            surface_restriction: Some(SurfaceRestriction {
                restricted_surfaces: vec!["restriction".to_string()],
                restricted_surfaces_perc: 0.6,
            }),
            job_id: "42".to_string(),
        };

        let mut result: RouteConfig = test_user_config.try_into().unwrap();
        result.job_id = "42".to_string();

        assert_eq!(result, target)
    }

    #[test]
    fn test_user_config_to_route_config_err() {
        let test_user_config = UserRouteConfig {
            start_lat: 0.1,
            start_lon: 0.2,
            route_mode: "other".to_string(),
            target_distance: 0.5,
            highway_types: "highway_1,highway_2".to_string(),
            surface_types: "surface_1,surface_2".to_string(),
            restricted_surfaces: Some("restriction".to_string()),
            restricted_surfaces_perc: Some(0.6),
            distance_tolerance: 0.05,
        };

        let target = "route_mode".to_string();

        let result_wrapped: Result<RouteConfig, ConfigError> =
            test_user_config.try_into();

        match result_wrapped {
            Ok(_) => panic!("That shouldn't have worked!"),
            Err(err) => match err {
                ConfigError::InvalidParamError(str) => {
                    assert_eq!(str, target)
                }
                _ => panic!("Got the wrong error type!"),
            },
        }
    }

    /// Check that the generated bounding box covers the expected area
    #[test]
    fn test_get_bounding_box() {
        let test_config = RouteConfig {
            centre: (0.0, 0.0).into(),
            route_mode: RouteMode::Hilly,
            min_distance: 9000.0,
            max_distance: 10000.0,
            highways: vec!["highway_1".to_string()],
            surfaces: vec!["surface_1".to_string()],
            surface_restriction: None,
            job_id: "42".to_string(),
        };

        // Verified using online calculators at c. 7k from origin
        let target_sl_dist = 0.044966;
        let epsilon = 0.0000001;

        let target_ne: Point =
            (0.0 + target_sl_dist, 0.0 + target_sl_dist).into();
        let target_sw: Point =
            (0.0 - target_sl_dist, 0.0 - target_sl_dist).into();

        let target = BBox::from_points(&target_ne, &target_sw);

        let result = test_config.get_bounding_box();

        assert_abs_diff_eq!(result.max_lat, target.max_lat, epsilon = epsilon);
        assert_abs_diff_eq!(result.max_lon, target.max_lon, epsilon = epsilon);
        assert_abs_diff_eq!(result.min_lat, target.min_lat, epsilon = epsilon);
        assert_abs_diff_eq!(result.min_lon, target.min_lon, epsilon = epsilon);
    }

    /// Check that highways are being formatted properly
    #[test]
    fn test_get_highway_str() {
        let test_config = RouteConfig {
            centre: (0.0, 0.0).into(),
            route_mode: RouteMode::Hilly,
            min_distance: 9000.0,
            max_distance: 10000.0,
            highways: vec!["highway_1".to_string(), "highway_2".to_string()],
            surfaces: vec!["surface_1".to_string()],
            surface_restriction: None,
            job_id: "42".to_string(),
        };

        let target = "'highway_1', 'highway_2'".to_string();

        let result = test_config.get_highway_str();

        assert_eq!(result, target);
    }

    /// Check that surfaces are being formatted properly
    #[test]
    fn test_get_surface_str() {
        let test_config = RouteConfig {
            centre: (0.0, 0.0).into(),
            route_mode: RouteMode::Hilly,
            min_distance: 9000.0,
            max_distance: 10000.0,
            highways: vec!["highway_1".to_string()],
            surfaces: vec!["surface_1".to_string(), "surface_2".to_string()],
            surface_restriction: None,
            job_id: "42".to_string(),
        };

        let target = "'surface_1', 'surface_2'".to_string();

        let result = test_config.get_surface_str();

        assert_eq!(result, target);
    }
}
