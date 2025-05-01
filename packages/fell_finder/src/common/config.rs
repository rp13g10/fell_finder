//! This crate contains structs which represent the route configuraiton options
//! selected by the end user. In particular, the RouteConfig struct is used
//! widely across this package to inform the route creation process.

use crate::common::bbox::BBox;
use geo::Point;
use geo::{Destination, Haversine};
use serde::Deserialize;
use std::str::FromStr;

/// Sets the type of route being created (optimise for max elevation
/// gain if Hilly, min elevation gain if Flat)
#[derive(Debug, Clone, PartialEq)]
pub enum RouteMode {
    Hilly,
    Flat,
}

impl FromStr for RouteMode {
    type Err = ();

    fn from_str(input: &str) -> Result<RouteMode, Self::Err> {
        match input {
            "hilly" => Ok(RouteMode::Hilly),
            "flat" => Ok(RouteMode::Flat),
            _ => Err(()),
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
                    let surfaces_vec: Vec<String> = surfaces
                        .split(',')
                        .into_iter()
                        .map(|item| String::from(item))
                        .collect();

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
    pub max_candidates: usize,
    pub target_distance: f64,
    pub highway_types: String,
    pub surface_types: String,
    pub restricted_surfaces: Option<String>,
    pub restricted_surfaces_perc: Option<f64>,
    pub distance_tolerance: f64,
}

impl Into<RouteConfig> for UserRouteConfig {
    fn into(self) -> RouteConfig {
        let centre = Point::new(self.start_lon, self.start_lat);

        let surface_restriction = SurfaceRestriction::new(
            self.restricted_surfaces,
            self.restricted_surfaces_perc,
        );

        let tol_mult = 1.0 + self.distance_tolerance;

        let min_distance = &self.target_distance / tol_mult;
        let max_distance = &self.target_distance * tol_mult;

        // TODO: Set up panic message to include error contents, set error
        //       contents to contain details of invalid string
        let route_mode = RouteMode::from_str(&self.route_mode)
            .expect("Received an invalid route mode");

        let highways: Vec<String> = self
            .highway_types
            .split(',')
            .into_iter()
            .map(|item| String::from(item))
            .collect();

        let surfaces: Vec<String> = self
            .surface_types
            .split(',')
            .into_iter()
            .map(|item| String::from(item))
            .collect();

        RouteConfig {
            centre: centre,
            route_mode: route_mode,
            max_candidates: self.max_candidates,
            min_distance: min_distance,
            max_distance: max_distance,
            highways: highways,
            surfaces: surfaces,
            surface_restriction: surface_restriction,
        }
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
    pub max_candidates: usize,
    pub min_distance: f64,
    pub max_distance: f64,
    pub highways: Vec<String>,
    pub surfaces: Vec<String>,
    pub surface_restriction: Option<SurfaceRestriction>,
}

// TODO: Set max_candidates to be determined according to user requested
//       distance (or size of graph, if you're feeling fancy)

impl RouteConfig {
    /// Generate a bounding box which contains the entire area which can be
    /// reached without going over the max distance from the start point
    pub fn get_bounding_box(&self) -> BBox {
        let dist_to_corner: f64 = (self.max_distance / 2.0) * (2.0_f64.sqrt());

        let ne =
            Haversine::destination(self.centre, 45.0, dist_to_corner.clone());
        let sw =
            Haversine::destination(self.centre, 225.0, dist_to_corner.clone());

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
    fn test_user_config_to_route_config() {
        let test_user_config = UserRouteConfig {
            start_lat: 0.1,
            start_lon: 0.2,
            route_mode: "hilly".to_string(),
            max_candidates: 4,
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
            max_candidates: 4,
            min_distance: 0.5 / 1.05,
            max_distance: 0.5 * 1.05,
            highways: vec!["highway_1".to_string(), "highway_2".to_string()],
            surfaces: vec!["surface_1".to_string(), "surface_2".to_string()],
            surface_restriction: Some(SurfaceRestriction {
                restricted_surfaces: vec!["restriction".to_string()],
                restricted_surfaces_perc: 0.6,
            }),
        };

        let result: RouteConfig = test_user_config.into();

        assert_eq!(result, target)
    }

    /// Check that the generated bounding box covers the expected area
    #[test]
    fn test_get_bounding_box() {
        let test_config = RouteConfig {
            centre: (0.0, 0.0).into(),
            route_mode: RouteMode::Hilly,
            max_candidates: 1,
            min_distance: 9000.0,
            max_distance: 10000.0,
            highways: vec!["highway_1".to_string()],
            surfaces: vec!["surface_1".to_string()],
            surface_restriction: None,
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
            max_candidates: 1,
            min_distance: 9000.0,
            max_distance: 10000.0,
            highways: vec!["highway_1".to_string(), "highway_2".to_string()],
            surfaces: vec!["surface_1".to_string()],
            surface_restriction: None,
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
            max_candidates: 1,
            min_distance: 9000.0,
            max_distance: 10000.0,
            highways: vec!["highway_1".to_string()],
            surfaces: vec!["surface_1".to_string(), "surface_2".to_string()],
            surface_restriction: None,
        };

        let target = "'surface_1', 'surface_2'".to_string();

        let result = test_config.get_surface_str();

        assert_eq!(result, target);
    }
}
