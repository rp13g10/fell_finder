use geo::Point;
use geo::{Destination, Haversine};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub struct Bounds {
    pub min_lat: f64,
    pub min_lon: f64,
    pub max_lat: f64,
    pub max_lon: f64,
}

impl Bounds {
    pub fn get_centre(&self) -> (f64, f64) {
        let lat_delta = self.max_lat - self.min_lat;
        let lon_delta = self.max_lon - self.min_lon;
        (
            self.min_lat + (lat_delta / 2.0),
            self.min_lon + (lon_delta / 2.0),
        )
    }
}

#[derive(Debug)]
pub struct BBox {
    pub ne: Point,
    pub nw: Point,
    pub se: Point,
    pub sw: Point,
}

impl BBox {
    // Assumption: No route will cover more than 4 partitions at once, distance
    // to cover one degree lat/lon is ~69 miles

    fn get_ptn_from_point(point: &Point) -> String {
        let x_i = point.x() as i32;
        let y_i = point.y() as i32;
        let ptn = format!("'{:.0}_{:.0}'", y_i, x_i);
        ptn.replace("-", "n")
    }

    pub fn get_partition_list(&self) -> Vec<String> {
        let ne_ptn = BBox::get_ptn_from_point(&self.ne);
        let nw_ptn = BBox::get_ptn_from_point(&self.nw);
        let se_ptn = BBox::get_ptn_from_point(&self.se);
        let sw_ptn = BBox::get_ptn_from_point(&self.sw);

        let mut ptn_list = vec![ne_ptn, nw_ptn, se_ptn, sw_ptn];
        ptn_list.dedup();
        ptn_list
    }

    pub fn get_min_max_coords(&self) -> Bounds {
        Bounds {
            min_lat: self.sw.y(),
            min_lon: self.sw.x(),
            max_lat: self.ne.y(),
            max_lon: self.ne.x(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum RouteType {
    Hilly,
    Flat,
}

// TODO: Check if there is a shorthand for making all attributes public

#[derive(Debug, Clone)]
pub struct SurfaceRestriction {
    pub restricted_surfaces: Vec<String>,
    pub restricted_surfaces_perc: f64,
}

impl SurfaceRestriction {
    pub fn new(
        restricted_surfaces: Option<Vec<String>>,
        restricted_surfaces_perc: Option<f64>,
    ) -> Option<SurfaceRestriction> {
        match restricted_surfaces {
            Some(surfaces) => match restricted_surfaces_perc {
                Some(perc) => Some(SurfaceRestriction {
                    restricted_surfaces: surfaces,
                    restricted_surfaces_perc: perc,
                }),
                None => None,
            },
            None => None,
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct UserRouteConfig {
    pub start_lat: f64,
    pub start_lon: f64,
    pub route_mode: String,
    pub max_candidates: usize,
    pub target_distance: f64,
    pub highway_types: String,
    pub surface_types: String,
    pub restricted_surfaces: Option<Vec<String>>,
    pub restricted_surfaces_perc: Option<f64>,
}

impl Into<RouteConfig> for UserRouteConfig {
    fn into(self) -> RouteConfig {
        let centre = Point::new(self.start_lon, self.start_lat);

        let surface_restriction: Option<SurfaceRestriction> =
            match self.restricted_surfaces {
                Some(rest) => match self.restricted_surfaces_perc {
                    Some(rest_perc) => Some(SurfaceRestriction {
                        restricted_surfaces: rest,
                        restricted_surfaces_perc: rest_perc,
                    }),
                    None => None,
                },
                None => None,
            };

        let min_distance = &self.target_distance / 1.1;
        let max_distance = &self.target_distance * 1.1;

        let route_mode = match self.route_mode.as_str() {
            "hilly" => RouteType::Hilly,
            "flat" => RouteType::Flat,
            _ => RouteType::Hilly, // TODO: Handle this properly
        };

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

#[derive(Debug, Clone)]
pub struct RouteConfig {
    pub centre: Point,
    pub route_mode: RouteType,
    pub max_candidates: usize,
    pub min_distance: f64,
    pub max_distance: f64,
    pub highways: Vec<String>,
    pub surfaces: Vec<String>,
    pub surface_restriction: Option<SurfaceRestriction>,
}

// TODO: Split out config between user and system level

impl RouteConfig {
    pub fn new(
        centre: Point,
        target_distance: f64,
        route_mode: RouteType,
        max_candidates: usize,
        highways: Vec<String>,
        surfaces: Vec<String>,
        restricted_surfaces: Option<Vec<String>>,
        restricted_surfaces_perc: Option<f64>,
    ) -> RouteConfig {
        // TODO: Remove this

        let min_distance = &target_distance / 1.1;
        let max_distance = &target_distance * 1.1;

        // TODO: Add validation of inputs (if restricted surfaces, must also
        //       have restricted surfaces perc)

        RouteConfig {
            centre: centre,
            route_mode: route_mode,
            max_candidates: max_candidates,
            min_distance: min_distance,
            max_distance: max_distance,
            highways: highways,
            surfaces: surfaces,
            surface_restriction: SurfaceRestriction::new(
                restricted_surfaces,
                restricted_surfaces_perc,
            ),
        }
    }

    pub fn get_bounding_box(&self) -> BBox {
        // let two: f64 = 2.0; // there must be a better way?
        let dist_to_corner: f64 = (self.max_distance / 2.0) * (2.0_f64.sqrt());

        let ne =
            Haversine::destination(self.centre, 45.0, dist_to_corner.clone());
        let se =
            Haversine::destination(self.centre, 135.0, dist_to_corner.clone());
        let sw =
            Haversine::destination(self.centre, 225.0, dist_to_corner.clone());
        let nw =
            Haversine::destination(self.centre, 315.0, dist_to_corner.clone());

        BBox {
            ne: ne,
            se: se,
            sw: sw,
            nw: nw,
        }
    }

    pub fn get_highway_str(&self) -> String {
        let highways: Vec<String> = self
            .highways
            .iter()
            .map(|highway| format!("'{highway}'"))
            .collect();
        highways.join(", ")
    }

    pub fn get_surface_str(&self) -> String {
        let surfaces: Vec<String> = self
            .surfaces
            .iter()
            .map(|surface| format!("'{surface}'"))
            .collect();
        surfaces.join(", ")
    }
}
