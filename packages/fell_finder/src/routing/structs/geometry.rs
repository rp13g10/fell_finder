//! Defines the structs which contain all of the details about the physical
//! geometry of a route (i.e. the points it visits)
use serde::Serialize;
use std::iter::zip;

use crate::common::bbox::BBox;
use crate::common::graph_data::EdgeData;

// TODO: Tidy up ambiguity around bbox/bounds (should be bbox)

/// Stores the geometry of each candidate in an unprocessed form
#[derive(Clone)]
pub struct CandidateGeometry {
    // TODO: Consider using the Default trait
    lats: Vec<f64>,
    lons: Vec<f64>,
    dists: Vec<f64>,
    eles: Vec<f64>,
}

impl CandidateGeometry {
    /// Set up a new container for route geometry
    pub fn new() -> CandidateGeometry {
        CandidateGeometry {
            lats: Vec::<f64>::new(),
            lons: Vec::<f64>::new(),
            dists: Vec::<f64>::new(),
            eles: Vec::<f64>::new(),
        }
    }
}

impl CandidateGeometry {
    pub fn take_step(&mut self, edata: &EdgeData) {
        self.lats.extend(edata.lats.clone());
        self.lons.extend(edata.lons.clone());
        self.dists.extend(edata.dists.clone());
        self.eles.extend(edata.eles.clone());
    }

    /// Determine the current centre point of the candidate route as a tuple
    /// containing the latitude and longitude
    pub fn get_centre(&self) -> (f64, f64) {
        let num = self.lats.len() as f64;
        let lasum: f64 = self.lats.iter().sum();
        let losum: f64 = self.lons.iter().sum();
        let lat = lasum / num;
        let lon = losum / num;

        (lat, lon)
    }

    /// Determine the current position of the candidate route as a tuple
    /// containing the latitude and longitude
    pub fn get_pos(&self) -> (f64, f64) {
        let lat = self.lats.last().unwrap().clone();
        let lon = self.lons.last().unwrap().clone();

        (lat, lon)
    }

    fn get_bbox(&self) -> BBox {
        let min_lat = self
            .lats
            .iter()
            .min_by(|a, b| a.partial_cmp(b).expect("NaN in lats!"))
            .unwrap();
        let min_lon = self
            .lons
            .iter()
            .min_by(|a, b| a.partial_cmp(b).expect("NaN in lons!"))
            .unwrap();
        let max_lat = self
            .lats
            .iter()
            .max_by(|a, b| a.partial_cmp(b).expect("NaN in lats!"))
            .unwrap();
        let max_lon = self
            .lons
            .iter()
            .max_by(|a, b| a.partial_cmp(b).expect("NaN in lons!"))
            .unwrap();

        BBox {
            min_lat: min_lat.clone(),
            min_lon: min_lon.clone(),
            max_lat: max_lat.clone(),
            max_lon: max_lon.clone(),
        }
    }

    fn zip_coords(&self) -> Vec<(f64, f64)> {
        let zipped = zip(self.lats.clone(), self.lons.clone());
        zipped.collect()
    }

    pub fn finalize(&self) -> RouteGeometry {
        RouteGeometry {
            coords: self.zip_coords(),
            dists: self.dists.clone(),
            eles: self.eles.clone(),
            bbox: self.get_bbox(),
        }
    }
}

/// Stores the geometry of each route in a format which can easily be rendered
/// in the frontend
#[derive(Debug, Serialize)]
pub struct RouteGeometry {
    pub coords: Vec<(f64, f64)>,
    pub dists: Vec<f64>,
    pub eles: Vec<f64>,
    pub bbox: BBox,
}
