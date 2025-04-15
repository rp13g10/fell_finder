//! Defines the structs which contain all of the details about the physical
//! geometry of a route (i.e. the points it visits)
use serde::Serialize;
use std::iter::zip;

use crate::common::bbox::BBox;
use crate::common::graph_data::EdgeData;

/// Stores the geometry of each candidate in an unprocessed form
#[derive(Clone, Debug, PartialEq)]
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
        // TODO: Find a faster way of doing this, sorting each list twice must
        //       be avoidable

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
#[derive(Debug, Serialize, PartialEq)]
pub struct RouteGeometry {
    pub coords: Vec<(f64, f64)>,
    pub dists: Vec<f64>,
    pub eles: Vec<f64>,
    pub bbox: BBox,
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_take_step() {
        let edge_1 = EdgeData {
            src: 0,
            dst: 1,
            highway: "highway".to_string(),
            surface: "surface".to_string(),
            elevation_gain: 2.0,
            elevation_loss: 3.0,
            distance: 4.0,
            lats: vec![5.0, 6.0],
            lons: vec![7.0, 8.0],
            dists: vec![9.0, 10.0],
            eles: vec![11.0, 12.0],
        };
        let edge_2 = EdgeData {
            src: 0,
            dst: 1,
            highway: "highway".to_string(),
            surface: "surface".to_string(),
            elevation_gain: 13.0,
            elevation_loss: 14.0,
            distance: 15.0,
            lats: vec![16.0, 17.0],
            lons: vec![18.0, 19.0],
            dists: vec![20.0, 21.0],
            eles: vec![22.0, 23.0],
        };

        let target = CandidateGeometry {
            lats: vec![5.0, 6.0, 16.0, 17.0],
            lons: vec![7.0, 8.0, 18.0, 19.0],
            dists: vec![9.0, 10.0, 20.0, 21.0],
            eles: vec![11.0, 12.0, 22.0, 23.0],
        };

        let mut result = CandidateGeometry::new();
        result.take_step(&edge_1);
        result.take_step(&edge_2);

        assert_eq!(result, target);
    }

    #[test]
    fn test_get_centre() {
        let test_candidate = CandidateGeometry {
            lats: vec![5.0, 6.0, 16.0, 17.0],
            lons: vec![7.0, 8.0, 18.0, 19.0],
            dists: vec![9.0, 10.0, 20.0, 21.0],
            eles: vec![11.0, 12.0, 22.0, 23.0],
        };

        let target = (11.0, 13.0);

        let result = test_candidate.get_centre();

        assert_eq!(result, target);
    }

    #[test]
    fn test_get_pos() {
        let test_candidate = CandidateGeometry {
            lats: vec![5.0, 6.0, 16.0, 17.0],
            lons: vec![7.0, 8.0, 18.0, 19.0],
            dists: vec![9.0, 10.0, 20.0, 21.0],
            eles: vec![11.0, 12.0, 22.0, 23.0],
        };

        let target = (17.0, 19.0);

        let result = test_candidate.get_pos();

        assert_eq!(result, target);
    }

    #[test]
    fn test_get_bbox() {
        let test_candidate = CandidateGeometry {
            lats: vec![5.0, 6.0, 16.0, 17.0],
            lons: vec![7.0, 8.0, 18.0, 19.0],
            dists: vec![9.0, 10.0, 20.0, 21.0],
            eles: vec![11.0, 12.0, 22.0, 23.0],
        };

        let target = BBox {
            min_lat: 5.0,
            min_lon: 7.0,
            max_lat: 17.0,
            max_lon: 19.0,
        };

        let result = test_candidate.get_bbox();

        assert_eq!(result, target);
    }

    #[test]
    fn test_zip_coords() {
        let test_candidate = CandidateGeometry {
            lats: vec![5.0, 6.0, 16.0, 17.0],
            lons: vec![7.0, 8.0, 18.0, 19.0],
            dists: vec![9.0, 10.0, 20.0, 21.0],
            eles: vec![11.0, 12.0, 22.0, 23.0],
        };

        let target = vec![(5.0, 7.0), (6.0, 8.0), (16.0, 18.0), (17.0, 19.0)];

        let result = test_candidate.zip_coords();

        assert_eq!(result, target);
    }

    #[test]
    fn test_finalize() {
        let test_candidate = CandidateGeometry {
            lats: vec![5.0, 6.0, 16.0, 17.0],
            lons: vec![7.0, 8.0, 18.0, 19.0],
            dists: vec![9.0, 10.0, 20.0, 21.0],
            eles: vec![11.0, 12.0, 22.0, 23.0],
        };

        let target_coords =
            vec![(5.0, 7.0), (6.0, 8.0), (16.0, 18.0), (17.0, 19.0)];
        let target_bbox = BBox {
            min_lat: 5.0,
            min_lon: 7.0,
            max_lat: 17.0,
            max_lon: 19.0,
        };

        let target = RouteGeometry {
            coords: target_coords,
            dists: vec![9.0, 10.0, 20.0, 21.0],
            eles: vec![11.0, 12.0, 22.0, 23.0],
            bbox: target_bbox,
        };

        let result = test_candidate.finalize();

        assert_eq!(result, target);
    }
}
