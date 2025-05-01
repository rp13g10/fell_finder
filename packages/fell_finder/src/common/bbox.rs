//! Defines a struct to represent a bounding box, which is used to represent
//! a 2d square(ish) on the planet's surface. This is used when reading in
//! data from postgres, but is also helpful when rendering a completed route,
//! as it can be used to set the viewport on a map visualization

use geo::Point;
use serde::Serialize;

/// A bounding box for geographical data. Contains the minimum and maximum
/// latitudes & longitudes, defining a 'rectangle' on the surface of the Earth
#[derive(Debug, Serialize, PartialEq)]
pub struct BBox {
    pub min_lat: f64,
    pub min_lon: f64,
    pub max_lat: f64,
    pub max_lon: f64,
}

impl BBox {
    /// Create a new bounding box based on the north-east and south-west points
    /// of a 'rectangle'
    pub fn from_points(ne: &Point, sw: &Point) -> Self {
        let max_lat = ne.y();
        let min_lat = sw.y();
        let max_lon = ne.x();
        let min_lon = sw.x();

        BBox {
            min_lat: min_lat,
            min_lon: min_lon,
            max_lat: max_lat,
            max_lon: max_lon,
        }
    }

    /// Determine the latitude and longitude which form the centre point of
    /// the bounding box
    pub fn get_centre(&self) -> (f64, f64) {
        let lat_delta = self.max_lat - self.min_lat;
        let lon_delta = self.max_lon - self.min_lon;
        (
            self.min_lat + (lat_delta / 2.0),
            self.min_lon + (lon_delta / 2.0),
        )
    }

    /// Convert a latitude & longitude into a partition string, which can be
    /// used when querying data from postgres. This mirrors the logic used
    /// to assign partitions in the ingestion layer
    fn get_ptn_from_coords(lat: &f64, lon: &f64) -> String {
        let lat_i = *lat as i32;
        let lon_i = *lon as i32;
        let ptn = format!("'{:.0}_{:.0}'", lat_i, lon_i);
        ptn.replace("-", "n")
    }

    /// Get a list of all of the partitions which contain data for this
    /// bounding box. This is a simplistic implementation which assumes that
    /// no route will ever span more than 4 partitions. This is based on the
    /// approximate distance to cover one degree of lat/lon (and by extension,
    /// on partition) is ~69 miles.
    /// Please note that partitions are returned wrapped in single quotes, as
    /// it is anticipated that they will be used directly in SQL queries
    pub fn get_partition_list(&self) -> Vec<String> {
        // Get the partitions containing each corner of the bounding box
        let ne_ptn = BBox::get_ptn_from_coords(&self.max_lat, &self.max_lon);
        let nw_ptn = BBox::get_ptn_from_coords(&self.max_lat, &self.min_lon);
        let se_ptn = BBox::get_ptn_from_coords(&self.min_lat, &self.max_lon);
        let sw_ptn = BBox::get_ptn_from_coords(&self.min_lat, &self.min_lon);

        // Combine into a list and deduplicate
        let mut ptn_list = vec![ne_ptn, nw_ptn, se_ptn, sw_ptn];
        ptn_list.dedup();
        ptn_list
    }
}

#[cfg(test)]
mod tests {

    use approx::assert_relative_eq;

    use super::*;

    /// Ensures that coordinates are being set correctly based on the provided
    /// points
    #[test]
    fn test_from_points() {
        let ne: Point = (-1.3387398, 51.0012009).into();
        let sw: Point = (-1.4242919, 50.9553663).into();

        let result = BBox::from_points(&ne, &sw);

        let target = BBox {
            max_lat: 51.0012009,
            max_lon: -1.3387398,
            min_lat: 50.9553663,
            min_lon: -1.4242919,
        };

        assert_eq!(result, target)
    }

    /// Ensures that the centre point of the bounding box is being calculated
    /// properly
    #[test]
    fn test_get_centre() {
        let bbox = BBox {
            max_lat: 51.0012009,
            max_lon: -1.3387398,
            min_lat: 50.9553663,
            min_lon: -1.4242919,
        };

        let (res_lat, res_lon) = bbox.get_centre();

        let (tgt_lat, tgt_lon) = (50.9782836, -1.38151585);

        assert_relative_eq!(res_lat, tgt_lat);
        assert_relative_eq!(res_lon, tgt_lon);
    }

    /// Ensures that the partition list for the bounding box is being
    /// generated correctly, with no duplicates
    #[test]
    fn test_get_partition_list() {
        let bbox = BBox {
            max_lat: 51.0012009,
            max_lon: -1.3387398,
            min_lat: 50.9553663,
            min_lon: -1.4242919,
        };

        let result = bbox.get_partition_list();

        let target: Vec<String> =
            vec!["'51_n1'".to_string(), "'50_n1'".to_string()];

        assert_eq!(result, target)
    }
}
