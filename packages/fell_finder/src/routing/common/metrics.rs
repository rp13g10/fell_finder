//! Defines the structs which contain high level information about a route
//! (i.e. total distance, total gain, etc)

use crate::common::graph_data::EdgeData;
use rustc_hash::FxHashMap;
use serde::Serialize;

/// Container for the metrics which are relevant to both candidates and routes
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct CommonMetrics {
    // TODO: Update metrics to use unsigned floats where appropriate
    pub dist: f64,
    pub gain: f64,
    pub loss: f64,
    pub s_dists: FxHashMap<String, f64>,
}

impl CommonMetrics {
    /// Determine the amount of elevation gain required to get back to the
    /// route start
    pub fn get_gain_potential(&self) -> f64 {
        if self.loss > self.gain {
            self.loss - self.gain
        } else {
            0.0
        }
    }

    /// Determine the amount of elevation loss required to get back to the
    /// route start
    pub fn get_loss_potential(&self) -> f64 {
        if self.gain < self.loss {
            self.gain - self.loss
        } else {
            0.0
        }
    }

    /// Determine the ratio of elevation gain to distance travelled
    pub fn get_ratio(&self) -> f64 {
        let potential = self.get_gain_potential();
        (self.gain + potential) / self.dist
    }
}

/// Container for the overall metrics of a candidate route
#[derive(Clone, PartialEq, Debug)]
pub struct CandidateMetrics {
    pub common: CommonMetrics,
}

impl CandidateMetrics {
    pub fn new() -> CandidateMetrics {
        CandidateMetrics {
            common: CommonMetrics {
                dist: 0.0,
                gain: 0.0,
                loss: 0.0,
                s_dists: FxHashMap::<String, f64>::default(),
            },
        }
    }
}

/// Coontainer for the overall metrics of a completed route
#[derive(Debug, Serialize, PartialEq)]
pub struct RouteMetrics {
    pub common: CommonMetrics,
}

impl CandidateMetrics {
    /// Update the metrics of a candidate to reflect their state after
    /// traversing the provided edge
    pub fn take_step(&mut self, edata: &EdgeData) {
        // Increment overall stats
        self.common.dist += edata.distance;
        self.common.gain += edata.elevation_gain;
        self.common.loss += edata.elevation_loss;

        // Track distance travelled across each surface type
        if let Some(s_dist) = self.common.s_dists.get_mut(&edata.surface) {
            *s_dist += edata.distance;
        } else {
            self.common
                .s_dists
                .insert(edata.surface.clone(), edata.distance);
        }
    }

    pub fn finalize(self) -> RouteMetrics {
        RouteMetrics {
            common: self.common,
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    /// Check behaviour when current position is above start point
    #[test]
    fn test_get_gain_potential_above_start() {
        let test_metrics = CommonMetrics {
            dist: 10.0,
            gain: 150.0,
            loss: 100.0,
            s_dists: FxHashMap::<String, f64>::default(),
        };

        let target = 0.0;

        let result = test_metrics.get_gain_potential();

        assert_eq!(result, target);
    }

    /// Check behaviour when current position is below start point
    #[test]
    fn test_get_gain_potential_below_start() {
        let test_metrics = CommonMetrics {
            dist: 10.0,
            gain: 100.0,
            loss: 150.0,
            s_dists: FxHashMap::<String, f64>::default(),
        };

        let target = 50.0;

        let result = test_metrics.get_gain_potential();

        assert_eq!(result, target);
    }

    /// Check that gain/dist ratios are calculated properly
    #[test]
    fn test_get_ratio() {
        let test_metrics = CommonMetrics {
            dist: 10.0,
            gain: 100.0,
            loss: 150.0,
            s_dists: FxHashMap::<String, f64>::default(),
        };

        let target = 15.0;

        let result = test_metrics.get_ratio();

        assert_eq!(result, target);
    }

    /// Check that step details are recorded properly
    #[test]
    fn test_take_step() {
        let edge_1 = EdgeData {
            src: 0,
            dst: 1,
            highway: "highway".to_string(),
            surface: "surface_1".to_string(),
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
            surface: "surface_2".to_string(),
            elevation_gain: 13.0,
            elevation_loss: 14.0,
            distance: 15.0,
            lats: vec![16.0, 17.0],
            lons: vec![18.0, 19.0],
            dists: vec![20.0, 21.0],
            eles: vec![22.0, 23.0],
        };

        let mut target_s_dists = FxHashMap::<String, f64>::default();
        target_s_dists.insert("surface_1".to_string(), 4.0);
        target_s_dists.insert("surface_2".to_string(), 15.0);

        let target = CandidateMetrics {
            common: CommonMetrics {
                dist: 19.0,
                gain: 15.0,
                loss: 17.0,
                s_dists: target_s_dists,
            },
        };

        let mut result: CandidateMetrics = CandidateMetrics::new();
        result.take_step(&edge_1);
        result.take_step(&edge_2);

        assert_eq!(result, target);
    }

    /// Check that the correct data is copied across
    #[test]
    fn test_finalize() {
        let common = CommonMetrics {
            dist: 0.0,
            gain: 1.0,
            loss: 2.0,
            s_dists: FxHashMap::<String, f64>::default(),
        };

        let test_candidate = CandidateMetrics {
            common: common.clone(),
        };

        let target = RouteMetrics {
            common: common.clone(),
        };

        let result = test_candidate.finalize();

        assert_eq!(result, target);
    }
}
