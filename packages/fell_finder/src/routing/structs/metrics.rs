//! Defines the structs which contain high level information about a route
//! (i.e. total distance, total gain, etc)

use crate::loading::structs::EdgeData;
use rustc_hash::FxHashMap;
use serde::Serialize;

/// Container for the metrics which are relevant to both candidates and routes
#[derive(Debug, Clone, Serialize)]
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
        // TODO: Update this to use total dist, may need to implement as a
        //       method of candidate directly
        let potential = self.get_gain_potential();
        (self.gain + potential) / self.dist
    }
}

/// Container for the overall metrics of a candidate route
#[derive(Clone)]
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
#[derive(Debug, Serialize)]
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
