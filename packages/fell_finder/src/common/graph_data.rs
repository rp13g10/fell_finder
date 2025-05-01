/// Sets the data which will be stored as weights in the petgraph graph, all
/// attributes will be populated after the graph is created
#[derive(Default, Debug, Clone, Copy, PartialEq)]
pub struct NodeData {
    pub id: i64,
    pub lat: f64,
    pub lon: f64,
    pub elevation: f64,
    pub is_start: bool,
    pub dist_to_start: Option<f64>,
}

/// Container for edge metadata which will be stored in the graph, all
/// attributes are populated based on the output of a SQL query
#[derive(Default, Debug, Clone, PartialEq)]
pub struct EdgeData {
    pub src: i64,
    pub dst: i64,
    pub highway: String,
    pub surface: String,
    pub elevation_gain: f64,
    pub elevation_loss: f64,
    pub distance: f64,
    pub lats: Vec<f64>,
    pub lons: Vec<f64>,
    pub eles: Vec<f64>,
    pub dists: Vec<f64>,
}
