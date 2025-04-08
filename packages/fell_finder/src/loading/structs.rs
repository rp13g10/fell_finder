/// Sets the data which will be stored as weights in the petgraph graph, all
/// attributes will be populated after the graph is created
#[derive(Default, Debug, Clone, Copy)]
pub struct NodeData {
    pub id: i64,
    pub lat: f64,
    pub lon: f64,
    pub elevation: f64,
    pub is_start: bool,
    pub dist_to_start: Option<f64>,
}

/// Container for the raw output of the nodes SQL query
#[derive(sqlx::FromRow, Debug, Clone, Copy)]
pub struct NodeRow {
    pub id: i64,
    lat: f64,
    lon: f64,
    elevation: f64,
}

impl NodeRow {
    /// Unpack the raw node data into a format which can be loaded into the
    /// graph
    pub fn prepare(self) -> NodeData {
        NodeData {
            id: self.id,
            lat: self.lat,
            lon: self.lon,
            elevation: self.elevation,
            is_start: false, // default, will be overwritten``
            dist_to_start: None, // default, will be overwritten
        }
    }
}

/// Container for edge metadata which will be stored in the graph, all
/// attributes are populated based on the output of a SQL query
#[derive(Default, Debug, Clone)]
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

/// Container for the raw output of the edges SQL query
#[derive(sqlx::FromRow, Debug)]
pub struct EdgeRow {
    pub src: i64,
    pub dst: i64,
    highway: String,
    surface: String,
    elevation_gain: f64,
    elevation_loss: f64,
    distance: f64,
    lats: Vec<f64>,
    lons: Vec<f64>,
    eles: Vec<f64>,
    dists: Vec<f64>,
}

impl EdgeRow {
    /// Unpack the raw edge data into a format which can be loaded into the
    /// graph
    pub fn prepare(self) -> (i64, i64, EdgeData) {
        let data = EdgeData {
            src: self.src,
            dst: self.dst,
            highway: self.highway,
            surface: self.surface,
            elevation_gain: self.elevation_gain,
            elevation_loss: self.elevation_loss,
            distance: self.distance,
            lats: self.lats,
            lons: self.lons,
            eles: self.eles,
            dists: self.dists,
        };

        (self.src, self.dst, data)
    }
}
