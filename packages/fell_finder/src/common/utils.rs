use std::io::{Error, ErrorKind};

/// For a provided vector of floats, retrieve the minimum and maximum values
/// and return them as a tuple. If an empty vector is provided, an Error will
/// be returned
pub fn get_min_max_vals(vals: &Vec<f64>) -> Result<(f64, f64), Error> {
    let min_val: f64;
    let max_val: f64;

    if let Some(val) = vals.iter().min_by(|a, b| a.total_cmp(b)) {
        min_val = *val;
    } else {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "Unable to get minimum value, maybe the input vector is empty?",
        ));
    }

    if let Some(val) = vals.iter().max_by(|a, b| a.total_cmp(b)) {
        max_val = *val;
    } else {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "Unable to get maximum value, maybe the input vector is empty?",
        ));
    }

    Ok((min_val, max_val))
}
