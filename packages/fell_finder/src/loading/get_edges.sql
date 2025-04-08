SELECT
    src,
    dst,
    highway,
    surface,
    elevation_gain,
    elevation_loss,
    distance,
    lats,
    lons,
    eles,
    dists
FROM
    routing.edges
WHERE
    ptn IN (< ptn_str >)
    AND highway IN (< highway_str >)
    AND surface IN (< surface_str >)
    AND src_lat BETWEEN < min_lat > AND < max_lat >
    AND src_lon BETWEEN < min_lon > AND < max_lon >