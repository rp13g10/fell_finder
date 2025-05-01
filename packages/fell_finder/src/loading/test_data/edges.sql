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
    ptn IN ('0_0')
    AND highway IN ('highway_1')
    AND surface IN ('surface_1', 'surface_2')
    AND (src_lat BETWEEN -0.044966013570321796 AND 0.04496601357032179)
    AND (src_lon BETWEEN -0.044966027418013255 AND 0.044966027418013255)