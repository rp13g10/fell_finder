COPY routing.edges (
    src,
    dst,
    src_lat,
    src_lon,
    highway,
    surface,
    elevation_gain,
    elevation_loss,
    distance,
    lats,
    lons,
    eles,
    dists,
    ptn
)
FROM
    STDIN