SELECT
    id,
    lat,
    lon,
    elevation
FROM routing.nodes
WHERE ptn IN (< ptn_str >)
AND (lat BETWEEN < min_lat > AND < max_lat >)
AND (lon BETWEEN < min_lon > AND < max_lon >)