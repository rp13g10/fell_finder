"""This temporary script is being set up to collect data to tune route
generation parameters. The intention is to maximise the quality of generated
routes, while keeping run times as short as possible.

Max no. of candidate routes held in memory during the route finding process
is currently set statically. For future builds the intention is to set it
according to the size of the graph."""

from typing import Any

import pandas as pd
from fell_viewer.common.containers import RouteConfig
from fell_viewer.utils.api import get_route_without_adjustments

config = RouteConfig(
    start_lat=51.0428188,
    start_lon=-1.3476727,
    target_distance=1000.0,
    route_mode="hilly",
    max_candidates=64,
    restricted_surfaces_perc=0.0,
    restricted_surfaces=[],
    highway_types=[
        "motorway",
        "motorway_link",
        "trunk",
        "trunk_link",
        "primary_link",
        "secondary",
        "secondary_link",
        "tertiary",
        "tertiary_link",
        "residential",
        "living_street",
        "track",
        "road",
        "pedestrian",
        "footway",
        "bridleway",
        "steps",
        "corridor",
        "path",
        "sidewalk",
        "crossing",
        "traffic_island",
        "cycleway",
        "lane",
        "shared_busway",
        "shared_lane",
        "unclassified",
    ],
    surface_types=[
        "paved",
        "asphalt",
        "chipseal",
        "concrete",
        "concrete:lanes",
        "concrete:plates",
        "paving_stones",
        "sett",
        "unhewn_cobblestone",
        "cobblestone",
        "unpaved",
        "compacted",
        "fine_gravel",
        "shells",
        "rock",
        "pebblestone",
        "grass_paver",
        "woodchips",
        "ground",
        "dirt",
        "earth",
        "grass",
        "mud",
        "snow",
        "ice",
        "unclassified",
    ],
    distance_tolerance=0.1,
)

records = []
for target_distance in range(5000, 11000, 1000):
    for start_lat, start_lon, label in [
        (51.0428188, -1.3476727, "Winchester"),
        (50.9873551, -1.406696, "Southampton Common"),
        (51.089682, -1.161272, "New Alresford"),
        (50.9539367, -1.2120688, "Bishop's Waltham"),
        (50.961510, -0.978346, "QECP"),
        (51.004783, -1.4752269, "Romsey"),
        (51.077385, -1.4896039, "King's Somborne"),
        (50.904031, -1.0708209, "Denmead"),
    ]:
        for max_candidates in [
            *range(128, 1024 + 128, 128),
            *range(2048, 16385, 1024),
        ]:
            config.target_distance = float(target_distance)
            config.max_candidates = max_candidates
            config.start_lat = start_lat
            config.start_lon = start_lon

            output: dict[str, Any] = {
                "start_lat": start_lat,
                "start_lon": start_lon,
                "label": label,
                "target_distance": target_distance,
                "max_candidates": max_candidates,
            }

            routes, metrics = get_route_without_adjustments(config)
            output.update(metrics)

            output["min_ele"] = (
                min(x.metrics.gain for x in routes) if routes else None
            )
            output["max_ele"] = (
                max(x.metrics.gain for x in routes) if routes else None
            )
            output["min_dist"] = (
                min(x.metrics.dist for x in routes) if routes else None
            )
            output["max_dist"] = (
                max(x.metrics.dist for x in routes) if routes else None
            )

            records.append(output)

            records_df = pd.DataFrame.from_records(records)
            records_df.to_csv("tuning.csv", header=True, index=False)
