"""Quick script which has been set up to check that updates to the
fell_finder API are working as expected"""

import json
import os
import time
from dataclasses import dataclass, field, fields
from typing import Literal

import requests


@dataclass
class RouteConfig:
    """Contains user configuration options for route calculation

    Args:
        start_lat: Latitude for the route start point
        start_lon: Longitude for the route start point
        target_distance: Target distance for the route (in m)
        tolerance: How far above/below the target distance a completed route
          can be while still being considered valid
        route_mode: Set to 'hilly' to generate the hilliest possible
          route, or 'flat' for the flattest possible route
        highway_types: The different types of terrain which should
          be considered for this route. If not provided, all terrain types
          will be considered. Defaults to None.
        surface_types: The different  types of surface which should be
          considered for this route. If not provided, all surface types will be
          considered. Defaults to None.

    """

    start_lat: float
    start_lon: float
    route_mode: Literal["hilly", "flat"]
    target_distance: float

    restricted_surfaces_perc: float
    distance_tolerance: float = float(
        # TODO: Set this as a user input
        os.environ.get("FF_DIST_TOLERANCE", "0.1")
    )

    highway_types: list[str] = field(default_factory=list)
    surface_types: list[str] = field(default_factory=list)
    restricted_surfaces: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Calculate min_distance and max_distance based on user provided
        target_distance and tolerance"""

        # Ensure inputs are stored using the expected datatype
        for attr in [
            "start_lat",
            "start_lon",
            "target_distance",
            "restricted_surfaces_perc",
        ]:
            setattr(self, attr, float(getattr(self, attr)))


def _gen_query_url(config: RouteConfig) -> str:
    base_url = "http://localhost:8000/route/request"

    query = []
    for field_ in fields(config):
        field_val = getattr(config, field_.name)
        if not field_val:
            continue
        if isinstance(field_val, list):
            field_val = ",".join(field_val)
        query.append(f"{field_.name}={field_val}")
    query = "&".join(query)

    url = f"{base_url}?{query}"

    return url


def _gen_status_url(job_id: str) -> str:
    base_url = "http://localhost:8000/route/status"

    return f"{base_url}?job_id={job_id}"


def _gen_retrieve_url(job_id: str) -> str:
    base_url = "http://localhost:8000/route/retrieve"

    return f"{base_url}?job_id={job_id}"


config = RouteConfig(
    start_lat=51.0428188,
    start_lon=-1.3476727,
    target_distance=5000.0,
    route_mode="hilly",
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

req_url = _gen_query_url(config)

req_resp = requests.get(req_url, headers={"Content-Type": "application/json"})
job_id = json.loads(req_resp.content)["job_id"]

stat_url = _gen_status_url(job_id)
res_url = _gen_retrieve_url(job_id)

finished = False
iter = 0
while not finished:
    stat_resp = requests.get(
        stat_url, headers={"Content-Type": "application/json"}
    )
    content = json.loads(stat_resp.content)
    print(f"{iter} - {stat_resp.status_code}: {content}")

    finished = content["status"] == "success"
    time.sleep(1)
    iter += 1

routes = requests.get(res_url, headers={"Content-Type": "application/json"})

routes = json.loads(routes.content)
print(f"{len(routes)} routes generated")
