"""Quick script which has been set up to check that updates to the
fell_finder API are working as expected"""

import json
import time
from dataclasses import fields

import requests
from fell_viewer.common.containers import RouteConfig


def _gen_query_url(config: RouteConfig) -> str:
    base_url = "http://localhost:8000/route/request"

    query = []
    for field in fields(config):
        field_val = getattr(config, field.name)
        if not field_val:
            continue
        if isinstance(field_val, list):
            field_val = ",".join(field_val)
        query.append(f"{field.name}={field_val}")
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
    target_distance=10000.0,
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
    print(f"{iter} - {stat_resp.status_code}: {stat_resp.content}")

    finished = b"Success" in stat_resp.content
    time.sleep(1)
    iter += 1

routes = requests.get(res_url, headers={"Content-Type": "application/json"})

print(json.loads(routes.content))
