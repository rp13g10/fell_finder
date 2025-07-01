"""Contains the functions required to interface with the API provided by
fell_finder"""

import json
import os
from dataclasses import fields

import requests

from fell_viewer.common.containers import Route, RouteConfig


def _gen_query_url(config: RouteConfig) -> str:
    base_url = "http://localhost:8000/loop"

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


def _get_max_candidates(config: RouteConfig) -> int:
    # TODO: Move this into rust code, set based on graph size rather than
    #       requested distance

    usr_dist_km = config.target_distance // 1000

    min_cands = 128
    max_cands = int(os.environ.get("FF_MAX_CANDS", "8192")) // 2
    increment = 128

    cands = int(usr_dist_km * increment)

    if cands < min_cands:
        return min_cands
    if cands > max_cands:
        return max_cands
    return cands


def get_user_requested_route(config: RouteConfig) -> list[Route]:
    """Based on the user-provided configuration, generate routes which match
    their requirements and return them for use in the webapp.

    Args:
        config: The user-provided configuration for route creation

    Returns:
        A list of generated routes

    """

    max_candidates = _get_max_candidates(config)
    abs_max_candidates = int(os.environ.get("FF_MAX_CANDS", "8192"))

    generated = []
    while not generated:
        config.max_candidates = max_candidates

        url = _gen_query_url(config)

        response = requests.get(
            url, headers={"Content-Type": "application/json"}
        )

        raw_routes = json.loads(response.content)

        for route in raw_routes:
            generated.append(Route.from_api_response(route))

        if max_candidates >= abs_max_candidates:
            break

        max_candidates = min(abs_max_candidates, max_candidates * 2)

    return generated
