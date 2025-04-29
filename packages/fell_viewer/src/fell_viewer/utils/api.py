"""Contains the functions required to interface with the API provided by
fell_finder"""

import json
from dataclasses import fields

import requests

from fell_viewer.containers.config import RouteConfig
from fell_viewer.containers.routes import Route


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


def get_user_requested_route(config: RouteConfig) -> list[Route]:
    """Based on the user-provided configuration, generate routes which match
    their requirements and return them for use in the webapp.

    Args:
        config: The user-provided configuration for route creation

    Returns:
        A list of generated routes

    """

    generated = []
    for mult in [1, 2, 2, 2]:
        config.max_candidates *= mult

        url = _gen_query_url(config)

        response = requests.get(
            url, headers={"Content-Type": "application/json"}
        )

        raw_routes = json.loads(response.content)

        for route in raw_routes:
            generated.append(Route.from_api_response(route))

        if generated:
            break

    return generated
