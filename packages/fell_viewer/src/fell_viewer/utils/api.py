"""Contains the functions required to interface with the API provided by
fell_finder"""

import json
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


def get_user_requested_route(
    config: RouteConfig,
) -> list[Route]:
    """Based on the user-provided configuration, generate routes which match
    their requirements and return them for use in the webapp.

    Args:
        config: The user-provided configuration for route creation

    Returns:
        A list of generated routes

    """

    url = _gen_query_url(config)

    response = requests.get(url, headers={"Content-Type": "application/json"})

    # TODO: Set other codes to display toast popups when progress bar is
    #       reinstated
    if response.status_code != 200:
        return []

    raw_routes = json.loads(response.content)

    generated = []
    for route in raw_routes:
        generated.append(Route.from_api_response(route))

    return generated


def get_route_without_adjustments(
    config: RouteConfig,
) -> tuple[list[Route], dict[str, int]]:
    """Simply request routes using the provided config, without making any
    automatic adjustments to improve completion rates. As this is most likely
    to be called during debugging/optimisation, this also returns some extra
    metrics about the route generation process.

    Args:
        config: The user-provided configuration for route creation

    Returns:
        A tuple containing a list of generated routes, and a dict containing
        additional metrics.

    """
    url = _gen_query_url(config)

    response = requests.get(url, headers={"Content-Type": "application/json"})
    raw_response = json.loads(response.content)
    raw_routes = raw_response["routes"]
    raw_metrics = raw_response["metrics"]

    generated = []
    for route in raw_routes:
        generated.append(Route.from_api_response(route))

    return generated, raw_metrics
