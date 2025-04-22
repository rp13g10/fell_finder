"""Contains the functions required to interface with the API provided by
fell_finder"""

import json
from dataclasses import fields

import requests

from fell_viewer.containers.config import RouteConfig
from fell_viewer.containers.routes import Route


def get_user_requested_route(config: RouteConfig) -> list[Route]:
    """Based on the user-provided configuration, generate routes which match
    their requirements and return them for use in the webapp.

    Args:
        config: The user-provided configuration for route creation

    Returns:
        A list of generated routes

    """
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

    print("Generated URL")
    print(url)

    response = requests.get(url, headers={"Content-Type": "application/json"})

    print(response.status_code)
    print(response.content[:512])

    raw_routes = json.loads(response.content)

    generated = []
    for route in raw_routes:
        generated.append(Route.from_api_response(route))

    return generated
