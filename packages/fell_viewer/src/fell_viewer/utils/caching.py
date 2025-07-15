"""Utility functions which can be used to help cache information as part of a
browser session, and retrieve it again"""

import json
from typing import List

from fell_viewer.common.containers import Route


def store_routes_to_str(routes: List[Route]) -> str:
    """Quick convenience function which allows the storing of a list of Route
    objects as a single string. Required in order to store the routes in
    memory on the client device

    Args:
        routes: A list of Route objects to be stored

    Returns:
        A string representing the completed routes

    """
    route_list = [route.to_dict() for route in routes]
    route_str = json.dumps(route_list)
    return route_str


def load_routes_from_str(route_str: str) -> List[Route]:
    """Quick convenience function which unpacks a string representing a list
    of routes into a list of Route objects

    Args:
        route_str: String representing multiple routes

    Returns:
        A list of Route objects

    """
    routes_list = json.loads(route_str)
    routes = [Route.from_dict(route_dict) for route_dict in routes_list]
    return routes
