"""Utility functions which can be used to store & retrieve in from a
browser session"""

import json
from typing import Dict, List
from dataclasses import fields
from fell_finder.app.utils.plotting import CompletedRoute


def store_route_to_dict(route: CompletedRoute) -> Dict:
    """Quick convenience function which stores all of the attributes in a
    CompletedRoute into a dictionary, so that it can be written to a JSON
    object

    Args:
        route: A completed route

    Returns:
        A dictionary containing all of the route's attributes
    """
    route_fields = fields(route)

    route_dict = {
        field.name: getattr(route, field.name)
        for field in route_fields
        if field.name not in {"current_position", "visited", "route", "points"}
    }

    return route_dict


def load_route_from_dict(route_dict: Dict) -> CompletedRoute:
    """Quick convenience function which returns a completed route object based
    on a dictionary

    Args:
        route_dict: A dictionary containing all attributes of a completed route

    Returns:
        A completed route object
    """
    route = CompletedRoute(**route_dict)

    return route


def store_routes_to_str(routes: List[CompletedRoute]) -> str:
    """Quick convenience function which allows the storing of a list of route
    objects as a single string. Required in order to store the routes in
    memory on the client device

    Args:
        routes: A list of completed routes to be stored

    Returns:
        A string representing the completed routes
    """
    route_list = [store_route_to_dict(route) for route in routes]
    route_str = json.dumps(route_list)
    return route_str


def load_routes_from_str(route_str: str) -> List[CompletedRoute]:
    """Quick convenience function which unpacks a string representing a list
    of completed routes into a true list of completed route objects

    Args:
        route_str: String representing multiple completed routes

    Returns:
        A true list of completed route objects
    """
    route_list = json.loads(route_str)
    routes = [CompletedRoute(**route_dict) for route_dict in route_list]
    return routes
