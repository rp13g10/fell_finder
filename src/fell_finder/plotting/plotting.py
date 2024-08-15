"""Some basic functions for plotting routes and their elevation profiles,
this will be a prime target for development once work on the webapp gets
underway."""

from typing import List, Dict

import dash_leaflet as dl
from rustworkx import PyDiGraph

# from networkx import DiGraph
from plotly import graph_objects as go

from fell_finder.routing.containers import Route


def generate_filename(route: Route) -> str:
    """Generate a file name for the provided route.

    Args:
        route (Route): A populated route

    Returns:
        str: A name for the provided route
    """
    gain = route.elevation_gain
    dist = route.distance
    name = f"gain_{gain:,.2f}_dist_{dist:,.2f}"
    return name


# TODO: Use dash-leaflet to rewrite this function


def get_geometry_from_route(
    graph: PyDiGraph, route: Route
) -> Dict[str, List[float]]:
    lats = []
    lons = []
    elevations = []
    distances = []

    last_id = None
    cur_dist = 0.0
    for node_id in route.route:
        if last_id is not None:
            last_edge = graph.get_edge_data(last_id, node_id)
            geometry = last_edge.geometry

            lats += geometry["lat"]
            lons += geometry["lon"]
            elevations += geometry["elevation"]
            for step_dist in geometry["distance"]:
                cur_dist += step_dist
                distances.append(cur_dist)

        route_geometry = {
            "lats": lats,
            "lons": lons,
            "elevations": elevations,
            "distances": distances,
        }

        last_id = node_id

    route_geometry["points"] = route.route

    return route_geometry
