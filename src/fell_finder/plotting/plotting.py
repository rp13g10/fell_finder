"""Some basic functions for plotting routes and their elevation profiles,
this will be a prime target for development once work on the webapp gets
underway."""

from typing import List

from networkx import DiGraph
from plotly import graph_objects as go

from fell_finder.containers.routes import Route


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


def unpack_route_details(cond_graph: DiGraph, route: Route) -> List[int]:
    """_summary_

    Args:
        graph (DiGraph): _description_
        route (Route): _description_

    Returns:
        List[int]: _description_
    """
    visited_points = []

    last_id = None
    for node_id in route.route:
        if last_id is not None:
            last_edge = cond_graph[last_id][node_id]
            if "via" in last_edge:
                visited_points += last_edge["via"]

        visited_points.append(node_id)

        last_id = node_id

    return visited_points


def plot_elevation_profile(
    full_graph: DiGraph, cond_graph: DiGraph, route: Route
) -> go.Figure:
    """For a generated route, generate a plotly graph which displays the
    elevation profile.

    Args:
        route (Route): A route generated by a RouteMaker"""

    # visited_points = unpack_route_details(graph, route)
    cml_dist = 0
    last_id = None
    dists, eles = [], []
    for node_id in unpack_route_details(cond_graph, route):
        node = full_graph.nodes[node_id]
        ele = node["elevation"]

        if last_id is None:
            step_dist = 0
        else:
            step_dist = full_graph[last_id][node_id]["distance"]

        cml_dist += step_dist
        last_id = node_id

        eles.append(ele)
        dists.append(cml_dist)

    route_trace = go.Scatter(
        mode="lines+markers", x=dists, y=eles, line=dict(shape="spline")
    )

    layout = go.Layout(title="Elevation Profile")

    figure = go.Figure(data=[route_trace], layout=layout)

    return figure


def plot_route(
    full_graph: DiGraph, cond_graph: DiGraph, route: Route
) -> go.Figure:
    """For a generated route, generate a Plotly graph which plots it onto
    a mapbox map.

    Args:
        graph (Graph): A graph containing latitude & longitude information for
          every node visited in the provided route
        route (Route): A route generated by a RouteMaker

    Returns:
        go.Figure: A mapbox plot of the provided route
    """
    last_id = None
    cml_dist = 0
    lats, lons, eles, dists = [], [], [], []
    for node_id in unpack_route_details(cond_graph, route):
        node = full_graph.nodes[node_id]
        lat = node["lat"]
        lon = node["lon"]
        ele = node["elevation"]

        if last_id is None:
            step_dist = 0
        else:
            step = full_graph[last_id][node_id]
            step_dist = step["distance"]

            if "via" in step:
                for lat, lon, ele in step["via"]:
                    lats.append(lat)
                    lons.append(lon)
                    eles.append(ele)
                    dists.append(None)

        cml_dist += step_dist
        last_id = node_id

        lats.append(lat)
        lons.append(lon)
        eles.append(ele)
        dists.append(cml_dist)

    text = [
        f"Distance: {dist:,.2f}km\nElevation: {ele:,.2f}m" if dist else ""
        for ele, dist in zip(eles, dists)
    ]

    route_trace = go.Scattermapbox(
        mode="lines",
        lat=lats,
        lon=lons,
        text=text,
    )
    se_trace = go.Scattermapbox(
        mode="markers", lat=lats[0:1], lon=lons[0:1], marker=dict(size=20)
    )

    distance = route.distance
    elevation = route.elevation_gain
    title = f"Distance: {distance}, Elevation: {elevation}"

    layout = go.Layout(
        # margin={"l": 0, "t": 0, "r": 0, "l": 0},
        mapbox={"center": {"lon": lons[0], "lat": lats[0]}},
        mapbox_style="open-street-map",
        mapbox_zoom=10,
        title=title,
        hovermode="closest",
    )

    fig = go.Figure(data=[route_trace, se_trace], layout=layout)

    return fig
