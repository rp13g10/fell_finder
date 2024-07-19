import json

import dash_leaflet as dl
from celery import Celery
from dash import (
    Dash,
    html,
    dcc,
    callback,
    Output,
    Input,
    State,
    no_update,
    CeleryManager,
)

from fell_finder.containers.routes import RouteConfig
from fell_finder.routing.route_maker import RouteMaker

from fell_finder.plotting.plotting import (
    generate_route_polyline,
    plot_elevation_profile,
)

from index import index
from layout.route_maker import update_progress_bar

# TODO: Update route plotting functions to account for new format of vias, lat
#       and lon are now available in the vias

DATA_DIR = "/home/ross/repos/fell_finder/data"


celery_app = Celery(
    __name__,
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/1",
)
background_callback_manager = CeleryManager(celery_app)

app = Dash(__name__)

app.layout = index


@callback(
    Output("route-plot", "children", allow_duplicate=True),
    Input("route-plot", "clickData"),
    State("route-plot", "children"),
    prevent_initial_call=True,
)
def show_clicked_point_on_map(click_data, current_children):
    if click_data is None:
        return no_update

    lat = click_data["latlng"]["lat"]
    lon = click_data["latlng"]["lng"]

    new_marker = dl.Marker(position=[lat, lon], id="route-plot-marker")

    # print("\n")
    # print(current_children)

    new_children = [
        child
        for child in current_children
        if child["props"]["id"] != "route-plot-marker"
    ]
    new_children.append(new_marker)

    return new_children


@callback(
    [
        Output("route-plot", "children", allow_duplicate=True),
        Output("route-profile", "figure"),
    ],
    Input("route-calculate", "n_clicks"),
    [
        State("route-plot", "children"),
        State("route-dist", "value"),
        State("route-mode", "value"),
        State("route-terrain", "value"),
    ],
    background=True,
    prevent_initial_call=True,
    progress=Output("progress-bar", "children"),
    manager=background_callback_manager,
)
def calculate_and_render_route(
    set_progress,
    n_clicks,
    current_children,
    route_dist,
    route_mode,
    route_terrain,
):
    if not n_clicks:
        return no_update, no_update

    current_marker = next(
        x for x in current_children if x["props"]["id"] == "route-plot-marker"
    )
    lat, lon = current_marker["props"]["position"]

    config = RouteConfig(
        start_lat=lat,
        start_lon=lon,
        target_distance=route_dist * 1000,
        route_mode=route_mode,
        max_candidates=256,
        tolerance=0.1,
        terrain_types=route_terrain,
    )

    maker = RouteMaker(config, DATA_DIR)
    for progress, routes in maker.find_routes():
        set_progress(
            update_progress_bar(
                cur_val=progress["avg_distance"],
                max_val=progress["max_distance"],
            )
        )

    route = routes[0]
    route_polyline = generate_route_polyline(maker.graph, route)

    new_children = [
        x for x in current_children if x["props"]["id"] != "route-plot-trace"
    ]
    new_children.append(route_polyline)

    profile_plot = plot_elevation_profile(maker.graph, route)

    return new_children, profile_plot


# @callback(Output('route-plot', 'children'),
#           Input('route-profile', 'hoverData'))
# def link_map_and_elevation_profile(hover_data):

#     selected_point = hover_data[0]

if __name__ == "__main__":
    app.run(debug=True)
