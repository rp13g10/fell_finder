"""Sets the layout for the route finder page"""

from dash import html, dcc
from fell_finder.app.content.route_finder.components import (
    sidebar_contents,
    progress_bar_wrapper,
    blank_map,
    blank_profile,
)
from fell_finder.app.content.route_finder.callbacks import init_callbacks

sidebar = html.Div(className="col-3", children=sidebar_contents)


plots = html.Div(
    className="col-9",
    children=[
        dcc.Store(id="route-store", storage_type="memory"),
        html.Div(
            className="container-fluid py-2",
            style={"overflow": "auto"},
            children=html.Div(
                className="d-flex flex-row flex-nowrap", id="route-cards"
            ),
        ),
        html.Div(
            className="row",
            children=html.Div(
                className="col-12", children=progress_bar_wrapper
            ),
        ),
        html.Div(
            className="row",
            children=[html.Div(className="col-12", children=blank_map)],
        ),
        html.Div(
            className="row",
            children=[html.Div(className="col-12", children=blank_profile)],
        ),
    ],
)

layout = html.Div(
    className="container",
    children=[html.Div(className="row", children=[sidebar, plots])],
)

init_callbacks()
