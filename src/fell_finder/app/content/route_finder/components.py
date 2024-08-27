"""Defines the individual components which make up the route finder page"""

from dash import html, dcc
from plotly import graph_objects as go
import dash_leaflet as dl
from fell_finder.app.content.route_finder.generators import (
    generate_progress_bar,
)
from fell_finder import app_config

blank_map = dl.Map(
    id="route-plot",
    children=[
        dl.TileLayer(id="route-plot-tiles"),
        dl.ScaleControl(position="bottomleft", id="route-plot-scale"),
        dl.Marker(position=[50.9690528, -1.3832098], id="route-plot-marker"),
    ],
    center=[50.9690528, -1.3832098],
    style={"width": "100%", "height": "50vh"},
    zoom=14,
)

blank_profile = dcc.Graph(
    id="route-profile",
    figure=go.Figure(
        data=go.Scatter(),
        layout=go.Layout(margin=dict(l=20, r=20, t=40, b=20)),
    ),
    style={"width": "100%", "height": "20vh"},
)

HIGHWAY_TYPES = list(app_config["highway_types"].keys())
SURFACE_TYPES = list(app_config["surface_types"].keys())

sidebar_contents = [
    html.Div(className="sidebar-heading", children="Route Configuration"),
    html.Div(
        className="list-group list-group-flush",
        children=[
            # TODO: Add an element to show selected value
            html.Div(
                className="list-group-item",
                children=[
                    html.Div(className="text", id="route-dist-display"),
                    dcc.Input(
                        type="range",
                        value=1,
                        min=1,
                        max=50,
                        step=1,
                        id="route-dist",
                    ),
                ],
            ),
            html.Div(
                className="list-group-item",
                children=dcc.Dropdown(
                    options=[
                        {"label": "Hilly", "value": "hilly"},
                        {"label": "Flat", "value": "flat"},
                    ],
                    value="hilly",
                    id="route-mode",
                ),
            ),
            html.Div(
                className="list-group-item",
                children=dcc.Dropdown(
                    options=HIGHWAY_TYPES,
                    value=HIGHWAY_TYPES,
                    multi=True,
                    id="route-highway",
                ),
            ),
            html.Div(
                className="list-group-item",
                children=dcc.Dropdown(
                    options=SURFACE_TYPES,
                    value=SURFACE_TYPES,
                    multi=True,
                    id="route-surface",
                ),
            ),
            html.Div(
                className="list-group-item",
                children=html.Div(
                    "Calculate",
                    id="route-calculate",
                    className="btn m-1 btn-primary",
                    role="button",
                ),
            ),
            html.Div(
                className="list-group-item",
                children=html.Div(
                    "Clear",
                    id="route-clear",
                    className="btn m-1 btn-danger disabled",
                    role="button",
                ),
            ),
        ],
    ),
]


progress_bar_wrapper = html.Div(
    className="col-12",
    children=html.Div(
        id="progress-bar",
        className="progress",
        children=generate_progress_bar(0, 100, 0, 0),
    ),
)
