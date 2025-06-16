"""Defines the individual components which make up the route finder page"""

import json
import os

import dash_leaflet as dl
from dash import dcc, html
from plotly import graph_objects as go

from fell_viewer.elements.buttons import Button, ButtonConfig
from fell_viewer.components.control_panel import (
    Control,
    SectionHeader,
    SectionSubHeader,
    Panel,
)
from fell_viewer.content.route_finder.generators import (
    generate_progress_bar,
)

CUR_DIR = os.path.dirname(os.path.abspath(__file__))
with open(
    os.path.join(CUR_DIR, "highway_types.json"), "r", encoding="utf8"
) as fobj:
    HIGHWAY_TYPES = list(json.load(fobj).keys())
with open(
    os.path.join(CUR_DIR, "surface_types.json"), "r", encoding="utf8"
) as fobj:
    SURFACE_TYPES = list(json.load(fobj).keys())


blank_map = dl.Map(
    id="route-plot",
    children=[
        dl.TileLayer(id="route-plot-tiles"),
        dl.ScaleControl(position="bottomleft", id="route-plot-scale"),
        dl.Marker(
            position=[50.9690528, -1.3832098],  # type: ignore
            id="route-plot-marker",
        ),
    ],
    center=[50.9690528, -1.3832098],  # type: ignore
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

sidebar_contents = Panel(
    title="Route Configuration",
    controls=[
        SectionHeader(title="Distance Selection"),
        Control(
            title=None,
            control=dcc.Input(
                type="range",
                value=1,
                min=1,
                max=50,
                step=1,
                id="route-dist",
            ),
        ),
        Control(
            title=None,
            control=html.Div(className="text", id="route-dist-display"),
        ),
        SectionHeader(title="Route Preferences"),
        Control(
            title="Profile",
            control=dcc.Dropdown(
                options=[
                    {"label": "Hilly", "value": "hilly"},
                    {"label": "Flat", "value": "flat"},
                ],
                value="hilly",
                id="route-mode",
            ),
        ),
        Control(
            title="Way Types",
            control=dcc.Dropdown(
                options=HIGHWAY_TYPES,
                value=HIGHWAY_TYPES,
                multi=True,
                id="route-highway",
            ),
        ),
        Control(
            title="Surfaces",
            control=dcc.Dropdown(
                options=SURFACE_TYPES,
                value=[x for x in SURFACE_TYPES if "*" not in x],
                multi=True,
                id="route-allowed-surfaces",
            ),
        ),
        SectionHeader(title="Surface Restriction"),
        SectionSubHeader(title="Restricted Surfaces"),
        Control(
            title=None,
            control=dcc.Dropdown(
                options=SURFACE_TYPES,
                value=[],
                multi=True,
                id="route-restricted-surfaces",
            ),
        ),
        SectionSubHeader(title=r"Max allowed % of total"),
        Control(
            title=None,
            control=dcc.Input(
                type="range",
                value=0.0,
                min=0,
                max=1,
                step=0.01,
                id="route-restricted-perc",
            ),
        ),
        Control(
            title=None,
            control=html.Div(
                className="text", id="route-restricted-perc-display"
            ),
        ),
        # TODO: Tidy up handling of rows of buttons
        # TODO: Move download button onto cards, or the main page
        # TODO: Set up controls to automatically call .generate as required
        Control(
            title=None,
            control=Button(
                config=ButtonConfig(
                    name="Calculate", colour="blue", id="route-download-button"
                )
            ).generate(),
        ),
        Control(
            title=None,
            control=Button(
                config=ButtonConfig(
                    name="Download", colour="blue", id="route-download-button"
                )
            ).generate(),
        ),
    ],
)


progress_bar_wrapper = html.Div(
    className="col-12",
    children=html.Div(
        id="progress-bar",
        className="progress",
        children=generate_progress_bar(0, 100, 0, 0),
    ),
)
