"""Defines the (blank) plots which make up the route finder page"""

import dash_leaflet as dl
from dash import dcc
from plotly import graph_objects as go

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
