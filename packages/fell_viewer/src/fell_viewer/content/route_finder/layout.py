"""Sets the layout for the route finder page"""

import dash_bootstrap_components as dbc
from dash import dcc

from fell_viewer.content.route_finder.callbacks import init_callbacks
from fell_viewer.content.route_finder.components import (
    blank_map,
    blank_profile,
    sidebar_contents,
)

sidebar = dbc.Col(width=3, children=sidebar_contents)


plots = dbc.Col(
    width=9,
    children=[
        dcc.Store(id="route-store", storage_type="memory"),
        dcc.Download(id="route-download"),
        dbc.Row(blank_map, style={"height": "calc(100vh - 256px)"}),
        dbc.Row(blank_profile),
    ],
    class_name="d-flex flex-column",
)

layout = dbc.Row(children=[sidebar, plots])


init_callbacks()
