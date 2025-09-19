"""Sets the layout for the route finder page"""

import dash_bootstrap_components as dbc
from dash import dcc

from fell_viewer.content.route_finder.components import (
    blank_map,
    blank_profile,
    error_toast,
    progress_bar,
    sidebar_contents,
)

sidebar = dbc.Col(width=3, children=sidebar_contents, class_name="z-2")


plots = dbc.Col(
    width=9,
    children=[
        dcc.Store(id="route-store", storage_type="memory"),
        dcc.Store(id="route-current-job", storage_type="memory"),
        dcc.Store(id="route-last-job-status", storage_type="memory"),
        dcc.Download(id="route-download"),
        dbc.Row(progress_bar),
        dbc.Row(blank_map, style={"height": "calc(100vh - 284px)"}),
        dbc.Row(blank_profile),
    ],
    class_name="d-flex flex-column z-2",
)

layout = dbc.Row(children=[sidebar, plots, error_toast])
