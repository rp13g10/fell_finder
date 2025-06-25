"""Sets the layout for the route finder page"""

from dash import dcc
import dash_bootstrap_components as dbc

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
        dbc.Row(
            children=[dbc.Col(width=12, children=blank_map)],
        ),
        dbc.Row(
            children=[dbc.Col(width=12, children=blank_profile)],
        ),
    ],
)

layout = dbc.Row(children=[sidebar, plots])


init_callbacks()
