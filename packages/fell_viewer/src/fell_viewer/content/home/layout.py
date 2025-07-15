"""Defines the layout of the webapp home page"""

import dash_bootstrap_components as dbc

from fell_viewer.content.home.components import home_image, home_text

layout = dbc.Container(
    dbc.Row(
        [
            dbc.Col(home_text.generate(), width=8),
            dbc.Col(home_image.generate(), width=4),
        ],
    ),
    class_name="pt-3",
)
