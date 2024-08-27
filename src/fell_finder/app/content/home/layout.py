"""Defines the layout of the webapp home page"""

from dash import html

layout = html.Div(
    className="container",
    children=[
        html.Div(
            className="row",
            children=html.Div(
                className="col-12", children="Welcome to the Home page!"
            ),
        )
    ],
)
