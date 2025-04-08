"""Set up the navbar which is displayed at the top of the page"""

from dash import html

navbar = html.Nav(
    className="navbar navbar-light bg-light py-1 my-0",
    children=html.Div(
        className="container-fluid",
        children=[
            html.Div(
                className="navbar-header",
                children=html.Div(
                    className="navbar-brand", children="Fell Finder"
                ),
            ),
            html.Div(
                className="navbar-text navbar-right",
                children="Ross' Pet Project!",
            ),
        ],
    ),
)
