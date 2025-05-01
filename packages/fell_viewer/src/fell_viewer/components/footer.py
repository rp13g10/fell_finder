"""Set up the footer which is displayed at the bottom of the page"""

from dash import html

from fell_viewer.utils.element_generators import (
    NavButtonConfig,
    generate_nav_button,
)

footer = html.Nav(
    className="navbar-fixed-bottom navbar-light bg-light fixed-bottom",
    children=html.Div(
        className="btn-toolbar ml-1 w-100",
        role="toolbar",
        children=[
            generate_nav_button(
                NavButtonConfig(name="Home", link="home", colour="green")
            ),
            generate_nav_button(
                NavButtonConfig(
                    name="Route Finder", link="route_finder", colour="blue"
                )
            ),
        ],
    ),
)
