"""Defines static page elements, which are displayed regardless of the page
the user is accessing."""

from dash import html

from fell_viewer.common.grid import NavbarConfig, Navbar
from fell_viewer.elements.buttons import (
    NavButtonConfig,
    NavButton,
)

page_header = Navbar(
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
    config=NavbarConfig(classnames=["py-1", "my-0"], footer=False),
)

page_footer = Navbar(
    children=html.Div(
        className="btn-toolbar ml-1 w-100",
        role="toolbar",
        children=[
            NavButton(
                NavButtonConfig(name="Home", link="home", colour="green")
            ).generate(),
            NavButton(
                NavButtonConfig(
                    name="Route Finder", link="route_finder", colour="blue"
                )
            ).generate(),
        ],
    ),
    config=NavbarConfig(footer=True, classnames=None),
)
