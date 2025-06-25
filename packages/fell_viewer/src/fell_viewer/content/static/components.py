"""Defines static page elements, which are displayed regardless of the page
the user is accessing."""

import dash_bootstrap_components as dbc
from dash import dcc

url_bar = dcc.Location(id="url", refresh="callback-nav")

nav_item = dbc.NavItem(dbc.NavLink("Home", href="home"))
dropdown = dbc.DropdownMenu(
    [dbc.DropdownMenuItem("Route Finder", href="route_finder")],
    in_navbar=True,
    nav=True,
    align_end=True,
    label="More",
)

page_header = dbc.Navbar(
    children=[
        dbc.Container(
            [
                dbc.NavbarBrand("Fell Finder"),
                dbc.NavbarToggler(id="navbar-toggler", n_clicks=0),
                dbc.Collapse(
                    dbc.Nav([nav_item, dropdown], navbar=True),
                    id="navbar-collapse",
                    navbar=True,
                    class_name="d-flex justify-content-end",
                ),
            ],
            fluid=True,
        ),
    ],
    color="secondary",
    fixed="top",
    sticky="top",
    class_name="mb-2",
)
