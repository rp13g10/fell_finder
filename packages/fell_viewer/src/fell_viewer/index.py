"""Sets the high-level page layout, which includes any components which will
be rendered on all pages of the app"""

from dash import Input, Output, callback, html
from dash.development.base_component import Component
import dash_bootstrap_components as dbc

from fell_viewer.app import app, celery_app
from fell_viewer.content.static.components import (
    url_bar,
    page_header,
)
from fell_viewer.content import home, route_finder

page_content = dbc.Container(id="inx-page-content", fluid=True)

layout = dbc.Container(
    children=[url_bar, page_header, page_content],
    fluid=True,
    class_name="p-0 m-0",
)


@callback(Output("inx-page-content", "children"), [Input("url", "pathname")])
def inx_display_page(pathname: str) -> Component:
    """Key callback function which renders the requested page based on the
    current URL

    Args:
        pathname: The current URL, relative to the base address

    Returns:
        The content of the requested page

    """
    match pathname:
        case None | "/" | "/home":
            layout = home
        case "/route_finder":
            layout = route_finder
        case _:
            layout = html.Div(
                f"Something went wrong while loading page: {pathname}"
            )

    return layout


app.layout = layout

__all__ = ["app", "celery_app"]
