"""Callbacks which are required in order to display the layout of the
entire webapp"""

from dash import Input, Output, callback, html
from dash.development.base_component import Component

from fell_viewer.content import home, route_finder


def init_callbacks() -> None:
    """This function needs to be called at the point the webapp layout is
    defined in order to ensure the enclosed callbacks are correctly
    registered"""

    @callback(
        Output("inx-page-content", "children"), [Input("url", "pathname")]
    )
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
