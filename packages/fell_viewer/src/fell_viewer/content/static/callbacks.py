"""Callbacks for components which will remain active at all times"""

from dash import Input, Output, State, callback


def init_callbacks() -> None:
    """This function needs to be called in a function which defines elements of
    the page config in order to ensure the enclosed callbacks are correctly
    registered"""

    # ruff: noqa: ANN202

    @callback(
        Output("navbar-collapse", "is_open"),
        [Input("navbar-toggler", "n_clicks")],
        [State("navbar-collapse", "is_open")],
    )
    def toggle_navbar_collapse(n_clicks: int, is_open: bool) -> bool:
        if n_clicks:
            return not is_open
        return is_open
