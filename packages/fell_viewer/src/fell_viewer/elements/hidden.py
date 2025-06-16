"""Defines an element which represents the content of the URL bar"""

from dash import dcc, html


def gen_url_bar(id_: str) -> html.Div:
    """Generate a div which exposes the current value in the URL bar. This is
    not visible to the end user, but is vital for a number of callbacks."""
    return html.Div([dcc.Location(id=id_, refresh="callback-nav")])
