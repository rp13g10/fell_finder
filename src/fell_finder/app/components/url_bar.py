"""Defines an element which represents the content of the URL bar"""

from dash import html, dcc

url_bar = html.Div(
    [
        # Represents the URL bar, doesn't render anything
        dcc.Location(id="url", refresh="callback-nav")
    ]
)
