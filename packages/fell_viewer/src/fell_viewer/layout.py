"""Sets the high-level page layout, which includes any components which will
be rendered on all pages of the app"""

import dash_bootstrap_components as dbc

from fell_viewer.callbacks import init_callbacks
from fell_viewer.content.static.components import (
    page_header,
    url_bar,
)

page_content = dbc.Container(id="inx-page-content", fluid=True)

layout = dbc.Container(
    children=[url_bar, page_header, page_content],
    fluid=True,
    class_name="p-0 m-0",
)

init_callbacks()
