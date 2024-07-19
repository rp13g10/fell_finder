from dash import html, dcc

from layout.navbar import navbar
from layout.route_maker import route_maker

index = html.Div(children=[navbar, route_maker])
