from dash import html, dcc
from plotly import graph_objects as go
import dash_leaflet as dl

TERRAIN_TYPES = [
    "footway",
    "living_street",
    "path",
    "pedestrian",
    "primary",
    "primary_link",
    "residential",
    "secondary",
    "secondary_link",
    "service",
    "steps",
    "tertiary",
    "tertiary_link",
    "track",
    "unclassified",
]

BLANK_MAP = dl.Map(
    id="route-plot",
    children=[
        dl.TileLayer(id="route-plot-tiles"),
        dl.ScaleControl(position="bottomleft", id="route-plot-scale"),
        dl.Marker(position=[50.9690528, -1.3832098], id="route-plot-marker"),
    ],
    center=[50.9690528, -1.3832098],
    style={"width": "100%", "height": "50vh"},
    zoom=12,
)

BLANK_PROFILE = dcc.Graph(
    id="route-profile",
    figure=go.Figure(
        data=go.Scatter(),
        layout=go.Layout(margin=dict(l=20, r=20, t=20, b=20)),
    ),
)


sidebar = html.Div(
    className="col-3",
    children=[
        html.Div(className="sidebar-heading", children="Route Configuration"),
        html.Div(
            className="list-group list-group-flush",
            children=[
                html.Div(
                    className="list-group-item",
                    children=dcc.Input(
                        type="number", value=10, id="route-dist"
                    ),
                ),
                html.Div(
                    className="list-group-item",
                    children=dcc.Dropdown(
                        options=["hilly", "flat"],
                        value="hilly",
                        id="route-mode",
                    ),
                ),
                html.Div(
                    className="list-group-item",
                    children=dcc.Dropdown(
                        options=TERRAIN_TYPES,
                        value=TERRAIN_TYPES,
                        multi=True,
                        id="route-terrain",
                    ),
                ),
                html.Div(
                    className="list-group-item",
                    children=html.Button("Calculate", id="route-calculate"),
                ),
                html.Div(
                    className="list-group-item",
                    children=html.Button("Clear", id="route-clear"),
                ),
            ],
        ),
    ],
)


plots = html.Div(
    className="col-9",
    children=[
        html.Div(
            className="row",
            children=[
                html.Div(className="col-12", children=BLANK_MAP),
            ],
        ),
        html.Div(
            className="row",
            children=html.Div(className="col-12", children=BLANK_PROFILE),
        ),
    ],
)

route_maker = html.Div(
    className="container",
    children=[
        html.Div(
            className="row",
            children=[
                sidebar,
                plots,
            ],
        )
    ],
)
