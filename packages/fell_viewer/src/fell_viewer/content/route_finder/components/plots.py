"""Defines the (blank) plots which make up the route finder page"""

import dash_bootstrap_components as dbc
import dash_leaflet as dl
from dash import dcc
from plotly import graph_objects as go

from fell_viewer.common._fv_component import FVComponent
from fell_viewer.utils.encoding import get_image_as_str

ROUTE_START_PNG = get_image_as_str("route_start.png")


class RouteMap(FVComponent):
    """Generates a blank map element using dash_leaflet which can be used to
    render generated routes."""

    def __init__(self, id: str) -> None:
        self.id = id

    def _gen_blank_map(self) -> dl.Map:
        blank_map = dl.Map(
            id=self.id,
            children=[
                dl.TileLayer(id=f"{self.id}-tiles"),
                dl.ScaleControl(position="bottomleft", id=f"{self.id}-scale"),
                dl.Marker(
                    position=[50.9690528, -1.3832098],  # type: ignore
                    id=f"{self.id}-marker",
                    icon={
                        "iconUrl": ROUTE_START_PNG,
                        "iconSize": [48, 48],
                        "iconAnchor": [24, 48],
                    },
                ),
            ],
            center=[50.9690528, -1.3832098],  # type: ignore
            zoom=14,
            className="w-100 h-100",
        )

        return blank_map

    def generate(self) -> dbc.Container:
        """Produces a blank map element wrapped in a Container"""
        blank_map = self._gen_blank_map()
        wrapped = dbc.Container(
            blank_map,
        )

        return wrapped


class RouteProfile(FVComponent):
    """Generates a blank route profile using plotly which can be used to
    render the elevation profiles of generated routes"""

    def __init__(self, id: str) -> None:
        self.id = id

    # TODO: Add toggle to switch between elevation & cumulative gain

    def _gen_blank_profile(self) -> dcc.Graph:
        figure = go.Figure(
            data=go.Scatter(),
            layout=go.Layout(
                margin=dict(l=20, r=20, t=40, b=20),
                xaxis={"showgrid": False, "range": [0, 5000]},
                yaxis={"showgrid": False, "range": [0, 100]},
            ),
        )

        blank_profile = dcc.Graph(
            id=self.id,
            figure=figure,
            className="w-100 h-100",
        )

        return blank_profile

    def generate(self) -> dbc.Container:
        """Produces a blank elevation profile wrapped in a container. The"""
        blank_profile = self._gen_blank_profile()
        wrapped = dbc.Container(blank_profile, style={"height": "192px"})
        return wrapped


blank_map = RouteMap(id="route-plot").generate()
blank_profile = RouteProfile(id="route-profile").generate()
