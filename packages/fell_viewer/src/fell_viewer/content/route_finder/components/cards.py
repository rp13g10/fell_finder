"""Defines the content of the generated route cards section"""

from datetime import datetime

import dash_leaflet as dl
import dash_bootstrap_components as dbc
from dash import dcc, html
from plotly import graph_objects as go

from fell_viewer.elements.buttons import Button, ButtonConfig
from fell_viewer.common._fv_component import FVComponent
from fell_viewer.common.containers import (
    Route,
    RouteGeometry,
    RouteMetrics,
    BBox,
)
from fell_viewer.elements.buttons import (
    NavButtonConfig,
    NavButton,
)


class RouteCard(FVComponent):
    """Defines the layout for a single route card"""

    def __init__(self, route: Route):
        self.route = route
        self.map_id = f"{route.route_id}{datetime.now()}"

    def generate(self) -> dbc.Card:
        """Generate a card for a single generate route, which includes a
        minimap, high-level metrics, and buttons to view & download"""
        # Generate a minimap showing the shape of the route
        polyline = self.route.to_polyline()
        plot = dl.Map(
            id=self.map_id,
            children=[dl.TileLayer(), polyline],
            center=self.route.geometry.bbox.centre,  # type: ignore
            style={"width": "128px", "height": "128px"},
            bounds=self.route.geometry.bbox.bounds,  # type: ignore
        )

        # Generate a summary of distance/elevation
        card_text = html.Div(
            [
                html.Div(f"Distance: {self.route.metrics.dist:,.0f}"),
                html.Div(f"Elevation: {self.route.metrics.gain:,.0f}"),
            ],
            className="card-text fs-6",
        )

        # TODO: Tidy up card formatting
        # TODO: Set minimaps as fixed, hide zoom buttons and lock the viewport

        # Generate a button to view the route
        view_button_config = ButtonConfig(
            name="View",
            id={
                "type": "route-view-button",
                "route-id": str(self.route.route_id),
            },
            colour="primary",
            size="sm",
        )
        view_button = Button(view_button_config).generate()

        # Generate a utton to download the route
        dl_button_config = ButtonConfig(
            name="Download",
            id={
                "type": "route-dl-button",
                "route-id": str(self.route.route_id),
            },
            colour="primary",
            size="sm",
        )
        dl_button = Button(dl_button_config).generate()

        # Combine all elements into a single card
        card = dbc.Card(
            children=dbc.Row(
                [
                    dbc.Col(
                        html.Div(
                            plot, className="img-fluid rounded-start p-1"
                        ),
                        width="auto",
                        class_name="px-0",
                    ),
                    dbc.Col(
                        [dbc.CardBody(card_text), view_button, dl_button],
                    ),
                ],
            ),
            class_name="mb-1",
        )

        return card


dummy_route = Route(
    route_id=99,
    geometry=RouteGeometry(
        lats=[50.5, 50.75, 60.0],
        lons=[-1.4, -1.4, -1.3],
        dists=[0.0, 0.5, 1.0],
        eles=[1.0, 2.0, 1.0, 2.0],
        bbox=BBox(min_lat=50.5, min_lon=-1.4, max_lat=60.0, max_lon=-1.3),
    ),
    metrics=RouteMetrics(
        dist=1.0, gain=2.0, loss=3.0, s_dists={"unclassified": 1.0}
    ),
)

cards = dbc.Container(
    # style={"overflow": "scroll"},
    children=dbc.Row(
        className="row-cols-1",
        id="route-cards",
        children=None,
    ),
    class_name="d-inline-block",
)
