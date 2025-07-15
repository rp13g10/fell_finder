"""Defines the content of the generated route cards section"""

from datetime import datetime

import dash_bootstrap_components as dbc
import dash_leaflet as dl
from dash import html

from fell_viewer.common._fv_component import FVComponent
from fell_viewer.common.containers import (
    Route,
)
from fell_viewer.elements.buttons import Button, ButtonConfig


class RouteCard(FVComponent):
    """Defines the layout for a single route card"""

    def __init__(self, route: Route) -> None:
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
            zoomControl=False,
            dragging=False,
            boxZoom=False,
            doubleClickZoom=False,  # type: ignore
            scrollWheelZoom=False,  # type: ignore
            attributionControl=False,
        )

        # Generate a summary of distance/elevation
        card_text = html.Div(
            [
                html.Div(
                    f"Distance: {self.route.metrics.dist / 1000:,.1f} km"
                ),
                html.Div(f"Elevation: {self.route.metrics.gain:,.0f} m"),
            ],
            className="card-text fs-6",
        )

        # TODO: Jazz up card formatting, any more info to add?

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

        # TODO: Support other file formats (tcx, etc)

        # Generate a button to download the route
        dl_button_config = ButtonConfig(
            name="Download",
            id={
                "type": "route-dl-button",
                "route-id": str(self.route.route_id),
            },
            colour="secondary",
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
                        [
                            dbc.Row(dbc.CardBody(card_text)),
                            dbc.Row(dbc.ButtonGroup([view_button, dl_button])),
                        ],
                    ),
                ],
            ),
            class_name="mb-1",
        )

        return card


cards = dbc.Container(
    children=dbc.Row(
        className="row-cols-1",
        id="route-cards",
        children=None,
    ),
    class_name="d-inline-block",
)
