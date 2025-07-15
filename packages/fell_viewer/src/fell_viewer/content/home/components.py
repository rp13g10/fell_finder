"""Defines the individual components which make up the route finder page"""

from typing import Any

import dash_bootstrap_components as dbc
from dash import html
from dash.development.base_component import Component

from fell_viewer.common._fv_component import FVComponent
from fell_viewer.utils.encoding import get_image_as_str


class HomeText(FVComponent):
    """Sets the layout for the introductory text displayed on the homepage"""

    INTRO = (
        "Welcome to Fell Finder, a personal project which aims to help you "
        "find the hilliest possible routes in your local area. Route "
        "generation is limited to the UK, as I have been unable to find a "
        "free source of elevation data which covers the entire planet!"
    )

    USAGE = (
        "The Route Finder page of this site allows you to generate circular "
        "routes with a number of options. The maximum supported distance is "
        "currently 50k, and only routes within 10% of the selected distance "
        "will be returned. You can set filters on allowable surface types "
        "and road/path types to exclude them entirely. Alternatively, you can "
        "set a max % of the total distance which you're willing to spend on "
        "certain surfaces. Please note that for longer distances, route "
        "generation may take a few minutes to complete."
    )

    LIMITATIONS = (
        "Simple route generation is generally quite reliable, but setting "
        "very restrictive filters may result in a failure to generate any "
        "routes at all. Similarly, if you try to generate a trail run which "
        "starts in the middle of a city, it's unlikely you'll get anything "
        "back."
    )

    ACKNOWLEDGEMENTS = (
        "This app is made possible by a number of open source datasets, "
        "applications and other resources. As well as all of the programming "
        "languages & packages used, my thanks to the maintainers of "
        "the following:"
    )

    SOURCES = [
        ("Open Street Map", "https://www.openstreetmap.org/"),
        (
            "LIDAR DTM 1m",
            "https://environment.data.gov.uk/dataset/13787b9a-26a4-4775-8523-806d13af58fc",
        ),
        ("Geofabrik", "https://www.geofabrik.de/"),
        ("OSM Parquetizer", "https://github.com/adrianulbona/osm-parquetizer"),
        ("Mappity", "https://www.mappity.org/"),
    ]

    def generate(self) -> Component:
        """Generates the text which will be shown on the project homepage"""

        text_kwargs: dict[str, Any] = dict(className="mb-3")

        text_rows = [
            dbc.Row(html.H1("Introduction")),
            dbc.Row(html.Div(self.INTRO, **text_kwargs)),
            dbc.Row(html.H1("Usage")),
            dbc.Row(html.Div(self.USAGE, **text_kwargs)),
            dbc.Row(html.H1("Limitations")),
            dbc.Row(html.Div(self.LIMITATIONS, **text_kwargs)),
            dbc.Row(html.H1("Acknowledgements")),
            dbc.Row(html.Div(self.ACKNOWLEDGEMENTS, className="mb-2")),
            html.Ul(
                [
                    html.Li(html.A(name, href=href))
                    for name, href in self.SOURCES
                ]
            ),
        ]

        return text_rows  # type: ignore


home_text = HomeText()


class HomeImage(FVComponent):
    """Sets the layout for the inspirational image displayed on the homepage"""

    def generate(self) -> html.Img:
        """Place Laz on the homepage"""
        return html.Img(
            src=get_image_as_str("barkley_start.jpg"),
            className="img-fluid rounded",
        )


home_image = HomeImage()
