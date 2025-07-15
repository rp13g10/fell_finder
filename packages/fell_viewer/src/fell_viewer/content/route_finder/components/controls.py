"""Defines the control panel for the route finder page"""

import json
import os

from dash import dcc

from fell_viewer.components.control_panel import (
    Control,
    Panel,
    PanelSection,
)
from fell_viewer.elements.buttons import Button, ButtonConfig

CUR_DIR = os.path.dirname(os.path.abspath(__file__))
with open(
    os.path.join(CUR_DIR, "..", "highway_types.json"), "r", encoding="utf8"
) as fobj:
    HIGHWAY_TYPES = list(json.load(fobj).keys())
with open(
    os.path.join(CUR_DIR, "..", "surface_types.json"), "r", encoding="utf8"
) as fobj:
    SURFACE_TYPES = list(json.load(fobj).keys())


control_panel = Panel(
    title="Route Configuration",
    controls=[
        Control(
            title="Distance",
            control=dcc.Slider(
                value=1,
                min=1,
                max=50,
                step=1,
                marks=None,
                id="route-dist",
                className="w-100",
                tooltip={
                    "always_visible": True,
                    "placement": "bottom",
                    "template": "{value} km",
                },
            ),
        ),
        PanelSection(
            title="Route Preferences",
            controls=[
                Control(
                    title="Profile",
                    control=dcc.Dropdown(
                        options=[
                            {"label": "Hilly", "value": "hilly"},
                            {"label": "Flat", "value": "flat"},
                        ],
                        value="hilly",
                        id="route-mode",
                    ),
                ),
                Control(
                    title="Way Types",
                    control=dcc.Dropdown(
                        options=HIGHWAY_TYPES,
                        value=[x for x in HIGHWAY_TYPES if "*" not in x],
                        multi=True,
                        id="route-highway",
                    ),
                ),
                Control(
                    title="Surfaces",
                    control=dcc.Dropdown(
                        options=SURFACE_TYPES,
                        value=[x for x in SURFACE_TYPES if "*" not in x],
                        multi=True,
                        id="route-allowed-surfaces",
                    ),
                ),
            ],
        ),
        PanelSection(
            title="Restrictions",
            controls=[
                Control(
                    title="Restricted Surfaces",
                    control=dcc.Dropdown(
                        options=SURFACE_TYPES,
                        value=[],
                        multi=True,
                        id="route-restricted-surfaces",
                    ),
                    control_width=12,
                ),
                Control(
                    title=r"Max %",
                    control=dcc.Slider(
                        value=0,
                        min=0,
                        max=100,
                        step=1,
                        id="route-restricted-perc",
                        marks=None,
                        className="w-100",
                        tooltip={
                            "always_visible": True,
                            "placement": "bottom",
                            "template": "{value} %",
                        },
                    ),
                ),
            ],
        ),
        # TODO: Set up controls to automatically call .generate as required
        Control(
            title=None,
            control=Button(
                config=ButtonConfig(
                    name="Calculate",
                    colour="primary",
                    id="route-calculate-button",
                )
            ).generate(),
        ),
    ],
).generate()
