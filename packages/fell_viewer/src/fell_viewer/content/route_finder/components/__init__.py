"""Defines all of the components required for the route finder page"""

from typing import Any

import dash_bootstrap_components as dbc

from fell_viewer.content.route_finder.components.cards import cards
from fell_viewer.content.route_finder.components.controls import control_panel
from fell_viewer.content.route_finder.components.plots import (
    blank_map,
    blank_profile,
)

__all__ = ["blank_map", "blank_profile", "sidebar_contents"]

tab_style: dict[str, Any] = dict(
    style={
        # TODO: Height was determined empirically, see if there's a more
        #       robust way to do this
        "height": "calc(100vh - 114px)",
    },
    class_name="overflow-scroll",
)

sidebar_contents = dbc.Tabs(
    [
        dbc.Tab(
            control_panel,
            label="Create",
            tab_id="route-tab-control",
            **tab_style,
        ),
        # TODO: Set this to disabled, enable only once routes generated
        dbc.Tab(cards, label="View", tab_id="route-tab-cards", **tab_style),
    ],
    id="route-tabs",
)
