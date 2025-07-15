"""Convenience functions for the creation of common page elements"""

from dataclasses import dataclass
from typing import Literal

import dash_bootstrap_components as dbc
from dash.development.base_component import Component

from fell_viewer.common._fv_component import FVComponent


@dataclass
class ButtonConfig:
    """Configuration for a standard button"""

    name: str
    colour: Literal[
        "primary", "secondary", "success", "info", "warning", "danger"
    ]
    size: Literal["sm", "md", "lg"] = "md"
    disabled: bool = False
    id: str | dict | None = None


class Button(FVComponent):
    """Class representing a button"""

    def __init__(self, config: ButtonConfig) -> None:
        self.config = config

    def generate(self) -> Component:
        """Generate a button element based on the provided config. Note that
        dcc.Link is used instead of html.A to avoid a full page refresh on
        click"""
        button = dbc.Button(
            type="button",
            size=self.config.size,
            disabled=self.config.disabled,
            id=self.config.id,
            color=self.config.colour,
            children=self.config.name,
        )

        return button
