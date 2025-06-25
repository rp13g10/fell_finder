"""Convenience functions for the creation of common page elements"""

from dataclasses import dataclass
from typing import Literal, override, Any

from dash import dcc
from dash.development.base_component import Component
import dash_bootstrap_components as dbc

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


@dataclass
class NavButtonConfig(ButtonConfig):
    """Configuration for a nav button"""

    link: str = ""
    search: str = ""


class NavButton(Button):
    """Class representing a nav button"""

    def __init__(self, config: NavButtonConfig) -> None:
        self.config = config

    def get_classname(self) -> str:
        """Set the button style based on the provided config"""
        styles = ["btn", "m-1"]

        if self.config.colour:
            styles.append(f"btn-{self.config.colour}")

        if self.config.size:
            styles.append(f"btn-{self.config.size}")

        if self.config.disabled:
            styles.append("disabled")

        style = " ".join(styles)
        return style

    def get_href(self) -> str:
        """Set the button link based on the provided config"""
        if self.config.link and self.config.search:
            href = f"/{self.config.link}?{self.config.search}"
        elif self.config.link:
            href = f"/{self.config.link}"
        elif self.config.search:
            href = f"?{self.config.search}"
        else:
            raise ValueError(
                "At least one of 'link', 'search' must be provided"
            )
        return href

    @override
    def generate(self) -> Component:
        """Generate a button element based on the provided config. Note that
        dcc.Link is used instead of html.A to avoid a full page refresh on
        click"""

        kwargs: dict[str, Any] = dict(
            children=self.config.name,
            href=self.get_href(),
            className=self.get_classname(),
        )
        if self.config.id:
            kwargs["id"] = self.config.id
        button = dcc.Link(**kwargs)

        return button
