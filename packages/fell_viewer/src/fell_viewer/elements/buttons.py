"""Convenience functions for the creation of common page elements"""

from dataclasses import dataclass
from typing import Literal, override

from dash import dcc, html

from dash.development.base_component import Component

from fell_viewer.common.grid import PageItem


@dataclass
class ButtonConfig:
    """Configuration for a standard button"""

    name: str
    colour: Literal["green", "blue", "teal", "yellow", "red"]
    compact: bool = False
    disabled: bool = False
    id: str | None = None


class Button(PageItem):
    """Class representing a button"""

    def __init__(self, config: ButtonConfig) -> None:
        self.config = config

    def get_classname(self) -> str:
        """Set the button style based on the provided config"""
        styles = ["btn", "m-1"]

        match self.config.colour:
            case "green":
                styles.append("btn-success")
            case "blue":
                styles.append("btn-primary")
            case "teal":
                styles.append("btn-info")
            case "yellow":
                styles.append("btn-warning")
            case "red":
                styles.append("btn-danger")

        if self.config.compact:
            styles.append("btn-sm")

        if self.config.disabled:
            styles.append("disabled")

        style = " ".join(styles)
        return style

    def generate(self) -> Component:
        """Generate a button element based on the provided config. Note that
        dcc.Link is used instead of html.A to avoid a full page refresh on
        click"""
        button = html.Div(
            self.config.name,
            className=self.get_classname(),
            id=self.config.id,
            role="button",
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
        button = dcc.Link(
            self.config.name,
            href=self.get_href(),
            className=self.get_classname(),
            id=self.config.id,
        )

        return button
