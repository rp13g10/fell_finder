"""Convenience functions for the creation of common page elements"""

from dataclasses import dataclass
from typing import Literal, Union

from dash import dcc, html


@dataclass
class NavButtonConfig:
    """Configuration for a nav button"""

    name: str
    colour: Literal["green", "blue", "teal", "yellow", "red"]
    compact: bool = False
    disabled: bool = False

    link: str = ""
    search: str = ""


def _get_nav_button_style(config: NavButtonConfig) -> str:
    styles = ["btn", "m-1"]

    match config.colour:
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

    if config.compact:
        styles.append("btn-sm")

    if config.disabled:
        styles.append("disabled")

    style = " ".join(styles)
    return style


def generate_nav_button(config: NavButtonConfig) -> Union[html.A, dcc.Link]:
    """Generate an HTML element for a nav button based on the provided
    config

    Args:
        config: The configuration for the button to be generated

    """

    if config.link and config.search:
        href = f"/{config.link}?{config.search}"
    elif config.link:
        href = f"/{config.link}"
    elif config.search:
        href = f"?{config.search}"
    else:
        raise ValueError("At least one of 'link', 'search' must be provided")

    # Use dcc.Link instead of html.A to avoid full page refresh on click
    button = dcc.Link(
        config.name, href=href, className=_get_nav_button_style(config)
    )

    return button
