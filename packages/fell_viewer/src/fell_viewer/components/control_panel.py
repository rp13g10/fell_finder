"""Defines utilities for the creation of complex control panels which can be
used to collect multiple user inputs"""

from typing import Literal, Sequence

import dash_bootstrap_components as dbc
from dash import html
from dash.development.base_component import Component

from fell_viewer.common._fv_component import FVComponent

type SectionControlType = Control
type ControlType = FVComponent | Control | "PanelSection"


class Control(FVComponent):
    """Defines a single control element, which is comprised of a title and
    another component, typically an interactive item from dash core
    components"""

    def __init__(
        self,
        title: str | None,
        control: Component,
        control_width: int | Literal["auto"] = "auto",
        title_id: str | None = None,
    ) -> None:
        self.title = title
        self.control = control
        self.control_width = control_width
        self.title_id = title_id

    def _get_widths(self) -> tuple[int, int] | tuple[str, None]:
        if self.control_width == "auto":
            return ("auto", None)
        else:
            assert type(self.control_width) is int, (
                f"control_width must be int, got {type(self.control_width)}"
            )
            assert 1 <= self.control_width <= 12, (
                "control_width must be between 0 and 12, "
                f"got {self.control_width}"
            )
            if self.control_width == 12:
                return (12, 12)
            return (12 - self.control_width, self.control_width)

    def generate(self) -> dbc.Row:
        """Render this Control so that it can be displayed on the page"""
        title_width, control_width = self._get_widths()
        if self.title:
            title_kwargs = {}
            if self.title_id:
                title_kwargs["id"] = self.title_id
            title_div = dbc.Label(self.title, **title_kwargs)
            wrapped = dbc.Row(
                children=[
                    dbc.Col(title_div, width=title_width),
                    dbc.Col(self.control, width=control_width),
                ],
                class_name="mb-2",
            )
        else:
            wrapped = dbc.Row(children=self.control)

        return wrapped


class PanelSection(FVComponent):
    """Defines a control panel section, which wraps multiple control elements
    and a title into a subsection of a panel. This should always be placed in
    a Panel object, and is not designed for use on its own."""

    def __init__(
        self, title: str, controls: Sequence[SectionControlType]
    ) -> None:
        self.title = title
        self.controls = controls

    def generate(self) -> Component:
        """Render this PanelSection so that it can be displayed on the page"""

        title_div = html.H6(
            className="sidebar-heading h5", children=self.title
        )

        controls = [control.generate() for control in self.controls]

        section = html.Div([title_div, *controls])

        return section


class Panel(FVComponent):
    """Defines a control panel component, which wraps multiple control elements
    and a title into a single panel."""

    def __init__(self, title: str, controls: Sequence[ControlType]) -> None:
        self.title = title
        self.controls = controls

    def generate(self) -> dbc.Container:
        """Render this Panel so that it can be displayed on the page"""
        title_div = html.Div(
            className="sidebar-heading h4", children=self.title
        )

        controls_div = dbc.Row(
            children=[control.generate() for control in self.controls],
        )

        panel = dbc.Container(children=[title_div, controls_div])

        return panel
