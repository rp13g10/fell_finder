"""Defines utilities for the creation of complex control panels which can be
used to collect multiple user inputs"""

from fell_viewer.common.grid import PageItem
from dash.development.base_component import Component
from dash import html

# TODO: Set this up with implementation for PanelSection, which can also be
#       provided to Panel. This should wrap a number of controls


type ControlType = Control | SectionHeader | SectionSubHeader


class Control(PageItem):
    """Defines a single control element, which is comprised of a title and
    another component, typically an interactive item from dash core
    components"""

    def __init__(self, title: str | None, control: Component) -> None:
        self.title = title
        self.control = control

    def generate(self) -> html.Div:
        if self.title:
            title_div = html.Div(className="h6", children=self.title)

            wrapped = html.Div(
                className="list-group-item", children=[title_div, self.control]
            )
        else:
            wrapped = html.Div(
                className="list-group-item", children=self.control
            )

        return wrapped


class SectionHeader(PageItem):
    """Defines a section header, which can be placed into a control panel to
    help group connected items"""

    def __init__(self, title: str) -> None:
        self.title = title

    def generate(self) -> html.Div:
        return html.Div(children=self.title, className="h5")


class SectionSubHeader(PageItem):
    """Defines a section sub-header, which can be placed into a control panel
    to help group connected items"""

    def __init__(self, title: str) -> None:
        self.title = title

    def generate(self) -> html.Div:
        return html.Div(children=self.title, className="h6")


class Panel(PageItem):
    """Defines a control panel component, which wraps multiple control elements
    and a title into a single panel."""

    def __init__(self, title: str, controls: list[ControlType]) -> None:
        self.title = title
        self.controls = controls

    def generate(self) -> list[html.Div]:
        title_div = html.Div(
            className="sidebar-heading h4", children=self.title
        )

        controls_div = html.Div(
            className="list-group list-group-flush",
            children=[control.generate() for control in self.controls],
        )

        return [title_div, *controls_div]
