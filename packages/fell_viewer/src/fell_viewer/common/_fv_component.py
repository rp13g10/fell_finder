"""Defines the behaviours which must be implemented by all custom components"""

from abc import ABC, abstractmethod

from dash.development.base_component import Component


class FVComponent(ABC):
    """Defines the behaviours which must be implemented by all custom
    components"""

    @abstractmethod
    def generate(self) -> Component:
        """Every component must include a generate method, which returns an
        object which can be rendered directly on the page by Dash"""
        ...
