"""Contains dataclasses which are used to configure the bootstrap grid system
for page elements/components. These will generally be used as wrappers around
small pieces of page content."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, Literal, Sequence, TypeVar, override

from dash import html
from dash.development.base_component import Component

type ComponentType = Component | Sequence[Component] | str | int | None

ConfigType = TypeVar("ConfigType", bound="ConfigItem")


@dataclass
class ConfigItem(ABC):
    """Abstract base class for a dataclass which holds the information required
    to configure an element of the page."""

    classnames: list[str] | None


class PageItem(ABC):
    """Abstract base class for any item which is to be placed into the webapp,
    defines the methods which are expected to be implemented."""

    @abstractmethod
    def generate(self) -> ComponentType:
        """Every PageItem must implement a generate method, which will return
        a Component or a sequence of Components which can be placed into the
        webapp"""


class GridItem(PageItem, Generic[ConfigType]):
    """Abstract base class for any item which is to be placed into the
    bootstrap grid system. Every implementation must provide a `generate`
    method, and a `get_classname` method. Each instance must be provided
    children (either a Component, GridItem, or a sequence of the two). As the
    classnames for this type of component are likely to be more complex than
    more simple elements, a configuration object must also be provided,
    which subclasses ConfigItem."""

    def __init__(
        self,
        children: ComponentType | "GridItem" | Sequence["GridItem"],
        config: ConfigType,
    ) -> None:
        self._children = children
        self.children: ComponentType
        self.config = config

    def __post_init__(self) -> None:
        """Once a GridItem has been instantiated, if any of its child elements
        are also GridItems, call their associated `generate` methods."""
        # TODO: Test this fully
        if hasattr(self._children, "generate"):
            self.children = self._children.generate()  # type: ignore
            return
        elif (ctype := type(self._children)) in {set, list, tuple}:
            new_children = []
            for child in self._children:  # type: ignore
                if hasattr(child, "generate"):
                    new_children.append(child.generate())
                else:
                    new_children.append(child)
            self.children = ctype(new_children)  # type: ignore
        else:
            self.children = self._children  # type: ignore

    @abstractmethod
    def get_classname(self) -> str:
        """Every implementation must provide a method which returns a single
        string, which can be set as the classname for a page element"""


@dataclass
class RowConfig(ConfigItem):
    """Configuration class for a row in the bootstrap grid system"""


class Row(GridItem):
    """Class representing a row in the boostrap grid system"""

    def get_classname(self) -> str:
        """Generates the classname for a row element"""
        return "row"

    def generate(self) -> html.Div:
        """Wraps the provided child element(s) in a row"""
        return html.Div(
            children=self.children,
            className=self.get_classname(),
        )


@dataclass
class ColumnConfig(ConfigItem):
    """Configuration options for a column in the bootstrap grid system"""

    breakpoint: Literal["sm", "md", "lg", "xl"] | None
    width: int

    def __post_init__(self) -> None:
        """Validate user provided inputs"""
        assert 0 < self.width <= 12, "width must be between 1 and 12!"
        assert self.breakpoint in {"sm", "md", "lg", "xl"}, (
            "breakpoint must be in sm, md, lg, xl!"
        )


class Column(GridItem):
    """Class representing a column in the bootstrap grid system"""

    def get_classname(self) -> str:
        """Set the classname for a column elemnt based on its config"""
        if self.config.breakpoint is None:
            return f"col-{self.config.width}"
        return f"col-{self.config.breakpoint}-{self.config.width}"

    def generate(self) -> html.Div:
        """Wraps the provided child element(s) in a column"""
        return html.Div(
            children=self.children,
            className=self.get_classname(),
        )


@dataclass
class ContainerConfig(ConfigItem):
    """Configuration options for a container"""

    fluid: bool

    def __post_init__(self) -> None:
        """Validate user provided inputs"""

        assert self.fluid in {True, False}, "fluid must be True or False!"


class Container(GridItem):
    """Class representing a container in the boostrap grid system"""

    def get_classname(self) -> str:
        """Sets the classname for the container"""
        match self.config.fluid:
            case True:
                return "container-fluid"
            case False:
                return "container"
        raise ValueError("Something went wrong setting up the container!")

    def generate(self) -> html.Div:
        """Generates a container element"""
        return html.Div(className=self.get_classname(), children=self.children)


@dataclass
class NavbarConfig(ConfigItem):
    """Configuration options for a navbar"""

    footer: bool


class Navbar(GridItem[NavbarConfig]):
    """Class representing a navbar in the bootstrap grid system"""

    default_classnames = ["navbar-light", "bg-light"]

    @override
    def get_classname(self) -> str:
        if self.config.classnames is None:
            names_list = self.default_classnames
        else:
            names_list = self.default_classnames + self.config.classnames

        match self.config.footer:
            case True:
                names_list += ["navbar-fixed-bottom", "fixed-bottom"]
            case False:
                names_list += ["navbar"]

        return " ".join(names_list)

    def generate(self) -> html.Div:
        """Generates a navbar element"""
        return html.Div(className=self.get_classname(), children=self.children)
