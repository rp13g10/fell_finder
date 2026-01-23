"""Exports the BaseLoader class, which all other classes in this module
inherit from.
"""

from fell_loader.base.base_loader import BaseLoader, BaseSparkLoader

__all__ = ["BaseLoader", "BaseSparkLoader"]
