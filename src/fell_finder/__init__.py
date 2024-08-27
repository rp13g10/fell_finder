"""Expose project config as fell_finder.app_config"""

from fell_finder.utils.config import get_config

app_config = get_config()

__all__ = ["app_config"]
