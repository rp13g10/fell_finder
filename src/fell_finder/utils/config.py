"""Utility functions which facilitate the configuration of other elements of
the fell_finder package"""

import os
from typing import Any, Dict
from yaml import load, Loader

# TODO: Switch to LibYAML for improved performance

cur_dir = os.path.dirname(os.path.abspath(__file__))


def get_config() -> Dict[str, Any]:
    """Read in the contents of config.yml from the package root and return it
    as a python dictionary.

    Returns:
        The parsed contents of config.yml
    """
    root_dir = os.path.join(cur_dir, "..")
    config_path = os.path.join(root_dir, "config.yml")
    with open(config_path, "r") as fobj:
        config = load(fobj, Loader)
    return config
