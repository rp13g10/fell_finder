"""Defines the backend application which powers the fell_finder webapp,
this has been moved out from the root level app.py as some callbacks
reference the background callback manager. Attempting to create the app
and set the layout in the same file results in a circular import.
"""

import dash_bootstrap_components as dbc
import diskcache
from celery import Celery
from dash import CeleryManager, Dash, DiskcacheManager

from fell_viewer.utils import get_env_var

__all__ = ["app", "background_callback_manager"]

# TODO: Find a way to avoid creating app/callback manager on import
#       move into functions instead

if get_env_var("FF_DEBUG_MODE", default="true") == "true":
    celery_app = None
    cache = diskcache.Cache()
    background_callback_manager = DiskcacheManager(cache)

else:
    redis_host = get_env_var("FF_REDIS_HOST")
    redis_port = get_env_var("FF_REDIS_PORT")

    celery_app = Celery(
        __name__,
        broker=f"redis://{redis_host}:{redis_port}/0",
        backend=f"redis://{redis_host}:{redis_port}/1",
    )
    background_callback_manager = CeleryManager(celery_app)

app = Dash(
    __name__,
    suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
)

server = app.server
