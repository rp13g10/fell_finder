"""Defines the backend application which powers the fell_finder webapp,
this has been moved out from the root level app.py as some callbacks
reference the background callback manager. Attempting to create the app
and set the layout in the same file results in a circular import."""

import os

import dash_bootstrap_components as dbc
import diskcache
from celery import Celery
from dash import CeleryManager, Dash, DiskcacheManager

__all__ = ["background_callback_manager", "app"]

if os.environ["FF_DEBUG_MODE"] == "true":
    celery_app = None
    cache = diskcache.Cache(
        os.path.join(os.environ["FF_DATA_DIR"], "temp/.cache")
    )
    background_callback_manager = DiskcacheManager(cache)
else:
    celery_app = Celery(
        __name__,
        broker="redis://localhost:6379/0",
        backend="redis://localhost:6379/1",
    )
    background_callback_manager = CeleryManager(celery_app)

app = Dash(
    __name__,
    suppress_callback_exceptions=True,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
)
