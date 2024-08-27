"""Defines the backend application which powers the fell_finder webapp"""

import diskcache
import os
from celery import Celery
from dash import Dash, CeleryManager, DiskcacheManager

from fell_finder import app_config

if app_config["debug"]:
    celery_app = None
    cache = diskcache.Cache(
        os.path.join(app_config["data_dir"], "temp/.cache")
    )
    background_callback_manager = DiskcacheManager(cache)
else:
    celery_app = Celery(
        __name__,
        broker="redis://localhost:6379/0",
        backend="redis://localhost:6379/1",
    )
    background_callback_manager = CeleryManager(celery_app)

app = Dash(__name__, suppress_callback_exceptions=True)
