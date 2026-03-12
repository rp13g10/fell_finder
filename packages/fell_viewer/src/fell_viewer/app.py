"""Defines the backend application which powers the fell_finder webapp"""

from fell_viewer.common.app import app, celery_app, server
from fell_viewer.layout import layout

__all__ = ["app", "celery_app", "server"]

app.layout = layout
