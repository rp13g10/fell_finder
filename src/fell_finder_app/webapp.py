"""Simple script which runs the webapp for this project in development mode"""

import os

from fell_viewer.app import app, celery_app

DEBUG = os.environ["FF_DEBUG_MODE"] == "true"

__all__ = ["app", "celery_app"]

if __name__ == "__main__":
    app.run(debug=DEBUG)
