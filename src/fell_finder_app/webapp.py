"""Simple script which runs the webapp for this project in development mode"""

from fell_viewer.app import app, celery_app
from fell_viewer.utils import get_env_var

DEBUG = get_env_var("FF_DEBUG_MODE", default="true") == "true"

__all__ = ["app", "celery_app"]

if __name__ == "__main__":
    app.run(debug=DEBUG, host="0.0.0.0", port=8050)
