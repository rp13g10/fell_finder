"""Simple script which runs the webapp for this project in development mode"""

from fell_finder.app.index import app, celery_app

__all__ = ["app", "celery_app"]

if __name__ == "__main__":
    app.run(debug=True)
