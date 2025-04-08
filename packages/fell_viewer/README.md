# Fell Viewer

This package defines the layout of the fell_viewer webapp, allowing users to generate routes according to their preferences. It is in an MVP state at the moment, providing a functional interface and GPX exports, but with a number of rough edges which still need to be tidied up.

## USAGE

* Start the [fell_finder](packages/fell_finder/README.md) API
* For a 'standard' launch
  * Make sure the `FF_DEBUG_MODE` environment variable is set to 'false'
    * First, start Celery with: `uv run celery -A webapp.celery_app worker --loglevel=INFO`
    * Then, without closing the celery process, start the webapp with `uv run python webapp.py`
    * You can then access the webapp by opening 'http://localhost:8050/' in your browser
* For a 'debug' launch
    * Make sure the `FF_DEBUG_MODE` environment variable is set to 'true'
    * Start the webapp with `uv run python webapp.py`