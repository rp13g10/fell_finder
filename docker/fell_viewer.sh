#!/bin/bash

# Start a Celery worker
celery -A fell_viewer.app.celery_app worker --loglevel=INFO &

# Serve the webapp
gunicorn fell_viewer.app:server --bind 0.0.0.0:8050 --workers 4 &

# Wait for any process to exit
wait -n

# Exit with status of process that exited first
exit $?