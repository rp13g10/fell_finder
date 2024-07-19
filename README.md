# Fell Finder

This project is still in early development, with significant work still required before it's ready for general use. That said, with a little leg-work a viable PoC is now up and running.

## Instructions for use
* After cloning, you'll need to select a folder which will hold all of the source data. In `app.py` and `ingestion.py`, set this directory as your `DATA_DIR`.
  * This will eventually be moved into a proper config file
* Your data folder will need the following subdirectories:
  * extracts
    * osm
    * lidar
  * parsed
  * enriched
  * optimised
  * temp
* In extracts/osm, you'll need to place a .osm.pbf file covering the area you want
* In extracts/lidar, you'll need to place the corresponding (extracted) LIDAR data
  * This can be downloaded [here](https://environment.data.gov.uk/survey)
  * After selecting the area to download, select 'LIDAR Composite DTM / 2022 / 1m'
* Pip install this package and all of its requirements
* Run `ingestion.py` to process your extracts.
  * In my own testing, it takes ~1 hour to completely process the data for all of Hampshire
* Once ingestion has completed, you can start the webapp in development mode
  * First, `cd` into the app folder. Start Celery with: `celery -A app.celery_app worker --loglevel=INFO`
  * Then, without closing the celery process, start the webapp with `python app.py`
  * You can then access the webapp by opening 'http://localhost:8050/' in your browser

## Future developments
* The webapp itself is currently very basic, and needs a lot of tidying up
* Some of the code-base is likely to be restructured as additional components are added
* Additional pages are planned
  * Manual route plotting
  * Finding the hilliest/flattest area within a radius of a selected point
* Additional route finding modes are planned
  * Find the route with the greatest proportion of trails
* Further performance improvements to the route finding process should be possible
  * In a future build, the more intensive processes may be rewritten in a more performant language
  * Very early prototypes took ~6 hours to generate a route, so I'm quite pleased with the current 1-2 minutes!
* The entire app needs to be containerised and set up to scale
* An airflow pipeline is planned, picking up updated LIDAR/OSM data once it appears
* Everything will ultimately be deployed into the cloud