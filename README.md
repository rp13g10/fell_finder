# Fell Finder

A personal project which aims to help you find the hilliest possible routes in your local area (so long as you're UK based). Development is typically limited to ~4 hours a week while I'm commuting in to the office, so new features may take a while to implement. You are welcome to use any of the code provided in this repo, so long as credit is given to me as the original author.

![Webapp Preview](./assets/webapp_preview.png)

## Objectives

The aims of this project are twofold. Primarily, it is intended that the final product will be a webapp which can be used to generate superlative routes (the hilli**est**, the flatt**est**), as opposed to simply a 'hilly' route. Future builds are likely to include other features which make it easier for users to identify large hills to run up, and may go as far as to offer integrations with other popular services (Strava, Komoot) depending on which APIs are available.
The secondary objective is to provide myself with a challenge, and an opportunity to try out some different technologies. Once the current phase is complete (getting a fully tested PoC running locally, with an architecture I'm satisfied with), the next step will be setting everything up to scale and deploying it to the cloud.

## Current State

This project is still in early development, with significant work still required before it's ready for general use. That said, with a little leg-work a viable PoC is now up and running. At present, the ingestion pipeline is largely up and running (although it's executed on-demand at the moment). A basic webapp is provided, which is able to plot routes on demand and display them to the user. The route finding page of the webapp is not feature complete; there are no GPX exports, and the UI is in need of refinement. Routes are generally created in less than a minute for shorter distances (up to 10k), although the algorithm has been tested up to marathon distance.

There are currently gaps in unit test coverage. However, as significant portions of the route finding algorithm are going to be rewritten in Rust, these are not being prioritised. My current focus is on learning Rust, and translating the existing algorithm across (with full test coverage for the rewritten code).



## Instructions for use
* Clone this repo to your local device
* Use Poetry to install the fell_finder package
  * Set up [poetry](https://python-poetry.org/docs/#installation) if you haven't already
  * Use `poetry install` to install fell_finder and its dependencies
    * You'll need to have `python3-devel` installed on your device (`python3.12-devel` if your default version isn't 3.12)
    * If you run into build errors with numpy, you might need to install the C build tools. Use `dnf group install c-development development-tools` to install them on Fedora
    * If you run into build errors with pyarrow, you can find the dependencies to install [here](https://arrow.apache.org/docs/developers/cpp/building.html)
* Install additional dependencies
  * Install redis `sudo dnf install redis` on Fedora and set it to run
    * `sudo systemctl start redis` to run it once, or `sudo systemctl enable redis` to run it every time your computer boots
* Configure the app for your system
  * As a short-term implementation, the config file is stored in `src/fell_finder/config.yml`
  * Set the data_dir to the (absolute) location of the 'data' subfolder on your device
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
* Run `ingestion.py` to process your extracts.
  * In my own testing, it takes ~1 hour to completely process the data for all of Hampshire
* Once ingestion has completed, you can start the webapp in two modes
  * For a 'standard' launch
    * Make sure 'debug' is set to False in `src/fell_finder/config.yml`
    * First, start Celery with: `poetry run celery -A webapp.celery_app worker --loglevel=INFO`
    * Then, without closing the celery process, start the webapp with `python webapp.py`
    * You can then access the webapp by opening 'http://localhost:8050/' in your browser
  * For a 'debug' launch
    * Make sure 'debug' is set to True in `src/fell_finder/config.yml`
    * Start the webapp with `poetry run python webapp.py`

## Roadmap

These new features are listed in approximate order of priority

### Backend

* Maximise performance of the route-finding algorithm
  * It is expected that this will involve a rewrite using Rust
  * Rustworkx is already in use, so an implementation of a graph library will not be required
* Ensure full test coverage on any code which is not app-specific
* Containerise everything
* Set up an airflow pipeline for ingestion
* Deploy to the cloud
* Identify ways to further improve the accuracy of calculated elevation gain/loss
  * Other sources of elevation data to be evaluated, candidates are OS Terrain and SRTM
* Improve the ingestion layer
  * Exclude ways/nodes which have been blocked, requires investigation into different tags which may be used on OSM
  * Smooth out elevation profiles for tunnels/bridges

### Frontend

* Build out the route finding page of the webapp
  * Page layout could stand to be improved, collapsible sidebars may help
* Formally define the expected end state of the webapp
  * Set out all of the different features to be built out
  * Define a target layout for each feature
  * This may include the following
    * Manual route plotting
    * Hill finder (find the largest/steepest climbs in the local area)
    * API integrations
      * This may then enable dashboards looking at past activity data
* Add user logins, allowing people to save & retrieve routes
* Create an admin dashboard to aid development & performance diagnostics


## Issues / Limitations

* Estimated gain/loss is typically ~30% higher than it should be, this needs to be investigated further
  * Part of the issue seems to be with the LIDAR data itself being a few metres out at times (vs. Strava)
  * Other issues include bridges/tunnels where the path stays level but the ground does not. Some mitigations are in place for this, but making better use of the OSM tags may be able to improve the situation further.
* Additional filters need to be added
  * access = No flag needs to be respected on load, blocked off paths are not currently respected
  * Further investigation is needed around surface types, some local footpaths seem to be showing as paved. This may be an issue with the underlying data.