# Changelog

## v0.12.1 - 2026-04-09

### New Features

* Project documentation overhauled and moved over to Sphinx

## v0.12.0 - 2026-03-12

### New Features

* Both data ingestion & webapp have been set up to run in Docker containers
* Data loads for the whole of the UK are now feasible

### Fixes

* Unit tests can now be executed without a .env file present
* Resolves some issues with the handling of 'reverse' edges
  * Elevation changes are now correctly reported
  * Rendering issues in the webapp have been fixed
* Fixes for bad data observed while loading whole-UK data
* Partition range for Postgres tables now covers the whole UK, previous range was incorrect

## v0.11.0 - 2026-01-23

### New Features

* Significant refactor of data ingestion process
* Data ingestion has been set up with support for incremental updates
* Data ingestion can now safely be aborted and resumed
* Switched to using pathlib for filesystem operations


## v0.10.0 - 2025-10-28

### New Features

* Implemented toggle for dijkstra validation during route generation
* Implemented toggle for alternate route binning strategy


## v0.9.1 - 2025-10-17

### Fixes

* Tidied up progress bar display
* Improved serialization for error messages returned by API

## v0.9.0 - 2025-09-23

### New Features

* Improved error handling in Rust code to limit risk of panics
* Backend improvements to route creation API
* Implemented progress bar during route creation
* Enabled toast popups to inform users of issues when route creation fails

## v0.8.0 - 2025-08-19

### New Features

* API response now includes metrics collected during route generation

### Fixes

* Improve performance of route pruning algorithm
* Enable threaded execution of route pruning
* Enable dynamic update of max candidates

## v0.7.0 - 2025-07-15

### New Features

* Reworked the route finding page, providing an improved UI and making better use of screen space
* Populated the home page
* Switched to single navbar layout, with dropdown menu for page navigation
* Switched to dash-bootstrap-components to define page layout
* Enable GPX download of routes directly from cards, this is no longer tied to the currently selected route
* Enabled pre-commit hooks for ruff and vulture
* Improved handling of roads with separate footpaths, reducing the likelihood of being told to run uphill along a road, and back down along the footpath.
* Tidied up the structure of the fell_viewer package, although further refactoring remains a possibility
* Improved appearance of generated routes both on cards, and main map
* Improved appearance of generated elevation profiles

### Fixes

* FF_MAX_CANDS now only controls the number of candidates held in memory while creating routes. FF_MAX_ROUTES has been added to control the number of routes which will be displayed on the webapp.

## v0.6.1 - 2025-06-16

### Fixes

* Update LIDAR parsing to handle files with non-standard dimensions

## v0.6.0 - 2025-06-03

### New Features

* Extensive rewrite of the route creation algorithm in Rust delivering significant performance improvements
* Enabled multi-threaded parsing of LIDAR files
* Enabled loading of LIDAR data without needing to extract the .zip archives first
* Swapped data ingestion code over to pyspark to handle larger input datasets

### Fixes

* Reduced memory usage during data ingestion
* Improved handling of roads with separate footways during data ingestion

## v0.5.0 - 2024-12-20

### Fixes

* Added a GPX export button to the webapp
* Prevented features such as tunnels and bridges from being factored in when calculating elevation loss/gain

  * Road/path geometry in these cases will not match elevations given in the LIDAR dataset

## v0.4.0 - 2024-11-22

### New Features

* Switched to using `poetry` for dependency management
* Initial build of unit tests for route generation algorithm

## v0.3.1 - 2024-08-27

### Fixes

* Adjusted classification of some way types to improve quality of generated routes
* Improved page formatting in webapp

## v0.3.0 - 2024-08-15

### New Features

* Switched route creation algorithm to `RustworkX`, delivering a performance improvement
* Overhaul of data ingestion logic, making better use of data partitioning to help scale beyond small scale datasets

### Fixes

* Further performance improvements to route pruning logic

## v0.2.0 - 2024-07-04

### New Features

* Improvements to the webapp UI

  * Enabled bootstrap to improve page layout
  * Additional configuration options provided to users during route creation

### Fixes

* Performance improvements to route pruning logic


## v0.1.0 - 2024-06-18

This was the first working version of Fell Finder in this repo. Prior to this, development was carried out with each component of the application in its own repository.

### New Features

* Initial working prototype of the Fell Finder application