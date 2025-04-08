# Fell Finder

This package is where the magic happens. It is fully dependent on OSM & Lidar data having been loaded into a postgres database using [fell_loader](packages/fell_loader/README.md), so make sure you've brought in the data before trying to run anything.

Once the database has been set up, you can run this code to expose an API for route requests. This can be called manually, or (much easier) by using the webapp provided by [fell_viewer](packages/fell_viewer/README.md).

Full documentation for this package is still pending, as it's likely to change quite significantly when tests are written and various TODO notes are addressed.

## Installation

This package needs to be built before it can be executed. It is recommended that this be done by executing `cargo build --release` via the command line (from this folder).

## Usage

* The compiled binary can be executed using bash

// TODO