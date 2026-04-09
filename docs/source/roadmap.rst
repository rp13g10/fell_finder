=======
Roadmap
=======

Current State
=============

This project is still in early development, with significant work still required before it's ready for general use. That said, with a little leg-work a viable PoC is now up and running. At present, the ingestion pipeline is largely complete (although it's executed on-demand at the moment). A basic webapp is provided, which is able to plot routes on demand and display them to the user. Route creation has been sped up significantly following a rewrite in rust, although longer distances can still take a few seconds to generate.

While there is still significant work to be done, work is now focused more around tidying up rough edges and improving performance. The basic functionality is now in place. I have stopped short of hosting anything on the cloud for the time being, but the app works well hosted locally.

Future Goals
============

The general roadmap for this project is outlined below. Items are not listed in any specific order.

Fell Loader
-----------

* Identify ways to further improve the accuracy of calculated elevation gain/loss

  * Other sources of elevation data to be evaluated, candidates are OS Terrain and SRTM
  * Potential to smooth out route profiles with post-processing, although this would result in route creation/selection being done based on suboptimal data
  * Investigate the use of the OSM dataset to identify areas in the LIDAR data where elevation is likely to be incorrect/invalid. Trees, tunnels, ditches, etc.
* Set up an airflow pipeline for ingestion, to be triggered when new files are added
  * Move to an elastic compute model, deallocating resource while no data loads are in progress

Fell Finder
-----------

* Continue efforts to optimise performance further
* Add an endpoint for identifying large hills / flat roads within a radius of a particular point
* Ensure the code is as close to panic-free as possible
* Revisit after building some more projects in Rust

Fell Viewer
-----------

* Additional views

  * hill/flat road identification
  * manual route creation

* Improve existing views with greater customisation of the route creation algorithm
* Integrations with Strava, Komoot, Garmin, etc

  * Ability to push generated routes to other profiles
  * Pull in activity history and build out a dashboard with stats?

* Add user logins, allowing users to save / retrieve routes

  * Likely prerequisite for hosting online, reduce impact of high traffic volumes

* Admin dashboard to help monitor performance
* Potential to change to a more performant backend than Flask
* Potential to change to a more flexible frontend (JS?)
* Show the available route creation space as users select different filters

Known Issues / Limitations
==========================

* Estimated gain/loss may vary by up to ~30% vs. the real world

  * This is not a unique problem! Estimates do seem to be in-line with some other services (notably, Garmin)
  * Part of the issue seems to be with the LIDAR data itself being a few metres out at times (vs. Strava)
  * Other issues include bridges/tunnels where the path stays level but the ground does not. Some mitigations are in place for this, but making better use of the OSM tags may be able to improve the situation further.
  * High accuracy of the LIDAR system also leads to some false-positive elevation changes. A flat road with a large tree, might appear to have a small hill on it in the data.

* While greatly improved, route generation does occasionally fail. Investigation is required to see if completion rates can be improved without drastically increasing run times.