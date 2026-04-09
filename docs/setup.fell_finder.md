# Setup - Fell Finder

The Fell Finder application takes the form of an API which generates running routes based on user requirements. As an API alone doesn't make for a great user experience, a simple webapp is also provided which allows users to interact with it. It provides the ability to request, view and download routes. While the code-base for this project does make a distinction between the two (with the webapp being called Fell Viewer), for the purposes of this documentation they are considered as part of a single unit.

## Prerequisites

* In order to run the Fell Finder webapp, you must first have followed the guidance on [setting up Fell Loader](./setup.fell_loader.md).

## Configuration

Once data has been loaded into PostgreSQL, navigate to `./docker/fell_finder_compose.yaml`. You will need to make a few adjustments to get it running on your system.

### Volumes

While the code-base is under active development, the python files required to run it are not being baked into the containers. This is primarily driven by poor network connectivity on trains making rebuilds very time consuming.
As an interim solution, the code must instead be mounted as volumes at runtime. This is done in the docker compose file.

``` yaml
volumes:
    fell_viewer:
        driver: local
        driver_opts:
            o: bind
            type: none
            device: /path/to/fell_finder/packages/fell_viewer/src/fell_viewer
    ff_src:
        driver: local
        driver_opts:
            o: bind
            type: none
            device: /path/to/fell_finder/src
    ff_db:
        driver: local
        driver_opts:
            o: bind
            type: none
            device: /path/to/ff_db/
```

You will notice that these are very similar to the volumes used for Fell Loader. As before, you will need to update the `device` parameters to match your system. Take care not to directly copy and paste however, as the package names being mounted are different (fell_loader becomes fell_viewer).

For `ff_db` you should use exactly the same settings as for `fell_loader`, otherwise the database won't have any data available when it starts.

### Variables

You will notice that there are a number of environment variables which can be set in this compose file. The default values will generally be fine, but details of what each one does are provided here. Any variables not noted below should not be modified.

* ff_app
    * FF_DIST_TOLERANCE: Sets the max deviation from the target distance for generated routes. E.g. if the user requests a 10km route and this is set to 0.1, routes between 9km and 11km will be returned. This will be set up as configurable by a user input in a future build.
* ff_api
    * FF_MAX_CANDS - Sets the absolute max number of candidate routes which will be held in memory at any one time. If this is exceeded, routes will be 'pruned' to bring the count back down (see the implementation docs for more info). Note that in most cases the number of candidates will be set dynamically, this global maximum should only take effect for very long routes. Higher values should yield better quality routes, but may dramatically increase run times. Conversely, lower values reduce run times at the expense of route quality.
    * FF_MAX_ROUTES - Sets the maximum number of routes which will be displayed to the user in the webapp.
    * FF_PRUNING_THRESHOLD - Only takes effect when FF_PRUNING_STRATEGY=fuzzy. Must be a value between 0 and 1. Sets the max allowed similarity score between two candidates during the pruning process. Increasing this may result in more homogeneous routes being generated, but reduces the risk of failing to generate any routes at all. Lower values will generate more diverse routes, but may increase failure rates.
    * FF_PRUNING_STRATEGY - Determines how similarity scores between routes are generated. Set this to 'naive' for maximum performance, the algorithm will select the top N routes with no accounting for similarity. Set this to 'fuzzy' for increased route quality, applying fuzzy deduplication before selecting the top N routes. This does come with a significant performance hit.
    * FF_DIJKSTRA_VALIDATION - If this is set to True, candidates which cannot get back to the start point without going over the max distance will be discarded earlier in the process. In practice this has minimal impact, and setting to false is recommended for a performance increase.
    * FF_DISPLAY_THRESHOLD - Sets the max similarity score between two routes which will be returned to the user. Must be a value between 0 and 1.
    * FF_BIN_SIZE - To improve performance, route pruning is carried out in bins across multiple threads. Reducing this will increase performance, but may reduce the quality of generated routes.
    * FF_BIN_STRATEGY - Route binning is done based on route geometry. Set this to last for faster performance, with routes simply binned according to their current position. Set this to centre to increase route quality, with routes binned according to their average position. Performance improvements for centre mode are planned in a future build.
    * FF_FINISHING_OVERLAPS - While the route finding algorithm generally disallows overlapping routes, this requirement is waived towards the end of the route to make it easier to get back to the starting point. This sets the number of junctions prior to the finish where overlaps are allowed.
    * FF_MAX_JOB_SECONDS - Sets the max run time for a job before it is aborted.
    * FF_UPDATE_FREQUENCY - Sets the approximate refresh frequency for the progress bar in seconds.

## Execution

Once you've updated the compose file, you should now be ready to run the webapp.

### Build

First, you need to build the `fell_viewer` image. Future iterations will likely host this online, but for now pre-built images are not available. All of these commands should be run from the root of the repo.

First, use UV to generate a requirements file:

``` bash
uv pip compile packages/fell_viewer/pyproject.toml -o dist/fell_viewer_requirements.txt
```

Then, build the image:

``` bash
docker build -f docker/fell_viewer.dockerfile --tag 'fell_viewer' .
```

You will also need to build the `fell_finder` image. This does not require a requirements file, simply run:

``` bash
docker build -f docker/fell_finder.dockerfile --tag 'fell_finder'
```

### Run

Once both images have been built, they can be executed with docker compose:

``` bash
docker compose -f docker/fell_finder_compose.yaml up
```

Once startup completes, you should be able to view the app [in your browser](http://localhost:8050/).