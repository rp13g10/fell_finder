# Notes

## Prerequisites

* [Docker](https://docs.docker.com/engine/install/)
  * [Post-install](https://docs.docker.com/engine/install/linux-postinstall/)
* K8s
  * [kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/#install-using-native-package-management)
  * [minikube](https://minikube.sigs.k8s.io/docs/start/?arch=%2Flinux%2Fx86-64%2Fstable%2Fbinary+download)
* Images
  * [Postgres](https://hub.docker.com/_/postgres)
  * [Redis](https://hub.docker.com/_/redis)
  * [Traefik](https://hub.docker.com/_/traefik)
  * [Python](https://hub.docker.com/_/python)
  * [Rust](https://hub.docker.com/_/rust)

## Resource requirements

TODO: Update with runtime on own hardware

The fell_loader container is by far the most resource intensive. This is a one-off execution, but may take a day or two to run if you're loading in the whole of the UK. For single counties, expect runtimes to be closer to 1 hour. I would recommend having at least the following available:

* 32GB RAM
* 2TB Storage (available and mounted under ff_data)

## Components

* API Server
* Ingestion
* Webserver
* Postgres
* Redis
* Load balancer

## Data Sources

These are set under `volumes` in compose.yaml

## Execution

* Docker
  * `sudo systemctl start docker` for one-time run
  * `sudo systemctl enable --now docker` to auto-run on boot


## Build

The commands below should be executed from the root of the repo. The following images will need to be pulled:

* redis:8.6.1
* postgres:18.1
* rust:1.93
* python:3.12

TODO: Add details of load balancer once in place

### Fell Loader

* `uv pip compile packages/fell_loader/pyproject.toml -o dist/fell_loader_requirements.txt`
  * Generate requirements file for fell_loader package
  * Add `--verbose` flag if this hangs to get a traceback
* `docker build -f docker/fell_loader.dockerfile --tag 'fell_loader' .`
  * Build and tag the image

### Fell Finder

* `docker build -f docker/fell_finder.dockerfile --tag 'fell_finder' .`
  * Build and tag the image

### Fell Viewer

* `uv pip compile packages/fell_viewer/pyproject.toml -o dist/fell_viewer_requirements.txt`
  * Generate requirements file for fell_viewer package
* `docker build -f docker/fell_viewer.dockerfile --tag 'fell_viewer' .`
  * Build and tag the image


## Run

Initially this is being set up with docker-compose, to be moved over to k8s once the basic structure is in place

* `docker compose -f docker/fell_loader_compose.yaml up`
* `docker compose -f docker/fell_finder_compose.yaml up`

Please note that the workload for fell_loader is not distributed, as it's being executed on my local device. As such, I have adopted a single executor architecture to make best use of the available hardware.

To run the database on its own (e.g. to inspect table contents), run

* `docker run -p 5432:5432 -e POSTGRES_DB=postgres -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres --mount type=bind,src=/home/ross/ff_db,dst=/var/lib/postgresql postgres:18.1`

## Rough next steps

* Set fell_loader_compose to exit gracefully once ingestion completes?
* Get everything working with one replica of each image
* Scale out, add load balancer
* Housekeeping before merge