# Fell Loader

This package is responsible for processing the raw OSM & Lidar feeds into a format which can be used by [fell_finder](packages/fell_finder/README.md). At present, it is executed on-demand. In future, the intention is to set it up as part of an airflow pipeline.

## Prerequisites

The final stage of this pipeline is the upload of enriched map data to a Postgres database. Until this app has been containerised, some setup is required.

* Install postgresql (instructions will vary depending on your OS)
* Create a username and password for the database
* Create an empty database called 'fell_finder'

## Usage

* Select a folder which will contain your data files, set the `FF_DATA_DIR` environment variable to point to this dictory
* Create an 'extracts' folder in your data directory
* Create 'osm' and 'lidar' folders in the 'extracts' folder
* In extracts/osm, you'll need to place a .osm.pbf file covering the area you want
* In extracts/lidar, you'll need to place the corresponding LIDAR data (they can be left as .zip archives)
  * This can be downloaded [here](https://environment.data.gov.uk/survey)
  * After selecting the area to download, select 'LIDAR Composite DTM / 2022 / 1m'
* You should now be able to execute `ingestion.py` at the root level of this repo
  * As we're using UV, the command to do this is `uv run --env-file .env python src/fell_finder_app/ingestion.py`
  * In my own testing, it takes ~1 hour to completely process the data for all of Hampshire

## Notes

### Authentication on Linux

These issues will be resolved when the app has been migrated to Docker, but until then some setup may be required to connect to Postgres during the ingestion process. Assuming you're only using postgres to run this app and are not using it to host anything else, you can make the following changes to get connected:

* sudo nano /var/lib/pgsql/data/pg_hba.conf (location on Fedora, other distros may vary)
  * This configuration file controls authentication methods for the postgresql server
* Set `method` to 'trust' for `type=local & database=all`
  * This is the change which gets `fell_loader` working
* Set `method` to `password` for `type=host & address=127.0.0.1/32` and `type=host & address=::1/128`
  * This should allow you to log in to the database using pgAdmin
* Restart the database with `sudo systemctl restart postgresql`

If you _are_ using postgres to host something else on your system, then you make these changes at your own risk. If in doubt, don't touch anything!