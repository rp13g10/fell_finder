# Fell Loader

This package is responsible for processing the raw OSM & Lidar feeds into a format which can be used by [fell_finder](packages/fell_finder/README.md). At present, it is executed on-demand. In future, the intention is to set up up as part of an airflow pipeline.

## Usage

* Select a folder which will contain your data files, set the `FF_DATA_DIR` environment variable to point to this dictory
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
* You should now be able to execute `ingestion.py` at the root level of this repo
  * As we're using UV, the command to do this is `uv run python ingestion.py`
  * In my own testing, it takes ~1 hour to completely process the data for all of Hampshire