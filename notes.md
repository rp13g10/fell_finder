# To Do

* Set up some different backends for route pruning for easier testing of approaches
* Add in support for step validation using dijkstra's algorithm

## Binning

Update tracking of midpoint of routes, can be done in O(N)

## Pruning

Options to be supported

* Route selection based on elevation
* Route selection based on ratio
* Select with fuzzy deduplication
* Select based purely on chosen metric

Trait `sorter` will need to be defined
Struct `ElevationSorter`
Struct `RatioSorter`
Trait `selector` will need to be defined
Struct `FuzzySelector`
Struct `NaiveSelector`

Maybe wrap everything into overarching struct?

# Ingestion Updates

LIDAR - Possibly no changes needed here

Pull in OSM data in raw format, keep tags in structs
 - Don't parse timestamps, easier to compare in int format
Determine records to be deleted, no real option other than full comparison of IDs
 - Store nodes & ways (not edges) which are in database but not in the new file, remove later in the process
 - Means data can't be stored in parquet any more, iceberg or database preferred
Determine nodes to be updated, use record modified date
 - For batch approach, needs to be a per-record comparison
First, update node data (in case any positions change)
 - Remove any nodes which need removing
 - Process in batches until no more nodes to update
 - For each batch, join onto elevation dataset
Determine edges to be updated, use record modified date of edge OR linked node
Select records to be processed, batches of ~100k? Check total data size & go from there
 - Pick edges, join lat/lon info from nodes
Join with elevation data, use broadcast join?
 - Store elevation as an array of floats to enable post-processing
Remove records previously flagged for deletion, insert new records
Data will need to be stored in a raw format (nodes & edges, no compression at this point)
Remaining processing will need to be done on the entire dataset


storage

nodes
id, lat, lon, timestamp

edges
src, dst, way_id, way_inx, src_lat, src_lon, dst_lat, dst_lon, dist, eles, tags, timestamp

store raw data in postgres, pushdown filtering from spark is supported

## Stages

Storage: delta could be better than postgres (https://delta.io/)

* landing
  * osm - unpack to parquet
  * lidar - current algo, store to parquet
* staging
  * nodes
    * read new files
    * record node IDs to be cleared, remove from DB (fell_finder.staged.nodes)
    * record nodes to be re-processed (join new onto DB, select where new timestamp > old (or old doesn't exist))
    * select first N records, broadcast join onto LIDAR
    * update records in DB, store source file & modified timestamp
    * iterate until no more records to update
  * edges
    * read new files
    * record node IDs to be cleared, remove from DB (fell_finder.staged.edges)
    * record node IDs to be re-processed (either timestamp > old, or source file for related node = current, or old doesn't exist)
      * confirm if second clause is required
    * select first N records, transform schema & broadcast join onto LIDAR
    * update records in DB, store source file & modified timestamp
    * iterate until no more records to update
  * cleanup
    * run compaction on delta tables once run completes
* sanitising
  * edges
    * filter based on tags, extract surface, highway etc as separate fields
    * generate reverse edges where required
    * store to db (fell_finder.sanitised.edges)
  * nodes
    * inner join with edges
    * store to db (fell_finder.sanitised.nodes)
* optimising
  * apply current logic
  * store to db (fell_finder.optimised.nodes/edges)

# Other

* Switch to uv_build backend
* Check storage requirements for full dataset, ~880gb currently available