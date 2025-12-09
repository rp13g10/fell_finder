"""Staging logic for the nodes dataset. Assigns elevations to all nodes, with
updates applied as a delta on top of data already in the staging layer.
"""

import contextlib
import logging
import shutil

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from fell_loader.base import BaseSparkLoader
from fell_loader.schemas import landing as lnd
from fell_loader.schemas import staging as stg

CHUNK_SIZE = 100_000

logger = logging.getLogger(__name__)


class NodeStager(BaseSparkLoader):
    """Defines staging logic for the nodes dataset."""

    def __init__(self, spark: SparkSession) -> None:
        super().__init__(spark)

        self.removals_applied = False
        self.updates_applied = False

        self.remaining_count = -1
        self.current_chunk_size = -1

    def initialize_output_table(self) -> None:
        """If no data is present in the staging layer, create an empty table"""
        DeltaTable.createIfNotExists(self.spark).location(
            (self.data_dir / "staging" / "nodes").as_posix()
        ).addColumns(stg.NODES_SCHEMA.fields).execute()

    def check_for_lidar_update(self) -> bool:
        """Determine whether there has been any change in the LIDAR data
        available since the last run. This cross-checks the current contents
        of the 'landing/lidar' folder in the data directory against
        'last_run_lidar.txt' in the 'staging' folder in the data directory.
        This file is written by the `EdgeStager` component at the end of each
        successful run.

        Returns:
            True if there has been a change in the available LIDAR data, False
            if there hasn't
        """
        lidar_files = (self.data_dir / "landing" / "lidar").glob("*.parquet")
        lidar_files = (path.as_posix() for path in lidar_files)

        last_run_loc = self.data_dir / "staging" / "last_run_lidar.txt"
        if not last_run_loc.exists():
            logger.debug("No record of lidar files from previous runs")
            last_run_files = ""
        else:
            logger.debug("Found record of previous lidar runs")
            last_run_files = last_run_loc.read_text()

        updated = str(sorted(lidar_files)) != last_run_files.strip()
        logger.debug(f"Lidar files updated: {updated}")
        return updated

    def clear_data_without_elevation(self) -> None:
        """Drop any records which don't currently have a value for elevation.
        This will cause them to be re-processed, and tagged with elevation
        using the latest set of available LIDAR data. This should only be
        triggered when the LIDAR data changes.
        """
        logger.info("Dropping records where elevation is NULL")

        nodes_tbl = DeltaTable.forPath(
            self.spark, (self.data_dir / "staging" / "nodes").as_posix()
        )

        nodes_tbl.delete(F.col("elevation").isNull())

        logger.debug("Records dropped successfully")

    def get_nodes_to_remove(self) -> DataFrame | None:
        """Determine which nodes need to be removed by finding any node IDs
        which are present in the existing nodes dataset, but not in the new
        one.

        Returns:
            A dataframe of node IDs to be removed, or None (if no removals are
            required)

        """
        if self.removals_applied:
            return None

        new_nodes = self.read_parquet(layer="landing", dataset="nodes").select(
            "id"
        )

        # Staging layer must be read in as delta table
        current_nodes = self.read_delta(
            layer="staging", dataset="nodes"
        ).select("id")

        # Need to remove nodes in staging layer which are not in the landing
        # layer
        to_delete = (
            current_nodes.join(new_nodes, on="id", how="anti")
            .select("id")
            .alias("removals")
        )

        return to_delete

    def get_nodes_to_update(self) -> DataFrame:
        """Determine which nodes need to be updated. Updates are required
        either when the last modified timestamp for a node has changed (as
        someone might have improved its position on the map), or the node
        is a new addition to the map.

        Nodes are updated in chunks due to the size of the table we need to
        join them to. This helps to reduce storage requirements, and more
        importantly allows the join to be aborted & resumed midway through.

        Returns:
            A dataframe containing a chunk of nodes which need to be updated

        """
        logger.debug("Determining nodes to update")
        # Read in new & existing datasets
        new_nodes = self.read_parquet(layer="landing", dataset="nodes")

        current_nodes = self.read_delta(
            layer="staging", dataset="nodes"
        ).select(
            "id",
            F.col("timestamp").alias("cur_timestamp"),
        )

        # Determine which nodes need updating
        update_mask = F.col("timestamp") > F.col("cur_timestamp")
        new_mask = F.col("cur_timestamp").isNull()
        to_update = (
            new_nodes.join(current_nodes, on="id", how="left")
            .filter(update_mask | new_mask)
            .drop("cur_timestamp")
        )

        if self.remaining_count == -1:
            self.remaining_count = to_update.count()

        # Sort according to BNG grid to minimise number of LIDAR partitions
        # required, then select first N records
        to_update = to_update.orderBy(
            # NOTE: Each LIDAR file has dimensions 5000x5000, so this is
            #       analogous to sorting by file ID
            F.try_divide(F.col("easting"), F.lit(5000)).cast("integer"),
            F.try_divide(F.col("northing"), F.lit(5000)).cast("integer"),
        ).limit(CHUNK_SIZE)

        # If no data is returned, the load operation can be stopped
        self.current_chunk_size = to_update.count()
        if not self.current_chunk_size:
            self.updates_applied = True
            logger.info("No further updates required")
        else:
            logger.info(
                f"Updating {self.current_chunk_size:,.0f} of "
                f"{self.remaining_count:,.0f} nodes"
            )

        return to_update

    def read_elevation(self, nodes: DataFrame) -> DataFrame:
        """Read in the elevation data required to tag the nodes in a
        particular chunk of records. Use the known bounds of the different
        partitions in the elevation dataset to limit the volume of data which
        is read in.

        Args:
            nodes: A dataframe containing a chunk of nodes to be updated

        Returns:
            A dataframe containing the elevation data for the area around the
            provided nodes

        """
        bounds = self.read_parquet(
            layer="landing", dataset="lidar_bounds.parquet"
        )

        # Get file IDs for nodes by joining onto bounds and getting distinct
        # values
        join_cond = (
            (nodes.easting >= bounds.min_easting)
            & (nodes.easting < bounds.max_easting)
            & (nodes.northing >= bounds.min_northing)
            & (nodes.northing < bounds.max_northing)
        )

        # Collect required IDs as list of strings
        files_df = (
            nodes.join(F.broadcast(bounds), on=join_cond)
            .select("file_id")
            .dropDuplicates()
        )

        files = [row.file_id for row in files_df.collect()]
        logger.debug("Loading elevation for: %s", repr(files))

        # Read in elevation dataset, limiting to only the file paths required
        paths = [
            (
                self.data_dir / "landing" / "lidar" / f"{file}.parquet"
            ).as_posix()
            for file in files
        ]

        # Schema must be provided, as `paths` may be empty
        elevation = self.spark.read.schema(lnd.LIDAR_SCHEMA).parquet(*paths)

        return elevation

    @staticmethod
    def tag_nodes(nodes: DataFrame, elevation: DataFrame) -> DataFrame:
        """Join node and elevation tables together based on their easting and
        northing coordinates. As each node represents a single point coordinate
        this is a straightforward operation.

        Args:
            nodes: A table representing nodes in the OSM graph
            elevation: A table containing elevation data at different
              coordinates

        Returns:
            A copy of nodes with an additional elevation field

        """
        # NOTE: This needs to be a left join otherwise any nodes which are not
        #       present in the LIDAR dataset will crop up as needing to be
        #       updated on every iteration

        tagged = nodes.join(
            elevation,
            on=["easting", "northing"],
            how="left",
        )

        return tagged

    def apply_node_updates(
        self, updates: DataFrame, removals: DataFrame | None
    ) -> None:
        """Apply a chunk of node updates to the data in the staging layer.

        Args:
            updates: A dataframe containing updated data for a chunk of nodes
            removals: A list of node IDs which should be removed

        """
        logger.debug("Writing node updates to temp folder")
        updates.write.format("delta").mode("overwrite").save(
            (self.data_dir / "temp" / "node_updates").as_posix()
        )

        # Create references to target table & updates
        nodes_tbl = DeltaTable.forPath(
            self.spark, (self.data_dir / "staging" / "nodes").as_posix()
        ).alias("nodes")
        updates_tbl = DeltaTable.forPath(
            self.spark, (self.data_dir / "temp" / "node_updates").as_posix()
        )
        updates_df = updates_tbl.toDF().alias("updates")

        # Update or insert records
        logger.info("Applying node updates")
        nodes_tbl.merge(
            updates_df, condition="nodes.id = updates.id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        logger.info("Node updates applied")

        # Remove records
        if removals is not None:
            remove_count = removals.count()
            logger.info(
                f"Removing {remove_count} nodes no longer present in map"
            )
            nodes_tbl.merge(
                removals, condition="nodes.id = removals.id"
            ).whenMatchedDelete().execute()
            logger.info("Node removals applied")
            self.removals_applied = True

    def optimise_nodes_table(self) -> None:
        """Once the data load operation has finished, optimise the files in
        the staging layer to prevent file counts from getting out of control.
        """
        nodes_tbl = DeltaTable.forPath(
            self.spark, (self.data_dir / "staging" / "nodes").as_posix()
        )
        logger.info("Optimizing nodes table")
        nodes_tbl.optimize().executeCompaction()
        nodes_tbl.vacuum()
        logger.info("Optimization completed")

    def clear_temp_files(self) -> None:
        """One the data load operation has finished, clear out any files in
        the temp directory
        """
        logger.info("Clearing temp directory")
        with contextlib.suppress(FileNotFoundError):
            shutil.rmtree((self.data_dir / "temp" / "node_updates").as_posix())

    def run(self) -> None:
        """End-to-end script for the staging of node data. This reads in the
        contents of the nodes dataset from the landing layer, calculates which
        records need to be updated and tags them with elevation data. Any nodes
        which have not been changed since the last data load will not be
        modified.
        """
        self.initialize_output_table()

        if self.check_for_lidar_update():
            self.clear_data_without_elevation()

        to_remove = self.get_nodes_to_remove()
        to_update_df = self.get_nodes_to_update()

        while not self.updates_applied:
            # Attempt to free up memory before each new iteration
            self.spark._jvm.System.gc()  # type: ignore

            elevation_df = self.read_elevation(to_update_df)

            to_update_df = self.tag_nodes(to_update_df, elevation_df)
            to_update_df = self.map_to_schema(to_update_df, stg.NODES_SCHEMA)

            self.apply_node_updates(to_update_df, to_remove)

            self.remaining_count -= self.current_chunk_size

            to_remove = self.get_nodes_to_remove()
            to_update_df = self.get_nodes_to_update()

        self.optimise_nodes_table()
        self.clear_temp_files()
