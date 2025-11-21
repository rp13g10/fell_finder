"""Staging logic for the nodes dataset. Assigns elevations to all nodes, with
updates applied as a delta on top of data already in the staging layer."""

import logging
import os

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# TODO: See what happens when this gets executed

CHUNK_SIZE = 100_000

logger = logging.getLogger(__name__)


class NodeStager:
    """Defines staging logic for the nodes dataset."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

        self.data_dir = os.environ["FF_DATA_DIR"]
        self.removals_applied = False
        self.updates_applied = False

    def initialize_output_table(self) -> None:
        """If no data is present in the staging layer, create an empty table"""

        DeltaTable.createIfNotExists(self.spark).location(
            os.path.join(self.data_dir, "staging", "nodes")
        ).addColumns(
            [
                T.StructField("id", T.LongType()),
                T.StructField("lat", T.DoubleType()),
                T.StructField("lon", T.DoubleType()),
                T.StructField("elevation", T.DoubleType()),
                T.StructField("timestamp", T.LongType()),
            ]
        ).execute()

    def get_nodes_to_remove(self) -> list[int]:
        """Determine which nodes need to be removed by finding any node IDs
        which are present in the existing nodes dataset, but not in the new
        one.

        Returns:
            A list of node IDs to be removed

        """

        if self.removals_applied:
            return []

        new_nodes = self.spark.read.parquet(
            os.path.join(self.data_dir, "landing", "nodes")
        ).select("id")

        # Staging layer must be read in as delta table
        current_nodes = (
            DeltaTable.forPath(
                self.spark, os.path.join(self.data_dir, "staging", "nodes")
            )
            .toDF()
            .select("id")
        )

        # Need to remove nodes in staging layer which are not in the landing
        # layer
        to_delete = current_nodes.join(new_nodes, on="id", how="anti")

        to_delete = [row["id"] for row in to_delete.collect()]

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
        new_nodes = self.spark.read.parquet(
            os.path.join(self.data_dir, "landing", "nodes")
        )

        current_nodes = (
            DeltaTable.forPath(
                self.spark, os.path.join(self.data_dir, "staging", "nodes")
            )
            .toDF()
            .select(
                "id",
                F.col("timestamp").alias("cur_timestamp"),
            )
        )

        # Determine which nodes need updating
        update_mask = F.col("timestamp") > F.col("cur_timestamp")
        new_mask = F.col("cur_timestamp").isNull()
        to_update = (
            new_nodes.join(current_nodes, on="id", how="left")
            .filter(update_mask | new_mask)
            .drop("cur_timestamp")
            # TODO: Set this up w. window function to make sure it works as
            #       expected.
        )

        # Sort according to BNG grid to minimise number of LIDAR partitions
        # required, then select first N records
        to_update = to_update.orderBy(
            # NOTE: Each LIDAR file has dimensions 5000x5000, so this is
            #       analogous to sorting by file ID
            F.try_divide(F.col("easting"), F.lit(5000)).cast("integer"),
            F.try_divide(F.col("northing"), F.lit(5000)).cast("integer"),
        ).limit(CHUNK_SIZE)

        # If no data is returned, the load operation can be stopped
        to_update_count = to_update.count()
        if not to_update_count:
            self.updates_applied = True
            logger.info("No further updates required")
        else:
            logger.info(f"Updating {to_update_count:,.0f} nodes")

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
        bounds = self.spark.read.parquet(
            os.path.join(self.data_dir, "landing", "lidar_bounds.parquet")
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
            os.path.join(self.data_dir, "landing", "lidar", f"{file}.parquet")
            for file in files
        ]
        elevation = self.spark.read.parquet(*paths)

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

    @staticmethod
    def set_node_output_schema(nodes: DataFrame) -> DataFrame:
        """Bring through only the required columns for the enriched node
        dataset

        Args:
            nodes: The enriched node dataset

        Returns:
            A subset of the input dataset

        """
        nodes = nodes.select(
            F.col("id").astype(T.LongType()),
            F.col("lat").astype(T.DoubleType()),
            F.col("lon").astype(T.DoubleType()),
            F.col("elevation").astype(T.DoubleType()),
            F.col("timestamp").astype(T.LongType()),
        )

        return nodes

    def apply_node_updates(
        self, updates: DataFrame, removals: list[int]
    ) -> None:
        """Apply a chunk of node updates to the data in the staging layer.

        Args:
            updates: A dataframe containing updated data for a chunk of nodes
            removals: A list of node IDs which should be removed

        """

        logger.debug("Writing node updates to temp folder")
        updates.write.format("delta").mode("overwrite").save(
            os.path.join(self.data_dir, "temp/node_updates")
        )

        # Create references to target table & updates
        nodes_tbl = DeltaTable.forPath(
            self.spark, os.path.join(self.data_dir, "staging", "nodes")
        ).alias("nodes")
        updates_tbl = DeltaTable.forPath(
            self.spark, os.path.join(self.data_dir, "temp", "node_updates")
        )
        updates_df = updates_tbl.toDF().alias("updates")

        # Update or insert records
        logger.info("Applying node updates")
        nodes_tbl.merge(
            updates_df, condition="nodes.id = updates.id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        logger.info("Node updates applied")

        # Remove records
        if removals:
            logger.info(f"Removing {len(removals):,.0f} nodes")
            nodes_tbl.delete(F.col("id").isin(*removals))
            self.removals_applied = True

    def optimise_nodes_table(self) -> None:
        """Once the data load operation has finished, optimise the files in
        the staging layer to prevent file counts from getting out of control.
        """
        nodes_tbl = DeltaTable.forPath(
            self.spark, os.path.join(self.data_dir, "staging", "nodes")
        )
        logger.info("Optimizing nodes table")
        nodes_tbl.optimize().executeCompaction()
        logger.info("Optimization completed")

    def load(self) -> None:
        """End-to-end script for the staging of node data. This reads in the
        contents of the nodes dataset from the landing layer, calculates which
        records need to be updated and tags them with elevation data. Any nodes
        which have not been changed since the last data load will not be
        modified.
        """

        self.initialize_output_table()

        to_remove = self.get_nodes_to_remove()
        to_update_df = self.get_nodes_to_update()

        while not self.updates_applied:
            elevation_df = self.read_elevation(to_update_df)

            to_update_df = self.tag_nodes(to_update_df, elevation_df)
            to_update_df = self.set_node_output_schema(to_update_df)

            self.apply_node_updates(to_update_df, to_remove)

            to_remove = self.get_nodes_to_remove()
            to_update_df = self.get_nodes_to_update()

        self.optimise_nodes_table()
