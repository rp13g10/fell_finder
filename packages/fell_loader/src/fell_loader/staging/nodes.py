"""Staging logic for the nodes dataset. Assigns elevations to all nodes, with
updates applied as a delta on top of data already in the staging layer."""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Read new files
# Get node IDs to be cleared
# Get node IDs to be re-processed
# Tag with elevation and update in staging in chunks

CHUNK_SIZE = 100000


class NodeStager:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

        self.data_dir = os.environ["FF_DATA_DIR"]

    def get_nodes_to_remove(self) -> list[int]:
        new_nodes = self.spark.read.parquet(
            os.path.join(self.data_dir, "landing", "nodes")
        ).select("id")

        current_nodes = self.spark.read.parquet(
            os.path.join(self.data_dir, "staging", "nodes")
        ).select("id")

        # Need to remove nodes in staging layer which are not in the landing
        # layer
        to_delete = current_nodes.join(new_nodes, on="id", how="anti")

        to_delete = [row["id"] for row in to_delete.collect()]

        return to_delete

    def get_nodes_to_update(self) -> DataFrame:
        # NOTE: Expected volume is ~7m records in each

        new_nodes = self.spark.read.parquet(
            os.path.join(self.data_dir, "landing", "nodes")
        )

        # TODO: Handle cases where staging directory doesn't exist yet
        current_nodes = self.spark.read.parquet(
            os.path.join(self.data_dir, "staging", "nodes")
        ).select("id", F.col("timestamp").alias("cur_timestamp"))

        to_update = (
            new_nodes.join(current_nodes, on="id")
            .filter(F.col("timestamp") > F.col("cur_timestamp"))
            .drop("cur_timestamp")
        ).limit(CHUNK_SIZE)

        return to_update

    @staticmethod
    def read_elevation(nodes: DataFrame, bounds: DataFrame) -> DataFrame:
        # Get file IDs for nodes by joining onto bounds and getting distinct
        # values

        # Collect required IDs as list of strings

        # Read in elevation dataset, limit to specific files by passing
        # through as paths on read

        ...

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

        nodes = F.broadcast(nodes)

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
        nodes = nodes.select("id", "lat", "lon", "elevation", "timestamp")

        return nodes

    @staticmethod
    def apply_node_updates(nodes: DataFrame) -> None:
        # Apply updates as delta to staging layer

        ...
