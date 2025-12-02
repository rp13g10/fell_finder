"""Sanitise the node data by restricting the scope of the dataset to only
the nodes which have a corresponding (usable) edge.
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from fell_loader.base import BaseSparkLoader

logger = logging.getLogger(__name__)


class NodeSanitiser(BaseSparkLoader):
    """Defines sanitising logic for the nodes dataset"""

    @staticmethod
    def remove_unused_nodes(nodes: DataFrame, edges: DataFrame) -> DataFrame:
        """Remove any nodes which don't have a corresponding edge in the
        (filtered) edges dataset in the sanitised layer.

        Args:
            nodes: The nodes dataset from the staging layer
            edges: The edges dataset from the sanitised layer

        Returns:
            The nodes dataset to be written to the sanitised layer
        """
        src_ids = edges.select(F.col("src").alias("id"))
        dst_ids = edges.select(F.col("dst").alias("id"))
        ids = src_ids.union(dst_ids).distinct()

        nodes = nodes.join(ids, on="id", how="inner")

        return nodes

    @staticmethod
    def set_output_schema(nodes: DataFrame) -> DataFrame:
        """Ensure only the required columns are written out to disk, enforce
        expected datatypes

        Args:
            nodes: The nodes dataset to be written to the sanitised layer

        Returns:
            A copy of the input dataframe, with a sanitised schema
        """
        return nodes.select(
            F.col("id").astype(T.LongType()),
            F.col("lat").astype(T.DoubleType()),
            F.col("lon").astype(T.DoubleType()),
            F.col("elevation").astype(T.DoubleType()),
        )

    def run(self) -> None:
        """End-to-end loading script for nodes going into the sanitised
        layer. This simply filters the nodes dataset to include only those
        which have a corresponding edge.
        """
        nodes = self.read_delta(layer="staging", dataset="nodes")
        edges = self.read_parquet(layer="sanitised", dataset="edges")

        nodes = self.remove_unused_nodes(nodes, edges)
        nodes = self.set_output_schema(nodes)

        self.write_parquet(nodes, layer="sanitised", dataset="nodes")
