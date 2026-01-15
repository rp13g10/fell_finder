"""Sanitise the node data by restricting the scope of the dataset to only
the nodes which have a corresponding (usable) edge.
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from fell_loader.base import BaseSparkLoader
from fell_loader.schemas.sanitised import NODES_SCHEMA

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
        # TODO: Move this into base class, implements same logic as
        #       `fell_loader.landing.osm.drop_nodes_not_in_edges`
        src_ids = edges.select(F.col("src").alias("id"))
        dst_ids = edges.select(F.col("dst").alias("id"))
        ids = src_ids.union(dst_ids).distinct()

        nodes = nodes.join(ids, on="id", how="inner")

        return nodes

    def run(self) -> None:
        """End-to-end loading script for nodes going into the sanitised
        layer. This simply filters the nodes dataset to include only those
        which have a corresponding edge.
        """
        nodes = self.read_delta(layer="staging", dataset="nodes")
        edges = self.read_parquet(layer="sanitised", dataset="edges")

        nodes = self.remove_unused_nodes(nodes, edges)
        nodes = self.map_to_schema(nodes, NODES_SCHEMA)

        self.write_parquet(nodes, layer="sanitised", dataset="nodes")
