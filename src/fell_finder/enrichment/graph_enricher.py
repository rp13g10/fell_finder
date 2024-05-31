"""Handles the joining of graph data with the corresponding elevation data"""

import os

from pyspark.sql import SparkSession, DataFrame, functions as F


class GraphEnricher:
    def __init__(self, data_dir: str, spark: SparkSession):
        self.data_dir = data_dir
        self.spark = spark

    def load_elevation_df(self) -> DataFrame:
        ele_dir = os.path.join(self.data_dir, "parsed/lidar")

        ele_df = self.spark.read.parquet(ele_dir)

        return ele_df

    def load_node_df(self) -> DataFrame:
        node_dir = os.path.join(self.data_dir, "parsed/nodes")

        node_df = self.spark.read.parquet(node_dir)

        return node_df

    def load_edge_df(self) -> DataFrame:
        edge_dir = os.path.join(self.data_dir, "parsed/edges")

        edge_df = self.spark.read.parquet(edge_dir)

        return edge_df

    def tag_nodes(self, nodes: DataFrame, elevation: DataFrame) -> DataFrame:
        tagged = nodes.join(
            elevation,
            on=["easting_ptn", "northing_ptn", "easting", "northing"],
            how="inner",
        )

        tagged = tagged.select(
            "id", "lat", "lon", "elevation", "easting_ptn", "northing_ptn"
        )

        return tagged

    def explode_edges(self, edges: DataFrame) -> DataFrame: ...
