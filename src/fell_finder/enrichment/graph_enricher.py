"""Handles the joining of graph data with the corresponding elevation data"""

import os

import numpy as np
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import ArrayType, DoubleType, IntegerType


class GraphEnricher:
    def __init__(self, data_dir: str, spark: SparkSession):
        self.data_dir = data_dir
        self.spark = spark

        self.edge_resolution_m = 10

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

    def explode_edges(self, edges: DataFrame) -> DataFrame:
        # Calculate edge distance based on start/end coords
        # each increment ~1m
        edges = edges.withColumn(
            "edge_size_h", F.col("dst_easting") - F.col("src_easting")
        )
        edges = edges.withColumn(
            "edge_size_v", F.col("dst_northing") - F.col("src_northing")
        )
        edges = edges.withColumn(
            "edge_size",
            ((F.col("edge_size_h") ** 2) + (F.col("edge_size_v") ** 2)) ** 0.5,
        )

        # Calculate required number of points
        edges = edges.withColumn(
            "edge_steps",
            F.round(
                (F.col("edge_size") / F.lit(self.edge_resolution_m)) + F.lit(1)
            ).astype(IntegerType()),
        )

        edges = edges.withColumn(
            "edge_steps", F.greatest(F.lit(2), F.col("edge_steps"))
        )

        @F.udf(returnType=ArrayType(DoubleType()))
        def linspace_udf(start, end, n_steps):
            start, end = float(start), float(end)
            range_ = end - start
            if n_steps == 2:
                return [start, end]

            delta = range_ / (n_steps - 1)
            return [start + (delta * step) for step in range(n_steps)]

        @F.udf(returnType=ArrayType(IntegerType()))
        def inx_udf(n_checkpoints):
            return list(range(n_checkpoints))

        edges = edges.withColumn(
            "easting_arr",
            linspace_udf(
                F.col("src_easting"),
                F.col("dst_easting"),
                F.col("edge_steps"),
            ),
        )

        edges = edges.withColumn(
            "northing_arr",
            linspace_udf(
                F.col("src_northing"),
                F.col("dst_northing"),
                F.col("edge_steps"),
            ),
        )

        edges = edges.withColumn("inx_arr", inx_udf(F.col("edge_steps")))

        edges = edges.withColumn(
            "coords_arr",
            F.arrays_zip(
                F.col("inx_arr"), F.col("easting_arr"), F.col("northing_arr")
            ),
        )

        edges = edges.withColumn("coords", F.explode(F.col("coords_arr")))

        edges = edges.select(
            "src",
            "dst",
            "src_lat",
            "src_lon",
            "dst_lat",
            "dst_lon",
            "src_easting",
            "src_northing",
            "dst_easting",
            "dst_northing",
            "type",
            F.col("coords.inx_arr").alias("inx"),
            F.round("coords.easting_arr")
            .astype(IntegerType())
            .alias("easting"),
            F.round("coords.northing_arr")
            .astype(IntegerType())
            .alias("northing"),
            "edge_size",
            "easting_ptn",
            "northing_ptn",
        )

        return edges
