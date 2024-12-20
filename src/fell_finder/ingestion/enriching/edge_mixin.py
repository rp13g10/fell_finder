"""Defines methods for the GraphEnricher class relating to the processing
of edges in the graph"""

from abc import ABC, abstractmethod
from typing import List


from geopy.distance import distance
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
)
from pyspark.sql.window import Window


class EdgeMixin(ABC):
    """Defines the methods required to enrich edge data"""

    @abstractmethod
    def __init__(self) -> None:
        """Defines the attributes required to enrich edge data"""
        self.data_dir: str
        self.spark: SparkSession
        self.edge_resolution_m: int

    def calculate_step_metrics(self, edges: DataFrame) -> DataFrame:
        """Work out the number steps which each edge will need to be broken
        down into in order to calculate the elevation gain/loss at the
        specified resolution. The number of steps will always be at least 2, to
        ensure that elevation at the start and end points is always taken into
        account.

        Args:
            edges: A table representing edges in the OSM graph

        Returns:
            A copy of the input table with an additional 'edge_steps' column
        """

        # Calculate edge distance based on start/end coords
        # each increment is ~1m, disregarding the curvature of the earth
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
        return edges

    @staticmethod
    def explode_edges(edges: DataFrame) -> DataFrame:
        """Starting from one record per edge in the OSM graph, explode the
        dataset out to (approximately) one record for every 10 metres along
        each edge. The 10m resolution can be changed via the
        self.edge_resolution_m parameter.

        Args:
            edges: A table representing edges in the OSM graph

        Returns:
            An exploded view of the input dataset
        """

        @F.udf(returnType=ArrayType(DoubleType()))
        def linspace_udf(start: int, end: int, n_steps: int) -> List[float]:
            """Return a list of n_steps evenly spaced points between the start
            and end coordinates.

            Args:
                start: The starting easting/northing
                end: The ending easting/northing
                n_steps: The number of steps to generate

            Returns:
                A list of evenly spaced points between start and end
            """
            start_f, end_f = float(start), float(end)
            range_ = end_f - start_f
            if n_steps == 2:
                return [start_f, end_f]

            delta = range_ / (n_steps - 1)
            return [start_f + (delta * step) for step in range(n_steps)]

        @F.udf(returnType=ArrayType(IntegerType()))
        def inx_udf(n_checkpoints: int) -> List[int]:
            """Returns a list of indexes between 0 and n_checkpoints

            Args:
                n_checkpoints: The number of indexes to generate

            Returns:
                A list of indexes between 0 and n_checkpoints
            """
            return list(range(n_checkpoints))

        # Generate the eastings, northings and indexes for each point
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

        # Zip them into a single column
        edges = edges.withColumn(
            "coords_arr",
            F.arrays_zip(
                F.col("inx_arr"), F.col("easting_arr"), F.col("northing_arr")
            ),
        )

        # Explode the dataset to one record per step
        edges = edges.withColumn("coords", F.explode(F.col("coords_arr")))
        edges = edges.drop(
            "inx_arr", "easting_arr", "northing_arr", "coords_arr"
        )

        return edges

    @staticmethod
    def unpack_exploded_edges(edges: DataFrame) -> DataFrame:
        """Standardize the schema of the exploded edges dataset, unpacking
        indexes, eastings and northing for each step into standard columns.

        Args:
            edges: A table representing exploded edges in the OSM
              graph

        Returns:
            A tidied up view of the input dataset
        """
        edges = edges.select(
            "src",
            "dst",
            "src_lat",
            "src_lon",
            "dst_lat",
            "dst_lon",
            "src_easting",
            "src_northing",
            "way_id",
            "way_inx",
            "highway",
            "surface",
            "is_flat",
            F.col("coords.inx_arr").alias("inx"),
            F.round(F.col("coords.easting_arr"))
            .astype(IntegerType())
            .alias("easting"),
            F.round(F.col("coords.northing_arr"))
            .astype(IntegerType())
            .alias("northing"),
        )
        return edges

    @staticmethod
    def tag_exploded_edges(
        edges: DataFrame, elevation: DataFrame
    ) -> DataFrame:
        """Join the exploded edges dataset onto the elevation table to retrieve
        the elevation at multiple points along each edge.

        Args:
            edges: A table representing exploded edges in the OSM graph
            elevation: A table containing elevation data

        Returns:
            A copy of the edges dataframe with an additional elevation column
        """
        tagged = edges.join(
            elevation,
            on=["easting_ptn", "northing_ptn", "easting", "northing"],
            how="inner",
        )

        tagged = tagged.select(
            "src",
            "dst",
            "src_lat",
            "src_lon",
            "dst_lat",
            "dst_lon",
            "src_easting",
            "src_northing",
            "inx",
            "elevation",
            "way_id",
            "way_inx",
            "highway",
            "surface",
            "is_flat",
        )

        return tagged

    @staticmethod
    def calculate_elevation_changes(edges: DataFrame) -> DataFrame:
        """For each step along each edge in the enriched edges dataset,
        calculate the change in elevation from the previous step. Store
        elevation loss and gain separately.

        Args:
            edges: A table representing exploded edges in the OSM graph

        Returns:
            A copy of the input dataset with additional elevation_gain and
            elevation_loss fields
        """
        rolling_window = Window.partitionBy("src", "dst").orderBy("inx")

        edges = edges.withColumn(
            "last_elevation", F.lag("elevation", offset=1).over(rolling_window)
        )
        edges = edges.withColumn(
            "delta", F.col("elevation") - F.col("last_elevation")
        )

        edges = edges.withColumn(
            "elevation_gain",
            F.when(F.col("is_flat"), F.lit(0.0)).otherwise(
                F.abs(F.greatest(F.col("delta"), F.lit(0.0)))
            ),
        )

        edges = edges.withColumn(
            "elevation_loss",
            F.when(F.col("is_flat"), F.lit(0.0)).otherwise(
                F.abs(F.least(F.col("delta"), F.lit(0.0)))
            ),
        )

        edges = edges.withColumn(
            "elevation", F.when(~F.col("is_flat"), F.col("elevation"))
        )

        return edges

    @staticmethod
    def implode_edges(edges: DataFrame) -> DataFrame:
        """Calculate the sum of elevation gains & losses across all steps,
        rolling the dataset back up to one record per edge.

        Args:
            edges: A table representing exploded edges in the OSM graph

        Returns:
            An aggregated view of the input dataset, with one record per edge
        """
        edges = edges.groupBy(
            "src",
            "dst",
            "src_lat",
            "src_lon",
            "dst_lat",
            "dst_lon",
            "src_easting",
            "src_northing",
            "way_id",
            "way_inx",
            "highway",
            "surface",
        ).agg(
            F.sum(F.col("elevation_gain")).alias("elevation_gain"),
            F.sum(F.col("elevation_loss")).alias("elevation_loss"),
        )
        return edges

    @staticmethod
    def calculate_edge_distances(edges: DataFrame) -> DataFrame:
        """For each edge in the graph, calculate the distance from the start
        point to the end point in metres. A UDF must be applied to achieve
        this, as pySpark does not provide this function natively.

        Args:
            edges: A table representing edges in the OSM graph

        Returns:
            A copy of the input dataset with an additional distance column
        """

        @F.udf(returnType=DoubleType())
        def distance_udf(
            src_lat: float, src_lon: float, dst_lat: float, dst_lon: float
        ) -> float:
            """Enables the use of the geopy distance function to calculate the
            length of each edge represented by the pyspark edges dataset.

            Args:
                src_lat (float): The source latitude
                src_lon (float): The source longitude
                dst_lat (float): The destination latitude
                dst_lon (float): The destination longitude

            Returns:
                float: The distance between source and destination in metres
            """
            dist = distance((src_lat, src_lon), (dst_lat, dst_lon))
            dist_m = dist.meters
            return dist_m

        edges = edges.withColumn(
            "distance",
            distance_udf(
                F.col("src_lat"),
                F.col("src_lon"),
                F.col("dst_lat"),
                F.col("dst_lon"),
            ),
        )

        return edges

    @staticmethod
    def set_edge_output_schema(edges: DataFrame) -> DataFrame:
        """Bring through only the required columns for the enriched edge
        dataset

        Args:
            edges: The enriched edge dataset

        Returns:
            A subset of the input dataset
        """
        edges = edges.select(
            "src",
            "dst",
            "src_lat",
            "src_lon",
            "dst_lat",
            "dst_lon",
            "way_id",
            "way_inx",
            "highway",
            "surface",
            "distance",
            "elevation_gain",
            "elevation_loss",
            "easting_ptn",
            "northing_ptn",
        )

        return edges
