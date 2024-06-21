"""Handles the joining of graph data with the corresponding elevation data"""

from abc import ABC, abstractmethod
from glob import glob
from typing import Set, Tuple, List
import os
import re

from geopy.distance import distance
from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StructType,
    StructField,
)
from pyspark.sql.window import Window
from fell_finder.utils.partitioning import add_partitions_to_spark_df


class NodeMixin(ABC):
    """Defines the methods required to enrich node data"""

    @abstractmethod
    def __init__(self):
        """Defines the attributes required to enrich node data"""
        self.data_dir: str
        self.spark: SparkSession

    def tag_nodes(self, nodes: DataFrame, elevation: DataFrame) -> DataFrame:
        """Join node and elevation tables together based on their easting and
        northing coordinates. As each node represents a single point coordinate
        this is a straightforward operation.

        Args:
            nodes (DataFrame): A table representing nodes in the OSM graph
            elevation (DataFrame): A table containing elevation data at
              different coordinates

        Returns:
            DataFrame: _description_
        """
        tagged = nodes.join(
            elevation,
            on=["easting_ptn", "northing_ptn", "easting", "northing"],
            how="inner",
        )

        return tagged

    def set_node_output_schema(self, nodes: DataFrame) -> DataFrame:
        """Bring through only the required columns for the enriched node
        dataset

        Args:
            nodes (DataFrame): The enriched node dataset

        Returns:
            DataFrame: A subset of the input dataset
        """
        nodes = nodes.select(
            "id", "lat", "lon", "elevation", "easting_ptn", "northing_ptn"
        )

        return nodes


class EdgeMixin(ABC):
    """Defines the methods required to enrich edge data"""

    @abstractmethod
    def __init__(self):
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
            edges (DataFrame): A table representing edges in the OSM graph

        Returns:
            DataFrame: A copy of the input table with an additional
              'edge_steps' column.
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

    def explode_edges(self, edges: DataFrame) -> DataFrame:
        """Starting from one record per edge in the OSM graph, explode the
        dataset out to (approximately) one record for every 10 metres along
        each edge. The 10m resolution can be changed via the
        self.edge_resolution_m parameter.

        Args:
            edges (DataFrame): A table representing edges in the OSM graph

        Returns:
            DataFrame: An exploded view of the input dataset
        """

        @F.udf(returnType=ArrayType(DoubleType()))
        def linspace_udf(start: int, end: int, n_steps: int) -> List[float]:
            """Return a list of n_steps evenly spaced points between the start
            and end coordinates.

            Args:
                start (int): The starting easting/northing
                end (int): The ending easting/northing
                n_steps (int): The number of steps to generate

            Returns:
                List[float]: A list of evenly spaced points between start and
                  end
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
                n_checkpoints (int): The number of indexes to generate

            Returns:
                List[int]: A list of indexes between 0 and n_checkpoints
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

    def unpack_exploded_edges(self, edges: DataFrame) -> DataFrame:
        """Standardize the schema of the exploded edges dataset, unpacking
        indexes, eastings and northing for each step into standard columns.

        Args:
            edges (DataFrame): A table representing exploded edges in the OSM
              graph

        Returns:
            DataFrame: A tidied up view of the input dataset
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
            "type",
            F.col("coords.inx_arr").alias("inx"),
            F.round(F.col("coords.easting_arr"))
            .astype(IntegerType())
            .alias("easting"),
            F.round(F.col("coords.northing_arr"))
            .astype(IntegerType())
            .alias("northing"),
        )
        return edges

    def tag_exploded_edges(
        self, edges: DataFrame, elevation: DataFrame
    ) -> DataFrame:
        """Join the exploded edges dataset onto the elevation table to retrieve
        the elevation at multiple points along each edge.

        Args:
            edges (DataFrame): A table representing exploded edges in the OSM
              graph
            elevation (DataFrame): A table containing elevation data

        Returns:
            DataFrame: _description_
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
            "type",
        )

        return tagged

    def calculate_elevation_changes(self, edges: DataFrame) -> DataFrame:
        """For each step along each edge in the enriched edges dataset,
        calculate the change in elevation from the previous step. Store
        elevation loss and gain separately.

        Args:
            edges (DataFrame): A table representing exploded edges in the OSM
              graph

        Returns:
            DataFrame: A copy of the input dataset with additional
              elevation_gain and elevation_loss fields
        """
        rolling_window = Window.partitionBy("src", "dst").orderBy("inx")

        edges = edges.withColumn(
            "last_elevation", F.lag("elevation", offset=1).over(rolling_window)
        )
        edges = edges.withColumn(
            "delta", F.col("elevation") - F.col("last_elevation")
        )

        edges = edges.withColumn(
            "elevation_gain", F.greatest(F.col("delta"), F.lit(0))
        )

        edges = edges.withColumn(
            "elevation_loss", F.abs(F.least(F.col("delta"), F.lit(0)))
        )

        return edges

    def implode_edges(self, edges: DataFrame) -> DataFrame:
        """Calculate the sum of elevation gains & losses across all steps,
        rolling the dataset back up to one record per edge.

        Args:
            edges (DataFrame): A table representing exploded edges in the OSM
              graph

        Returns:
            DataFrame: An aggregated view of the input dataset, with one record
              per edge
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
            "type",
        ).agg(
            F.sum(F.col("elevation_gain")).alias("elevation_gain"),
            F.sum(F.col("elevation_loss")).alias("elevation_loss"),
        )
        return edges

    def calculate_edge_distances(self, edges: DataFrame) -> DataFrame:
        """For each edge in the graph, calculate the distance from the start
        point to the end point in metres. A UDF must be applied to achieve
        this, as pySpark does not provide this function natively.

        Args:
            edges (DataFrame): A table representing edges in the OSM graph

        Returns:
            DataFrame: A copy of the input dataset with an additional distance
              column
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

    def set_edge_output_schema(self, edges: DataFrame) -> DataFrame:
        """Bring through only the required columns for the enriched edge
        dataset

        Args:
            edges (DataFrame): The enriched edge dataset

        Returns:
            DataFrame: A subset of the input dataset
        """
        edges = edges.select(
            "src",
            "dst",
            "src_lat",
            "src_lon",
            "dst_lat",
            "dst_lon",
            "type",
            "distance",
            "elevation_gain",
            "elevation_loss",
            "easting_ptn",
            "northing_ptn",
        )

        return edges


class GraphEnricher(NodeMixin, EdgeMixin):
    """Defines an enrich method, which combines the parsed lidar, node and edge
    datasets. Generates enriched node and edge datasets, which contain
    elevation data (nodes and edges), and distance data (edges only)

    Args:
        NodeMixin (_type_): Mixin class defining required logic for nodes
        EdgeMixin (_type_): Mixin class defining required logic for edges
    """

    def __init__(self, data_dir: str, spark: SparkSession):
        """Create a GraphEnricher object, which is tied to the provided
        data directory and spark session.

        Args:
            data_dir (str): A folder containing parsed lidar, node and edge
              datasets
            spark (SparkSession): The active spark session
        """
        self.data_dir = data_dir
        self.spark = spark

        self.edge_resolution_m = 10

        common_ptns = self.get_common_partitions()
        self.num_ptns = len(common_ptns)

        self.common_ptns_df = self.get_common_partitions_df(common_ptns)

        self.start_shuffle_ptns = self.spark.conf.get(
            "spark.sql.shuffle.partitions", "200"
        )

        self.spark.conf.set("spark.sql.shuffle.partitions", str(self.num_ptns))

    def get_available_partitions(self, subfolder: str) -> Set[Tuple[int, int]]:
        """Use the filesystem to determine which partitions are available in
        the specified subfolder. This should be much faster than using a
        pyspark groupby operation.

        Args:
            subfolder (str): The subfolder containing the dataset to be
              analysed

        Raises:
            FileNotFoundError: If no partitions can be identified, an exception
              will be raised

        Returns:
            Set[Tuple[int, int]]: A set in which each tuple represents a single
              easting_ptn/northing_ptn pair which is present in the data
        """

        # Fetch a list of all parquet files in the dataset
        all_files = glob(
            os.path.join(self.data_dir, subfolder, "**", "*.parquet"),
            recursive=True,
        )

        def _get_easting_northing(file_path: str) -> Tuple[int, int]:
            """Helper function which uses regular expressions to fetch the
            easting and northing partition for the provided file path and
            return them as a tuple.

            Args:
                file_path (str): The file path to be analysed

            Raises:
                FileNotFoundError: If no partitions can be identified, an
                  exception will be raised

            Returns:
                Tuple[int, int]: A tuple containing the easting_ptn and
                  northing_ptn for the provided file_path
            """
            match_ = re.search(
                r".*/easting_ptn=(\d+)/northing_ptn=(\d+)/.*", file_path
            )
            if match_ is None:
                raise FileNotFoundError(
                    f"Unable to identify a partition for {file_path}"
                )
            easting = int(match_.group(1))
            northing = int(match_.group(2))
            return easting, northing

        all_partitions = {_get_easting_northing(file_) for file_ in all_files}

        return all_partitions

    def get_common_partitions(self) -> Set[Tuple[int, int]]:
        """Determine which partitions are present in both the lidar and OSM
        datasets. This can be used to filter both input datasets before
        attempting any expensive join operations.

        Returns:
            Set[Tuple[int, int]]: A set of partitions which are present in both
              datasets
        """
        elevation_ptns = self.get_available_partitions("parsed/lidar")
        graph_ptns = self.get_available_partitions("parsed/nodes")

        common_ptns = elevation_ptns.intersection(graph_ptns)

        return common_ptns

    def get_common_partitions_df(
        self, common_partitions: Set[Tuple[int, int]]
    ) -> DataFrame:
        """Get a small dataframe containing the partitions which are present
        in both the lidar and OSM dataset. Mark it as suitable for broadcast
        joins.

        Args:
            common_partitions (Set[Tuple[int, int]]): A list of partitions
              which are present in both datasets

        Returns:
            DataFrame: A dataframe with easting_ptn and northing_ptn fields
              containing the data in common_partitions
        """

        schema = StructType(
            [
                StructField("easting_ptn", IntegerType()),
                StructField("northing_ptn", IntegerType()),
            ]
        )

        df = self.spark.createDataFrame(data=common_partitions, schema=schema)

        df = F.broadcast(df)

        return df

    def filter_df_by_common_partitions(self, df: DataFrame) -> DataFrame:
        """Filter the provided dataframe to include only the partitions which
        are present in both the lidar and osm datasets.

        Args:
            df (DataFrame): The dataframe to be filtered

        Returns:
            DataFrame: A filtered copy of the input dataset
        """

        df = df.join(
            self.common_ptns_df,
            on=["easting_ptn", "northing_ptn"],
            how="inner",
        )

        return df

    def load_df(self, dataset: str) -> DataFrame:
        """Load in the contents of a single dataset, filtering it to include
        only the partitions which are present in all datasets.

        Args:
            dataset (str): The dataset to be loaded

        Returns:
            DataFrame: The filtered contents of the specified dataset
        """
        data_dir = os.path.join(self.data_dir, "parsed", dataset)

        df = self.spark.read.parquet(data_dir)
        df = self.filter_df_by_common_partitions(df)
        df = df.repartition(self.num_ptns, "easting_ptn", "northing_ptn")

        return df

    def store_df(self, df: DataFrame, target: str):
        """Store an enriched dataframe to disk, partitioning it by easting_ptn
        and northing_ptn.

        Args:
            df (DataFrame): The dataframe to be stored
            target (str): The target location for the enriched dataset
        """

        # Attempt to minimise the number of files written
        df = df.repartition(self.num_ptns, "easting_ptn", "northing_ptn")

        # Write the dataframe out to disk
        df.write.partitionBy("easting_ptn", "northing_ptn").mode(
            "overwrite"
        ).parquet(os.path.join(self.data_dir, "enriched", target))

    def enrich(self):
        """Perform all of the steps required to enrich both the nodes and edges
        datasets with elevation & distance data. Store the enriched datasets
        to the 'enriched' subfolder within the specified data_dir.
        """

        # Read in elevation data
        elevation_df = self.load_df("lidar")

        # Enrich & store nodes
        nodes_df = self.load_df("nodes")
        nodes_df = self.tag_nodes(nodes_df, elevation_df)
        nodes_df = self.set_node_output_schema(nodes_df)
        self.store_df(nodes_df, target="nodes")
        del nodes_df

        # Enrich & store edges
        edges_df = self.load_df("edges")
        edges_df = self.calculate_step_metrics(edges_df)
        edges_df = self.explode_edges(edges_df)
        edges_df = self.unpack_exploded_edges(edges_df)
        edges_df = add_partitions_to_spark_df(edges_df)
        edges_df = self.tag_exploded_edges(edges_df, elevation_df)
        edges_df = self.calculate_elevation_changes(edges_df)
        edges_df = self.implode_edges(edges_df)
        edges_df = add_partitions_to_spark_df(
            edges_df, easting_col="src_easting", northing_col="src_northing"
        )
        edges_df = self.calculate_edge_distances(edges_df)
        edges_df = self.set_edge_output_schema(edges_df)

        self.store_df(edges_df, target="edges")
        del edges_df

        # Restore the original settings of the spark session
        self.spark.conf.set(
            "spark.sql.shuffle.partitions",
            self.start_shuffle_ptns,  # type: ignore
        )
