"""Handles the joining of graph data with the corresponding elevation data"""

from glob import glob
from typing import Set, Tuple, Literal
import os
import re

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import (
    IntegerType,
    StructType,
    StructField,
)
from fell_finder.utils.partitioning import add_partitions_to_spark_df


from fell_finder.ingestion.enriching.node_mixin import NodeMixin
from fell_finder.ingestion.enriching.edge_mixin import EdgeMixin


class GraphEnricher(NodeMixin, EdgeMixin):
    """Defines an enrich method, which combines the parsed lidar, node and edge
    datasets. Generates enriched node and edge datasets, which contain
    elevation data (nodes and edges), and distance data (edges only)

    Args:
        NodeMixin (_type_): Mixin class defining required logic for nodes
        EdgeMixin (_type_): Mixin class defining required logic for edges
    """

    def __init__(self, data_dir: str, spark: SparkSession) -> None:
        """Create a GraphEnricher object, which is tied to the provided
        data directory and spark session.

        Args:
            data_dir: A folder containing parsed lidar, node and edge
              datasets
            spark: The active spark session
        """
        self.data_dir = data_dir
        self.spark = spark

        self.edge_resolution_m = 10
        self.num_ptns = 0

    def get_available_partitions(self, subfolder: str) -> Set[Tuple[int, int]]:
        """Use the filesystem to determine which partitions are available in
        the specified subfolder. This should be much faster than using a
        pyspark groupby operation.

        Args:
            subfolder: The subfolder containing the dataset to be analysed

        Raises:
            FileNotFoundError: If no partitions can be identified, an exception
              will be raised

        Returns:
            A set in which each tuple represents a single
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
                file_path: The file path to be analysed

            Raises:
                FileNotFoundError: If no partitions can be identified, an
                  exception will be raised

            Returns:
                A tuple containing the easting_ptn and northing_ptn for the
                provided file_path
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
            A set of partitions which are present in both datasets
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
            common_partitions: A list of partitions which are present in both
              datasets

        Returns:
            A dataframe with easting_ptn and northing_ptn fields containing the
            data in common_partitions
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

    @staticmethod
    def filter_df_by_common_partitions(
        data_df: DataFrame, common_ptns_df: DataFrame
    ) -> DataFrame:
        """Filter the provided dataframe to include only the partitions which
        are present in both the lidar and osm datasets.

        Args:
            data_df: The dataframe to be filtered
            common_ptns_df: A dataframe containing the partitions to be
              retained

        Returns:
            A filtered copy of data_df
        """

        df = data_df.join(
            common_ptns_df,
            on=["easting_ptn", "northing_ptn"],
            how="inner",
        )

        return df

    def load_df(
        self,
        dataset: Literal["nodes", "edges", "lidar"],
        common_ptns_df: DataFrame,
    ) -> DataFrame:
        """Load in the contents of a single dataset, filtering it to include
        only the partitions which are present in all datasets.

        Args:
            dataset: The name of the dataset to be loaded
            common_ptns_df: A dataframe containing the partitions to be
              retained

        Returns:
            The filtered contents of the specified dataset
        """
        data_dir = os.path.join(self.data_dir, "parsed", dataset)

        df = self.spark.read.parquet(data_dir)
        df = self.filter_df_by_common_partitions(df, common_ptns_df)
        df = df.repartition(self.num_ptns, "easting_ptn", "northing_ptn")

        return df

    def store_df(
        self, df: DataFrame, target: Literal["nodes", "edges", "lidar"]
    ) -> None:
        """Store an enriched dataframe to disk, partitioning it by easting_ptn
        and northing_ptn.

        Args:
            df: The dataframe to be stored
            target: The target location for the enriched dataset
        """

        # Attempt to minimise the number of files written
        df = df.repartition(self.num_ptns, "easting_ptn", "northing_ptn")

        # Write the dataframe out to disk
        df.write.partitionBy("easting_ptn", "northing_ptn").mode(
            "overwrite"
        ).parquet(os.path.join(self.data_dir, "enriched", target))

    def enrich(self) -> None:
        """Perform all of the steps required to enrich both the nodes and edges
        datasets with elevation & distance data. Store the enriched datasets
        to the 'enriched' subfolder within the specified data_dir.
        """

        common_ptns = self.get_common_partitions()
        num_ptns = len(common_ptns)

        start_shuffle_ptns = self.spark.conf.get(
            "spark.sql.shuffle.partitions", "200"
        )

        self.spark.conf.set("spark.sql.shuffle.partitions", str(num_ptns))

        common_ptns_df = self.get_common_partitions_df(common_ptns)

        # Read in elevation data
        elevation_df = self.load_df("lidar", common_ptns_df)

        # Enrich & store nodes
        nodes_df = self.load_df("nodes", common_ptns_df)
        nodes_df = self.tag_nodes(nodes_df, elevation_df)
        nodes_df = self.set_node_output_schema(nodes_df)
        self.store_df(nodes_df, target="nodes")
        del nodes_df

        # Enrich & store edges
        edges_df = self.load_df("edges", common_ptns_df)
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
            start_shuffle_ptns,  # type: ignore
        )
