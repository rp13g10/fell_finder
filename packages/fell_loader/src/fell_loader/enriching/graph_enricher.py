"""Handles the joining of graph data with the corresponding elevation data"""

import os
from typing import Literal

from pyspark.sql import DataFrame, SparkSession


from fell_loader.enriching.edge_mixin import EdgeMixin
from fell_loader.enriching.node_mixin import NodeMixin


class GraphEnricher(NodeMixin, EdgeMixin):
    """Defines an enrich method, which combines the parsed lidar, node and edge
    datasets. Generates enriched node and edge datasets, which contain
    elevation data (nodes and edges), and distance data (edges only)

    Args:
        NodeMixin: Mixin class defining required logic for nodes
        EdgeMixin: Mixin class defining required logic for edges

    """

    def __init__(self, spark: SparkSession) -> None:
        """Create a GraphEnricher object, which is tied to the provided
        data directory and spark session.

        Args:
            data_dir: A folder containing parsed lidar, node and edge
              datasets
            spark: The active spark session

        """
        self.data_dir = os.environ["FF_DATA_DIR"]
        self.spark = spark

        self.edge_resolution_m = int(os.environ["FF_EDGE_RESOLUTION"])

    def load_df(
        self,
        dataset: Literal["nodes", "edges", "lidar"],
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

        # Write the dataframe out to disk
        df.write.mode("overwrite").parquet(
            os.path.join(self.data_dir, "enriched", target)
        )

    def enrich(self) -> None:
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
        edges_df = self.tag_exploded_edges(edges_df, elevation_df)
        edges_df = self.calculate_elevation_changes(edges_df)
        edges_df = self.implode_edges(edges_df)
        edges_df = self.calculate_edge_distances(edges_df)
        edges_df = self.set_edge_output_schema(edges_df)

        self.store_df(edges_df, target="edges")
        del edges_df
