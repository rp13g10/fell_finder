"""Defines methods for the GraphEnricher class relating to the processing
of nodes in the graph"""

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession


class NodeMixin(ABC):
    """Defines the methods required to enrich node data"""

    @abstractmethod
    def __init__(self) -> None:
        """Defines the attributes required to enrich node data"""
        self.data_dir: str
        self.spark: SparkSession

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
        tagged = nodes.join(
            elevation,
            on=["easting_ptn", "northing_ptn", "easting", "northing"],
            how="inner",
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
            "id", "lat", "lon", "elevation", "easting_ptn", "northing_ptn"
        )

        return nodes
