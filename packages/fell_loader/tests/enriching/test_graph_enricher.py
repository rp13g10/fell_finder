"""Tests for the user-facing graph enricher class"""

import inspect
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType
from pyspark.testing import assertDataFrameEqual

from fell_loader.enriching.graph_enricher import GraphEnricher


class MockGraphEnricher(GraphEnricher):
    """Mock implementation of GraphEnricher with static params"""

    def __init__(self) -> None:
        self.data_dir = "data_dir"
        self.edge_resolution_m = 10
        self.spark: MagicMock = MagicMock()


def test_load_df():
    """Make sure data is being read in and repartitioned correctly"""

    # Arrange #################################################################

    test_enricher = MockGraphEnricher()

    test_dataset = "nodes"

    test_df = MagicMock()
    test_enricher.spark.read.parquet.return_value = test_df

    target_read_call = "data_dir/parsed/nodes"

    # Act #####################################################################
    result = test_enricher.load_df(test_dataset)

    # Assert ##################################################################
    test_enricher.spark.read.parquet.assert_called_once_with(target_read_call)
    assert result is test_enricher.spark.read.parquet.return_value


def test_store_df():
    """Make sure data is being stored to disk correctly"""

    # Arrange #################################################################

    test_enricher = MockGraphEnricher()

    test_df = MagicMock()
    test_target = "nodes"

    target_parquet_call = "data_dir/enriched/nodes"

    # Act #####################################################################

    test_enricher.store_df(test_df, test_target)

    # Assert ##################################################################
    test_df.write.mode.return_value.parquet.assert_called_once_with(
        target_parquet_call
    )


@patch("fell_loader.enriching.graph_enricher.add_partitions_to_spark_df")
def test_enrich(mock_add_partitions_to_spark_df: MagicMock):
    """Make sure the correct functions are being called with the correct
    arguments"""

    # Arrange #################################################################

    # Inputs ------------------------------------------------------------------

    test_enricher = MockGraphEnricher()

    # Set up mocks which return traceable 'dataframes'
    mock_nodes_df = MagicMock()
    mock_edges_df = MagicMock()
    mock_lidar_df = MagicMock()

    def load_side_effect(dataset: str) -> MagicMock:
        match dataset:
            case "nodes":
                return mock_nodes_df
            case "edges":
                return mock_edges_df
            case "lidar":
                return mock_lidar_df
        raise ValueError("Something went wrong!")

    test_enricher.load_df = MagicMock(side_effect=load_side_effect)

    # Set transform methods to return input 'dataframe' to retain traceability
    # Nodes
    test_enricher.tag_nodes = MagicMock(side_effect=lambda x, _: x)
    test_enricher.set_node_output_schema = MagicMock(side_effect=lambda x: x)

    # Edges
    test_enricher.calculate_step_metrics = MagicMock(side_effect=lambda x: x)
    test_enricher.explode_edges = MagicMock(side_effect=lambda x: x)
    test_enricher.unpack_exploded_edges = MagicMock(side_effect=lambda x: x)
    test_enricher.tag_exploded_edges = MagicMock(side_effect=lambda x, _: x)
    test_enricher.calculate_elevation_changes = MagicMock(
        side_effect=lambda x: x
    )
    test_enricher.implode_edges = MagicMock(side_effect=lambda x: x)
    test_enricher.calculate_edge_distances = MagicMock(side_effect=lambda x: x)
    test_enricher.set_edge_output_schema = MagicMock(side_effect=lambda x: x)

    # Keep track of what's being stored and where
    test_enricher.store_df = MagicMock()

    # Keep track of partitions being added
    def add_partitions_side_effect(df: Any, *args, **kwargs) -> Any:
        return df

    mock_add_partitions_to_spark_df.side_effect = add_partitions_side_effect

    # Targets -----------------------------------------------------------------

    target_methods = inspect.getmembers(
        GraphEnricher, predicate=inspect.iscoroutine
    )

    # Act #####################################################################

    test_enricher.enrich()

    # Assert ##################################################################

    # Every function defined is being called, and at full coverage we know
    # they have all been tested individually. Tracing exactly which df is
    # passed to each method has not been done at present, as it requires
    # significant effort for questionable gain
    for method, _ in target_methods:
        getattr(test_enricher, method).assert_has_calls()
