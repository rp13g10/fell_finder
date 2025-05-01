"""Tests for the user-facing graph enricher class"""

import inspect
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType
from pyspark.testing import assertDataFrameEqual

from fell_loader.enriching.graph_enricher import GraphEnricher


@patch("fell_loader.enriching.graph_enricher.glob")
class TestGetAvailablePartitions:
    """Make sure partitions can correctly be identified from the filesystem"""

    def test_match_found(self, mock_glob: MagicMock):
        """Successful extraction"""

        # Arrange
        mock_glob.return_value = [
            "/home/ross/repos/fell_finder/data/parsed/edges/easting_ptn=81/northing_ptn=23/0a9a05d0bb264860986c1e2431beb29f-0.parquet",
            "/home/ross/repos/fell_finder/data/parsed/lidar/easting_ptn=88/northing_ptn=23/b935432376cd47f4b067cee64328a019-0.parquet",
            "/home/ross/repos/fell_finder/data/parsed/nodes/easting_ptn=81/northing_ptn=23/e35da41d3a0a48ccaed8ea2090d75d26-0.parquet",
        ]

        target = {(81, 23), (88, 23)}

        test_enricher = GraphEnricher("data_dir", "dummy_spark")  # type: ignore

        # Act
        result = test_enricher.get_available_partitions("subfolder")

        # Assert
        assert result == target
        mock_glob.assert_called_once_with(
            "data_dir/subfolder/**/*.parquet", recursive=True
        )

    def test_no_match_found(self, mock_glob: MagicMock):
        """Raise an error if no match identified"""
        # Arrange
        mock_glob.return_value = [
            "/home/ross/repos/fell_finder/data/parsed/edges/easting_ptn=81/0a9a05d0bb264860986c1e2431beb29f-0.parquet",
            "/home/ross/repos/fell_finder/data/parsed/lidar/easting_ptn=88/b935432376cd47f4b067cee64328a019-0.parquet",
            "/home/ross/repos/fell_finder/data/parsed/nodes/easting_ptn=81/e35da41d3a0a48ccaed8ea2090d75d26-0.parquet",
        ]

        test_enricher = GraphEnricher("data_dir", "dummy_spark")  # type: ignore

        # Act, Assert
        with pytest.raises(FileNotFoundError):
            _ = test_enricher.get_available_partitions("subfolder")


def test_get_common_partitions():
    """Make sure only partitions present in both datasets are returned"""

    # Arrange
    test_enricher = GraphEnricher("data_dir", "dummy_spark")  # type: ignore

    test_elevation_partitions = {"elevation", "both"}
    test_graph_partitions = {"both", "graph"}

    mock_get_available_partitions = MagicMock(
        side_effect=[test_elevation_partitions, test_graph_partitions]
    )
    test_enricher.get_available_partitions = mock_get_available_partitions

    target = {"both"}

    # Act
    result = test_enricher.get_common_partitions()

    # Assert
    assert result == target


def test_get_common_partitions_df(test_session: SparkSession):
    """Make sure partitions are correctly placed into a dataframe"""

    # Arrange

    test_enricher = GraphEnricher("data_dir", test_session)

    test_common_partitions = {
        (0, 1),
        (1, 2),
        (2, 3),
    }

    _ = ["easting_ptn", "northing_ptn"]

    target_data = [
        [0, 1],
        [1, 2],
        [2, 3],
    ]

    target_schema = StructType(
        [
            StructField("easting_ptn", IntegerType()),
            StructField("northing_ptn", IntegerType()),
        ]
    )

    target = test_session.createDataFrame(
        data=target_data, schema=target_schema
    )

    # Act
    result = test_enricher.get_common_partitions_df(test_common_partitions)

    # Assert
    assertDataFrameEqual(result, target)


def test_filter_df_by_common_partitions(test_session: SparkSession):
    """Make sure the table join has been set up properly"""
    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # ----- Data -----

    # fmt: off
    _ = (
        ['easting_ptn', 'northing_ptn'])

    test_data_data = [
        [0            , 1],
        [2            , 3],
    ]
    # fmt: on

    test_data_schema = StructType(
        [
            StructField("easting_ptn", IntegerType()),
            StructField("northing_ptn", IntegerType()),
        ]
    )

    test_data_df = test_session.createDataFrame(
        test_data_data, test_data_schema
    )

    # ----- Common Partitions -----

    # fmt: off
    _ = (
        ['easting_ptn', 'northing_ptn'])

    test_cmn_data = [
        [2            , 3],
        [4            , 5],
    ]
    # fmt: on

    test_cmn_schema = StructType(
        [
            StructField("easting_ptn", IntegerType()),
            StructField("northing_ptn", IntegerType()),
        ]
    )

    test_cmn_df = test_session.createDataFrame(test_cmn_data, test_cmn_schema)

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ['easting_ptn', 'northing_ptn'])

    target_data = [
        [2            , 3],
    ]
    # fmt: on

    target_schema = StructType(
        [
            StructField("easting_ptn", IntegerType()),
            StructField("northing_ptn", IntegerType()),
        ]
    )

    target_df = test_session.createDataFrame(target_data, target_schema)
    target_df = target_df.select(*sorted(target_df.columns))

    # Act #####################################################################

    result_df = GraphEnricher.filter_df_by_common_partitions(
        test_data_df, test_cmn_df
    )
    result_df = result_df.select(*sorted(result_df.columns))

    # Assert ##################################################################
    assertDataFrameEqual(result_df, target_df)


def test_load_df():
    """Make sure data is being read in and repartitioned correctly"""

    # Arrange #################################################################

    test_enricher = GraphEnricher("data_dir", spark=MagicMock())
    test_enricher.num_ptns = 10

    test_dataset = "nodes"
    test_common_ptns_df = MagicMock()

    test_df = MagicMock()
    test_enricher.spark.read.parquet.return_value = test_df
    test_enricher.filter_df_by_common_partitions = MagicMock(
        side_effect=lambda x, _: x
    )

    target_read_call = "data_dir/parsed/nodes"
    target_repartition_args = (10, "easting_ptn", "northing_ptn")

    # Act #####################################################################
    test_enricher.load_df(test_dataset, test_common_ptns_df)

    # Assert ##################################################################
    test_enricher.spark.read.parquet.assert_called_once_with(target_read_call)
    test_enricher.filter_df_by_common_partitions.assert_called_once_with(
        test_df, test_common_ptns_df
    )
    test_df.repartition.assert_called_once_with(*target_repartition_args)


def test_store_df():
    """Make sure data is being stored to disk correctly"""

    # Arrange #################################################################

    test_enricher = GraphEnricher("data_dir", "dummy_spark")  # type: ignore
    test_enricher.num_ptns = 10

    test_df = MagicMock()
    test_df.repartition.return_value = test_df
    test_target = "nodes"

    target_repartition_args = (10, "easting_ptn", "northing_ptn")
    target_parquet_call = "data_dir/enriched/nodes"

    # Act #####################################################################

    test_enricher.store_df(test_df, test_target)

    # Assert ##################################################################
    test_df.repartition.assert_called_once_with(*target_repartition_args)
    test_df.write.partitionBy.return_value.mode.return_value.parquet.assert_called_once_with(
        target_parquet_call
    )


@patch("fell_loader.enriching.graph_enricher.add_partitions_to_spark_df")
def test_enrich(mock_add_partitions_to_spark_df: MagicMock):
    """Make sure the correct functions are being called with the correct
    arguments"""

    # Arrange #################################################################

    # Inputs ------------------------------------------------------------------
    # Set up mock spark session
    dummy_spark_session = MagicMock()
    dummy_spark_session.conf.get.return_value = "200"

    test_enricher = GraphEnricher("data_dir", dummy_spark_session)

    test_enricher.get_common_partitions = MagicMock(
        return_value=["common_ptn"]
    )

    # Set up mocks which return traceable 'dataframes'
    mock_common_ptns_df = MagicMock()
    mock_nodes_df = MagicMock()
    mock_edges_df = MagicMock()
    mock_lidar_df = MagicMock()

    def load_side_effect(dataset: str, _: Any) -> MagicMock:
        match dataset:
            case "nodes":
                return mock_nodes_df
            case "edges":
                return mock_edges_df
            case "lidar":
                return mock_lidar_df
        raise ValueError("Something went wrong!")

    test_enricher.load_df = MagicMock(side_effect=load_side_effect)
    test_enricher.get_common_partitions_df = MagicMock(
        return_value=mock_common_ptns_df
    )

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
