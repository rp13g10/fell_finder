"""Tests for fell_loader.sanitised.nodes"""

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from fell_loader.sanitised.nodes import NodeSanitiser
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.testing import assertDataFrameEqual


class MockNodeSanitiser(NodeSanitiser):
    """Mock implementation of the node sanitiser class, uses static values
    of fetching info from environment variables
    """

    def __init__(self, spark: SparkSession | None = None) -> None:
        # Attrs from base
        self.data_dir = Path("data_dir")
        self.skip_load = False
        self.spark: Any = spark if spark is not None else MagicMock()


# MARK: From Base


class TestMapToSchema:
    """Make sure schema mapping works properly"""

    def test_with_cast(self, test_session: SparkSession):
        """Check behaviour when casting is enabled"""
        # Arrange
        test_df_schema = StructType(
            [
                StructField("tgt_one", IntegerType()),
                StructField("tgt_two", StringType()),
                StructField("other", StringType()),
            ]
        )
        test_df_data = [[1, "2", "other"]]
        test_df = test_session.createDataFrame(test_df_data, test_df_schema)

        tgt_schema = StructType(
            [
                StructField("tgt_one", StringType()),
                StructField("tgt_two", IntegerType()),
            ]
        )
        tgt_data = [["1", 2]]
        tgt_df = test_session.createDataFrame(tgt_data, tgt_schema)

        test_schema = StructType(
            [
                StructField("tgt_one", StringType()),
                StructField("tgt_two", IntegerType()),
            ]
        )

        # Act
        res_df = NodeSanitiser.map_to_schema(test_df, test_schema, cast=True)

        # Assert
        assertDataFrameEqual(res_df, tgt_df)

    def test_no_cast(self, test_session: SparkSession):
        """Check behaviour when casting is not enabled"""
        # Arrange
        test_df_schema = StructType(
            [
                StructField("tgt_one", IntegerType()),
                StructField("tgt_two", StringType()),
                StructField("other", StringType()),
            ]
        )
        test_df_data = [[1, "2", "other"]]
        test_df = test_session.createDataFrame(test_df_data, test_df_schema)

        tgt_schema = StructType(
            [
                StructField("tgt_one", IntegerType()),
                StructField("tgt_two", StringType()),
            ]
        )
        tgt_data = [[1, "2"]]
        tgt_df = test_session.createDataFrame(tgt_data, tgt_schema)

        test_schema = StructType(
            [
                StructField("tgt_one", StringType()),
                StructField("tgt_two", IntegerType()),
            ]
        )

        # Act
        res_df = NodeSanitiser.map_to_schema(test_df, test_schema, cast=False)

        # Assert
        assertDataFrameEqual(res_df, tgt_df)


@patch("pathlib.Path.mkdir", autospec=True)
@patch("pathlib.Path.exists", autospec=True)
class TestWriteParquet:
    """Make sure parquet writes have been set up properly"""

    def test_target_dir_exists(
        self, mock_exists: MagicMock, mock_mkdir: MagicMock
    ):
        """If the target folder already exists, it should not be recreated"""
        # Arrange
        mock_exists.return_value = True

        test_loader = MockNodeSanitiser()

        test_df = MagicMock()
        test_layer = "landing"
        test_dataset = "nodes"

        # Act
        test_loader.write_parquet(test_df, test_layer, test_dataset)

        # Assert
        mock_mkdir.assert_not_called()
        test_df.write.parquet.assert_called_once_with(
            "data_dir/landing/nodes", mode="overwrite"
        )

    def test_target_dir_does_not_exist(
        self, mock_exists: MagicMock, mock_mkdir: MagicMock
    ):
        """If the target folder does not exist, it should be created"""
        # Arrange
        mock_exists.return_value = False

        test_loader = MockNodeSanitiser()

        test_df = MagicMock()
        test_layer = "landing"
        test_dataset = "nodes"

        # Act
        test_loader.write_parquet(test_df, test_layer, test_dataset)

        # Assert
        mock_mkdir.assert_called_once_with(
            Path("data_dir/landing/nodes"), parents=True
        )
        test_df.write.parquet.assert_called_once_with(
            "data_dir/landing/nodes", mode="overwrite"
        )


@patch("pathlib.Path.exists", autospec=True)
class TestReadParquet:
    """Make sure parquet reads have been set up properly"""

    def test_target_exists(self, mock_exists: MagicMock):
        """If the file exists, read it"""
        # Arrange
        mock_exists.return_value = True

        test_loader = MockNodeSanitiser()

        test_layer = "landing"
        test_dataset = "nodes"

        # Act
        result = test_loader.read_parquet(test_layer, test_dataset)

        # Assert
        test_loader.spark.read.parquet.assert_called_once_with(
            "data_dir/landing/nodes"
        )
        assert result is test_loader.spark.read.parquet.return_value

    def test_target_does_not_exist(self, mock_exists: MagicMock):
        """If the file does not exist, an exception should be raised"""
        # Arrange
        mock_exists.return_value = False

        test_loader = MockNodeSanitiser()

        test_layer = "landing"
        test_dataset = "nodes"

        # Act, Assert
        with pytest.raises(FileNotFoundError):
            _ = test_loader.read_parquet(test_layer, test_dataset)

        # Assert
        test_loader.spark.read.parquet.assert_not_called()


@patch("fell_loader.base.base_loader.DeltaTable")
@patch("pathlib.Path.exists", autospec=True)
class TestReadDelta:
    """Make sure delta reads have been set up properly"""

    def test_target_exists(
        self, mock_exists: MagicMock, mock_deltatable: MagicMock
    ):
        """If the file exists, read it"""
        # Arrange
        mock_exists.return_value = True

        test_loader = MockNodeSanitiser()

        test_layer = "landing"
        test_dataset = "nodes"

        # Act
        result = test_loader.read_delta(test_layer, test_dataset)

        # Assert
        mock_deltatable.forPath.assert_called_once_with(
            test_loader.spark, "data_dir/landing/nodes"
        )
        assert result is mock_deltatable.forPath.return_value.toDF.return_value

    def test_target_does_not_exist(
        self, mock_exists: MagicMock, mock_deltatable: MagicMock
    ):
        """If the file does not exist, an exception should be raised"""
        # Arrange
        mock_exists.return_value = False

        test_loader = MockNodeSanitiser()

        test_layer = "landing"
        test_dataset = "nodes"

        # Act, Assert
        with pytest.raises(FileNotFoundError):
            _ = test_loader.read_delta(test_layer, test_dataset)

        # Assert
        mock_deltatable.forPath.assert_not_called()


# MARK: Implementation Specific


def test_remove_unused_nodes(test_session: SparkSession):
    """Make sure that any nodes not present in an edge are dropped"""
    # Arrange #################################################################

    # ----- Test Data -----

    # fmt: off
    #    id, other
    test_nodes_data = [
        # nodes only
        [1 , 'other'],
        # edges only
        # both (src and dst) # noqa: ERA001
        [3 , 'other'],
        # both (src only)
        [4 , 'other'],
        # both (dst only)
        [5 , 'other']
    ]
    # fmt: on

    test_nodes_schema = StructType(
        [StructField("id", IntegerType()), StructField("other", StringType())]
    )

    test_nodes_df = test_session.createDataFrame(
        test_nodes_data, test_nodes_schema
    )

    # fmt: off
    #     src, dst, other
    test_edges_data = [
        # nodes only
        # edges only
        [2   , 6  , 'other'],
        # both (src and dst) # noqa: ERA001
        [3   , 7  , 'other'],
        [8   , 3  , 'other'],
        # both (src only),
        [4   , 9  , 'other'],
        # both (dst only)
        [10  , 5  , 'other'],
    ]
    # fmt: on

    test_edges_schema = StructType(
        [
            StructField("src", IntegerType()),
            StructField("dst", IntegerType()),
            StructField("other", StringType()),
        ]
    )

    test_edges_df = test_session.createDataFrame(
        test_edges_data, test_edges_schema
    )

    # ----- Target Data -----

    # fmt: off
    #    id, other
    tgt_data = [
        # nodes only
        # edges only
        # both (src and dst) # noqa: ERA001
        [3 , 'other'],
        # both (src only)
        [4 , 'other'],
        # both (dst only)
        [5 , 'other']
    ]
    # fmt: on

    tgt_schema = StructType(
        [StructField("id", IntegerType()), StructField("other", StringType())]
    )

    tgt_df = test_session.createDataFrame(tgt_data, tgt_schema)

    # Act #####################################################################
    res_df = NodeSanitiser.remove_unused_nodes(test_nodes_df, test_edges_df)

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


@pytest.mark.skip("High effort, low value")
def test_run():
    """Check that all expected function calls are generated"""
