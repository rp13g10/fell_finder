"""Tests for `fell_loader.staging.nodes"""

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from fell_loader.staging.nodes import NodeStager
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.testing import assertDataFrameEqual


class MockNodeStager(NodeStager):
    """Mock implementation of the node loader class, uses static values instead
    of fetching info from environment variables
    """

    def __init__(self, spark: SparkSession | None = None) -> None:
        # Attrs from base
        self.data_dir = Path("data_dir")
        self.binary_loc = Path("binary_loc")
        self.skip_load = False
        self.spark: Any = spark if spark is not None else MagicMock()

        # Implementation specific
        self.removals_applied = False
        self.updates_applied = False
        self.remaining_count = -1
        self.current_chunk_size = -1


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
        res_df = NodeStager.map_to_schema(test_df, test_schema, cast=True)

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
        res_df = NodeStager.map_to_schema(test_df, test_schema, cast=False)

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

        test_loader = MockNodeStager()

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

        test_loader = MockNodeStager()

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

        test_loader = MockNodeStager()

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

        test_loader = MockNodeStager()

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

        test_loader = MockNodeStager()

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

        test_loader = MockNodeStager()

        test_layer = "landing"
        test_dataset = "nodes"

        # Act, Assert
        with pytest.raises(FileNotFoundError):
            _ = test_loader.read_delta(test_layer, test_dataset)

        # Assert
        mock_deltatable.forPath.assert_not_called()


@patch("pathlib.Path.read_text")
@patch("pathlib.Path.exists")
@patch("pathlib.Path.glob")
class TestCheckForLidarUpdates:
    """Make sure the check for updated LIDAR files has been set up properly"""

    def test_no_last_run_file(
        self,
        mock_glob: MagicMock,
        mock_exists: MagicMock,
        mock_read_text: MagicMock,
    ):
        """Check behaviour on first run (no last_run_lidar.txt file exists)"""
        # Arrange
        test_stager = MockNodeStager()

        mock_glob.return_value = [Path("file_one")]
        mock_exists.return_value = False
        mock_read_text.return_value = "['file_one']"

        # Act
        res_updated = test_stager.check_for_lidar_update()

        # Assert
        mock_read_text.assert_not_called()
        assert res_updated

    def test_no_file_change(
        self,
        mock_glob: MagicMock,
        mock_exists: MagicMock,
        mock_read_text: MagicMock,
    ):
        """If no files have changed, return False"""
        # Arrange
        test_stager = MockNodeStager()

        mock_glob.return_value = [Path("file_one")]
        mock_exists.return_value = True
        mock_read_text.return_value = "['file_one']"

        # Act
        res_updated = test_stager.check_for_lidar_update()

        # Assert
        mock_read_text.assert_called_once()
        assert not res_updated

    def test_file_change(
        self,
        mock_glob: MagicMock,
        mock_exists: MagicMock,
        mock_read_text: MagicMock,
    ):
        """If files have changed, return True"""
        # Arrange
        test_stager = MockNodeStager()

        mock_glob.return_value = [Path("file_one"), Path("file_two")]
        mock_exists.return_value = True
        mock_read_text.return_value = "['file_one']"

        # Act
        res_updated = test_stager.check_for_lidar_update()

        # Assert
        mock_read_text.assert_called_once()
        assert res_updated


@patch("fell_loader.staging.base.F")
@patch("fell_loader.staging.base.DeltaTable")
def test_clear_data_without_elevation(
    mock_deltatable: MagicMock, mock_f: MagicMock
):
    """Make sure the right files are being deleted"""
    # Arrange
    test_stager = MockNodeStager()

    mock_tbl = MagicMock()
    mock_deltatable.forPath.return_value = mock_tbl

    # Act
    test_stager.clear_data_without_elevation(dataset="nodes")

    # Assert
    mock_deltatable.forPath.assert_called_once_with(
        test_stager.spark, "data_dir/staging/nodes"
    )
    mock_tbl.delete.assert_called_once_with(
        mock_f.col.return_value.isNull.return_value
    )


@patch("fell_loader.staging.base.shutil.rmtree")
class TestClearTempFiles:
    """Make sure temp files are being cleaned up properly"""

    def test_files_found(self, mock_rmtree: MagicMock):
        """If temp files are present, they should be removed"""
        # Arrange
        test_loader = MockNodeStager()

        # Act
        test_loader.clear_temp_files("node_updates")

        # Assert
        mock_rmtree.assert_called_once_with("data_dir/temp/node_updates")

    def test_files_not_found(self, mock_rmtree: MagicMock):
        """If no temp files are present, the raised exception should be
        suppressed
        """
        # Arrange
        test_loader = MockNodeStager()
        mock_rmtree.side_effect = FileNotFoundError

        # Act
        test_loader.clear_temp_files("node_updates")

    def test_suppression_setup(self, mock_rmtree: MagicMock):
        """Make sure that suppression is definitely working by demonstrating
        that other exception types do get raised. Without this, the above
        test is not sufficient to show the expected behaviour when files are
        not found. In isolation it doesn't prove that the side effect has been
        set up properly.
        """
        # Arrange
        test_loader = MockNodeStager()
        mock_rmtree.side_effect = ValueError

        # Act, Assert
        with pytest.raises(ValueError):
            test_loader.clear_temp_files("node_updates")


# MARK: Implementation Specific


@patch("fell_loader.staging.nodes.stg")
@patch("fell_loader.staging.nodes.DeltaTable")
def test_initialize_output_table(
    mock_deltatable: MagicMock, mock_stg: MagicMock
):
    """Check that the correct calls are being generated"""
    # Arrange
    test_stager = MockNodeStager()

    mock_stg.NODES_SCHEMA.fields = "fields"

    mock_deltatable.createIfNotExists.return_value = mock_deltatable
    mock_deltatable.location.return_value = mock_deltatable
    mock_deltatable.addColumns.return_value = mock_deltatable

    # Act
    test_stager.initialize_output_table()

    # Assert
    mock_deltatable.createIfNotExists.assert_called_once_with(
        test_stager.spark
    )
    mock_deltatable.location.assert_called_once_with("data_dir/staging/nodes")
    mock_deltatable.addColumns.assert_called_once_with("fields")
    mock_deltatable.execute.assert_called_once()


class TestGetNodesToRemove:
    """Check that the correct nodes are being flagged for removal"""

    def test_removals_applied(self):
        """If removals have already been applied, no action should be taken"""
        # Arrange
        test_stager = MockNodeStager()
        test_stager.removals_applied = True

        # Act
        result = test_stager.get_nodes_to_remove()

        # Assert
        assert result is None

    def test_removals_not_applied(self, test_session: SparkSession):
        """If removals have not yet been applied, the appropriate records
        should be returned
        """
        # Arrange #############################################################

        # ----- Mocking -----
        test_stager = MockNodeStager()
        test_stager.read_parquet = MagicMock()
        test_stager.read_delta = MagicMock()

        # ----- New Nodes -----
        test_new_data = [[0], [1], [2], [3]]
        test_new_schema = StructType([StructField("id", IntegerType())])
        test_new_df = test_session.createDataFrame(
            test_new_data, test_new_schema
        )
        test_stager.read_parquet.return_value = test_new_df

        # ----- Existing Nodes -----
        test_ex_data = [[2], [3], [4], [5]]
        test_ex_schema = StructType([StructField("id", IntegerType())])
        test_ex_df = test_session.createDataFrame(test_ex_data, test_ex_schema)
        test_stager.read_delta.return_value = test_ex_df

        # ----- Target -----
        tgt_data = [[4], [5]]
        tgt_schema = StructType([StructField("id", IntegerType())])
        tgt_df = test_session.createDataFrame(tgt_data, tgt_schema)

        # Act #################################################################
        res_df = test_stager.get_nodes_to_remove()

        # Assert ##############################################################
        assert res_df is not None
        assertDataFrameEqual(res_df, tgt_df)


class TestGetNodesToUpdate:
    """Check that the correct nodes are being pulled through for updates"""

    def test_records_returned(self, test_session: SparkSession):
        """Check that the right records are being fetched"""
        # Arrange #############################################################

        # ----- Mocking -----
        test_stager = MockNodeStager()
        test_stager.read_parquet = MagicMock()
        test_stager.read_delta = MagicMock()

        # ----- New Nodes -----
        # fmt: off
        #    id, timestamp, easting, northing
        test_new_data = [
            # old only
            # new only
            [1 , 123456   , 50000  , 100000],
            # both, same timestamp
            [2 , 123456   , 50000  , 100000],
            # both, updated timestamp
            [3 , 234567   , 50000  , 100000],
        ]
        # fmt: on
        test_new_schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("timestamp", LongType()),
                StructField("easting", IntegerType()),
                StructField("northing", IntegerType()),
            ]
        )
        test_new_df = test_session.createDataFrame(
            test_new_data, test_new_schema
        )
        test_stager.read_parquet.return_value = test_new_df

        # ----- Existing Nodes -----
        # fmt: off
        #    id, timestamp
        test_ex_data = [
            # old only
            [0 , 123456],
            # new only
            # both, same timestamp
            [2 , 123456],
            # both, updated timestamp
            [3 , 123456],
        ]
        # fmt: on
        test_ex_schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("timestamp", LongType()),
            ]
        )
        test_ex_df = test_session.createDataFrame(test_ex_data, test_ex_schema)
        test_stager.read_delta.return_value = test_ex_df

        # ----- Target -----
        # fmt: off
        #    id, timestamp, easting, northing
        tgt_data = [
            # old only
            # new only
            [1 , 123456   , 50000  , 100000],
            # both, same timestamp
            # both, updated timestamp
            [3 , 234567   , 50000  , 100000],
        ]
        # fmt: on
        tgt_schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("timestamp", LongType()),
                StructField("easting", IntegerType()),
                StructField("northing", IntegerType()),
            ]
        )
        tgt_df = test_session.createDataFrame(tgt_data, tgt_schema)

        # Act #################################################################
        res_df = test_stager.get_nodes_to_update()

        # Assert ##############################################################
        assertDataFrameEqual(res_df, tgt_df)
        assert test_stager.remaining_count == 2
        assert not test_stager.updates_applied

    def test_no_records_returned(self, test_session: SparkSession):
        """If there are no records left to updated, return an empty dataframe
        and set the updates_applied flag to True
        """
        # Arrange #############################################################

        # ----- Mocking -----
        test_stager = MockNodeStager()
        test_stager.read_parquet = MagicMock()
        test_stager.read_delta = MagicMock()

        # ----- New Nodes -----
        # fmt: off
        #    id, timestamp, easting, northing
        test_new_data = [
            # old only
            # both, same timestamp
            [2 , 123456   , 50000  , 100000],
        ]
        # fmt: on
        test_new_schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("timestamp", LongType()),
                StructField("easting", IntegerType()),
                StructField("northing", IntegerType()),
            ]
        )
        test_new_df = test_session.createDataFrame(
            test_new_data, test_new_schema
        )
        test_stager.read_parquet.return_value = test_new_df

        # ----- Existing Nodes -----
        # fmt: off
        #    id, timestamp
        test_ex_data = [
            # old only
            [0 , 123456],
            # both, same timestamp
            [2 , 123456],
        ]
        # fmt: on
        test_ex_schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("timestamp", LongType()),
            ]
        )
        test_ex_df = test_session.createDataFrame(test_ex_data, test_ex_schema)
        test_stager.read_delta.return_value = test_ex_df

        # ----- Target -----
        # fmt: off
        #    id, timestamp, easting, northing
        tgt_data = [
            # old only
            # both, same timestamp
        ]
        # fmt: on
        tgt_schema = StructType(
            [
                StructField("id", IntegerType()),
                StructField("timestamp", LongType()),
                StructField("easting", IntegerType()),
                StructField("northing", IntegerType()),
            ]
        )
        tgt_df = test_session.createDataFrame(tgt_data, tgt_schema)

        # Act #################################################################
        res_df = test_stager.get_nodes_to_update()

        # Assert ##############################################################
        assertDataFrameEqual(res_df, tgt_df)
        assert test_stager.remaining_count == 0
        assert test_stager.updates_applied


@patch("fell_loader.staging.nodes.lnd")
def test_read_elevation(mock_lnd: MagicMock, test_session: SparkSession):
    """Make sure the right elevation data is being read in based on the
    lidar bounds dataset
    """
    # Arrange #################################################################

    # ----- Mocking -----
    mock_spark = MagicMock()
    test_stager = MockNodeStager(spark=mock_spark)
    test_stager.read_parquet = MagicMock()

    mock_lnd.LIDAR_SCHEMA = "LIDAR_SCHEMA"

    # ----- Nodes -----

    # fmt: off
    #    easting, northing, id
    test_nodes_data = [
        # both outside
        [0      , 13      , 1],
        # easting inside, northing outside
        [7      , 13      , 2],
        # easting outside, northing inside
        [0      , 9       , 3],
        # both inside (1)
        [7      , 9       , 4],
        # both inside (2)
        [17     , 19      , 5]
    ]
    # fmt: on

    test_nodes_schema = StructType(
        [
            StructField("easting", IntegerType()),
            StructField("northing", IntegerType()),
            StructField("id", IntegerType()),
        ]
    )
    test_nodes_df = test_session.createDataFrame(
        test_nodes_data, test_nodes_schema
    )
    # ----- Lidar Bounds -----
    # fmt: off
    #    file_id     , min_easting, max_easting, min_northing, max_northing
    test_bounds_data = [
        ['file_one'  , 5          , 10         , 5           , 10],
        ['file_two'  , 15         , 20         , 15          , 20],
        ['file_three', 25         , 30         , 25          , 30],
    ]
    # fmt: on
    test_bounds_schema = StructType(
        [
            StructField("file_id", StringType()),
            StructField("min_easting", IntegerType()),
            StructField("max_easting", IntegerType()),
            StructField("min_northing", IntegerType()),
            StructField("max_northing", IntegerType()),
        ]
    )
    test_bounds_df = test_session.createDataFrame(
        test_bounds_data, test_bounds_schema
    )
    test_stager.read_parquet.return_value = test_bounds_df

    # ----- Target -----
    tgt_paths = [
        "data_dir/landing/lidar/file_one.parquet",
        "data_dir/landing/lidar/file_two.parquet",
    ]

    # Act #####################################################################

    result = test_stager.read_elevation(test_nodes_df)

    # Assert ##################################################################

    test_stager.spark.read.schema.assert_called_once_with("LIDAR_SCHEMA")
    assert sorted(
        test_stager.spark.read.schema.return_value.parquet.call_args.args
    ) == sorted(tgt_paths)
    assert (
        result
        is test_stager.spark.read.schema.return_value.parquet.return_value
    )


def test_tag_nodes(test_session: SparkSession):
    """Make sure the join operation has been set up properly"""
    # Arrange #################################################################

    # ----- Nodes -----

    # fmt: off
    #    easting, northing, nodes
    test_nodes_data = [
        # Nodes only
        [1      , 11      , 'nodes'],
        # Elevation only
        # Both
        [3      , 13      , 'nodes']
    ]
    # fmt: on
    test_nodes_schema = StructType(
        [
            StructField("easting", IntegerType()),
            StructField("northing", IntegerType()),
            StructField("nodes", StringType()),
        ]
    )
    test_nodes_df = test_session.createDataFrame(
        test_nodes_data, test_nodes_schema
    )

    # ----- Elevation -----

    # fmt: off
    #    easting, northing, elevation
    test_ele_data = [
        # Nodes only
        # Elevation only
        [2      , 12      , 'elevation'],
        # Both
        [3      , 13      , 'elevation']
    ]
    # fmt: on
    test_ele_schema = StructType(
        [
            StructField("easting", IntegerType()),
            StructField("northing", IntegerType()),
            StructField("elevation", StringType()),
        ]
    )
    test_ele_df = test_session.createDataFrame(test_ele_data, test_ele_schema)

    # ----- Target -----

    # fmt: off
    #    easting, northing, nodes  , elevation
    tgt_data = [
        # Nodes only
        [1      , 11      , 'nodes', None],
        # Elevation only
        # Both
        [3      , 13      , 'nodes', 'elevation']
    ]
    # fmt: on
    tgt_schema = StructType(
        [
            StructField("easting", IntegerType()),
            StructField("northing", IntegerType()),
            StructField("nodes", StringType()),
            StructField("elevation", StringType()),
        ]
    )
    tgt_df = test_session.createDataFrame(tgt_data, tgt_schema)

    # Act #####################################################################
    res_df = NodeStager.tag_nodes(test_nodes_df, test_ele_df)

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


@pytest.mark.skip("Minimal value, all operations must be mocked")
class TestApplyNodeUpdates:
    """Make sure the nodes output dataset is being updated properly"""

    def test_with_removals(self):
        """Check behaviour when no removals need to be applied"""
        raise AssertionError()

    def test_without_removals(self):
        """Check behaviour when removals do need to be applied"""


@patch("fell_loader.staging.nodes.DeltaTable")
def test_optimise_nodes_table(mock_deltatable: MagicMock):
    """Make sure the right calls are being generated to optimise the table"""
    # Arrange
    test_stager = MockNodeStager()

    mock_tbl = MagicMock()
    mock_deltatable.forPath.return_value = mock_tbl

    # Act
    test_stager.optimise_nodes_table()

    # Asser
    mock_deltatable.forPath.assert_called_once_with(
        test_stager.spark, "data_dir/staging/nodes"
    )
    mock_tbl.optimize.return_value.executeCompaction.assert_called_once()
    mock_tbl.vacuum.assert_called_once()


@pytest.mark.skip("High effort, low value")
def test_run():
    """Check that all expected function calls are generated"""
