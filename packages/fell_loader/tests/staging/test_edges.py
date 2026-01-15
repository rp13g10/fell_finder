"""Tests for `fell_loader.staging.edges`"""

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from fell_loader.staging.edges import EdgeStager
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.testing import assertDataFrameEqual


class MockEdgeStager(EdgeStager):
    """Mock implementation of the edge loader class, uses static values instead
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
        res_df = EdgeStager.map_to_schema(test_df, test_schema, cast=True)

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
        res_df = EdgeStager.map_to_schema(test_df, test_schema, cast=False)

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

        test_loader = MockEdgeStager()

        test_df = MagicMock()
        test_layer = "landing"
        test_dataset = "edges"

        # Act
        test_loader.write_parquet(test_df, test_layer, test_dataset)

        # Assert
        mock_mkdir.assert_not_called()
        test_df.write.parquet.assert_called_once_with(
            "data_dir/landing/edges", mode="overwrite"
        )

    def test_target_dir_does_not_exist(
        self, mock_exists: MagicMock, mock_mkdir: MagicMock
    ):
        """If the target folder does not exist, it should be created"""
        # Arrange
        mock_exists.return_value = False

        test_loader = MockEdgeStager()

        test_df = MagicMock()
        test_layer = "landing"
        test_dataset = "edges"

        # Act
        test_loader.write_parquet(test_df, test_layer, test_dataset)

        # Assert
        mock_mkdir.assert_called_once_with(
            Path("data_dir/landing/edges"), parents=True
        )
        test_df.write.parquet.assert_called_once_with(
            "data_dir/landing/edges", mode="overwrite"
        )


@patch("pathlib.Path.exists", autospec=True)
class TestReadParquet:
    """Make sure parquet reads have been set up properly"""

    def test_target_exists(self, mock_exists: MagicMock):
        """If the file exists, read it"""
        # Arrange
        mock_exists.return_value = True

        test_loader = MockEdgeStager()

        test_layer = "landing"
        test_dataset = "edges"

        # Act
        result = test_loader.read_parquet(test_layer, test_dataset)

        # Assert
        test_loader.spark.read.parquet.assert_called_once_with(
            "data_dir/landing/edges"
        )
        assert result is test_loader.spark.read.parquet.return_value

    def test_target_does_not_exist(self, mock_exists: MagicMock):
        """If the file does not exist, an exception should be raised"""
        # Arrange
        mock_exists.return_value = False

        test_loader = MockEdgeStager()

        test_layer = "landing"
        test_dataset = "edges"

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

        test_loader = MockEdgeStager()

        test_layer = "landing"
        test_dataset = "edges"

        # Act
        result = test_loader.read_delta(test_layer, test_dataset)

        # Assert
        mock_deltatable.forPath.assert_called_once_with(
            test_loader.spark, "data_dir/landing/edges"
        )
        assert result is mock_deltatable.forPath.return_value.toDF.return_value

    def test_target_does_not_exist(
        self, mock_exists: MagicMock, mock_deltatable: MagicMock
    ):
        """If the file does not exist, an exception should be raised"""
        # Arrange
        mock_exists.return_value = False

        test_loader = MockEdgeStager()

        test_layer = "landing"
        test_dataset = "edges"

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
        test_stager = MockEdgeStager()

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
        test_stager = MockEdgeStager()

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
        test_stager = MockEdgeStager()

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
    test_stager = MockEdgeStager()

    mock_tbl = MagicMock()
    mock_deltatable.forPath.return_value = mock_tbl

    # Act
    test_stager.clear_data_without_elevation(dataset="edges")

    # Assert
    mock_deltatable.forPath.assert_called_once_with(
        test_stager.spark, "data_dir/staging/edges"
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
        test_loader = MockEdgeStager()

        # Act
        test_loader.clear_temp_files("edge_updates")

        # Assert
        mock_rmtree.assert_called_once_with("data_dir/temp/edge_updates")

    def test_files_not_found(self, mock_rmtree: MagicMock):
        """If no temp files are present, the raised exception should be
        suppressed
        """
        # Arrange
        test_loader = MockEdgeStager()
        mock_rmtree.side_effect = FileNotFoundError

        # Act
        test_loader.clear_temp_files("edge_updates")

    def test_suppression_setup(self, mock_rmtree: MagicMock):
        """Make sure that suppression is definitely working by demonstrating
        that other exception types do get raised. Without this, the above
        test is not sufficient to show the expected behaviour when files are
        not found. In isolation it doesn't prove that the side effect has been
        set up properly.
        """
        # Arrange
        test_loader = MockEdgeStager()
        mock_rmtree.side_effect = ValueError

        # Act, Assert
        with pytest.raises(ValueError):
            test_loader.clear_temp_files("edge_updates")


# MARK: Implementation Specific


@patch("fell_loader.staging.edges.stg")
@patch("fell_loader.staging.edges.DeltaTable")
def test_initialize_output_table(
    mock_deltatable: MagicMock, mock_stg: MagicMock
):
    """Check that the correct calls are being generated"""
    # Arrange
    test_stager = MockEdgeStager()

    mock_stg.EDGES_SCHEMA.fields = "fields"

    mock_deltatable.createIfNotExists.return_value = mock_deltatable
    mock_deltatable.location.return_value = mock_deltatable
    mock_deltatable.addColumns.return_value = mock_deltatable

    # Act
    test_stager.initialize_output_table()

    # Assert
    mock_deltatable.createIfNotExists.assert_called_once_with(
        test_stager.spark
    )
    mock_deltatable.location.assert_called_once_with("data_dir/staging/edges")
    mock_deltatable.addColumns.assert_called_once_with("fields")
    mock_deltatable.execute.assert_called_once()


class TestGetEdgesToRemove:
    """Check that the correct edges are being flagged for removal"""

    def test_removals_applied(self):
        """If removals have already been applied, no action should be taken"""
        # Arrange
        test_stager = MockEdgeStager()
        test_stager.removals_applied = True

        # Act
        result = test_stager.get_edges_to_remove()

        # Assert
        assert result is None

    def test_removals_not_applied(self, test_session: SparkSession):
        """If removals have not yet been applied, the appropriate records
        should be returned
        """
        # Arrange #############################################################

        # ----- Mocking -----
        test_stager = MockEdgeStager()
        test_stager.read_parquet = MagicMock()
        test_stager.read_delta = MagicMock()

        # ----- New Edges -----
        test_new_data = [[0, 0], [1, 1], [2, 2], [3, 3]]
        test_new_schema = StructType(
            [
                StructField("src", IntegerType()),
                StructField("dst", IntegerType()),
            ]
        )
        test_new_df = test_session.createDataFrame(
            test_new_data, test_new_schema
        )
        test_stager.read_parquet.return_value = test_new_df

        # ----- Existing Edges -----
        test_ex_data = [[2, 2], [3, 3], [4, 4], [5, 5]]
        test_ex_schema = StructType(
            [
                StructField("src", IntegerType()),
                StructField("dst", IntegerType()),
            ]
        )
        test_ex_df = test_session.createDataFrame(test_ex_data, test_ex_schema)
        test_stager.read_delta.return_value = test_ex_df

        # ----- Target -----
        tgt_data = [[4, 4], [5, 5]]
        tgt_schema = StructType(
            [
                StructField("src", IntegerType()),
                StructField("dst", IntegerType()),
            ]
        )
        tgt_df = test_session.createDataFrame(tgt_data, tgt_schema)

        # Act #################################################################
        res_df = test_stager.get_edges_to_remove()

        # Assert ##############################################################
        assert res_df is not None
        assertDataFrameEqual(res_df, tgt_df)


class TestGetEdgesToUpdate:
    """Check that the correct edges are being pulled through for updates"""

    def test_records_returned(self, test_session: SparkSession):
        """Check that the right records are being fetched"""
        # Arrange #############################################################

        # ----- Mocking -----
        test_stager = MockEdgeStager()
        test_stager.read_parquet = MagicMock()
        test_stager.read_delta = MagicMock()

        # ----- New Edges -----
        # fmt: off
        #    src, dst, timestamp, src_easting, src_northing
        test_new_data = [
            # old only
            # new only
            [1  , 1  , 123456   , 50000      , 100000],
            # both, same timestamp
            [2  , 2  , 123456   , 50000      , 100000],
            # both, updated timestamp
            [3  , 3  , 234567   , 50000      , 100000],
        ]
        # fmt: on
        test_new_schema = StructType(
            [
                StructField("src", IntegerType()),
                StructField("dst", IntegerType()),
                StructField("timestamp", LongType()),
                StructField("src_easting", IntegerType()),
                StructField("src_northing", IntegerType()),
            ]
        )
        test_new_df = test_session.createDataFrame(
            test_new_data, test_new_schema
        )
        test_stager.read_parquet.return_value = test_new_df

        # ----- Existing Edges -----
        # fmt: off
        #    src, dst, timestamp
        test_ex_data = [
            # old only
            [0  , 0  , 123456],
            # new only
            # both, same timestamp
            [2  , 2  , 123456],
            # both, updated timestamp
            [3  , 3  , 123456],
        ]
        # fmt: on
        test_ex_schema = StructType(
            [
                StructField("src", IntegerType()),
                StructField("dst", IntegerType()),
                StructField("timestamp", LongType()),
            ]
        )
        test_ex_df = test_session.createDataFrame(test_ex_data, test_ex_schema)
        test_stager.read_delta.return_value = test_ex_df

        # ----- Target -----
        # fmt: off
        #    src, dst, timestamp, src_easting, src_northing
        tgt_data = [
            # old only
            # new only
            [1  , 1  , 123456   , 50000      , 100000],
            # both, same timestamp
            # both, updated timestamp
            [3  , 3  , 234567   , 50000      , 100000],
        ]
        # fmt: on
        tgt_schema = StructType(
            [
                StructField("src", IntegerType()),
                StructField("dst", IntegerType()),
                StructField("timestamp", LongType()),
                StructField("src_easting", IntegerType()),
                StructField("src_northing", IntegerType()),
            ]
        )
        tgt_df = test_session.createDataFrame(tgt_data, tgt_schema)

        # Act #################################################################
        res_df = test_stager.get_edges_to_update()

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
        test_stager = MockEdgeStager()
        test_stager.read_parquet = MagicMock()
        test_stager.read_delta = MagicMock()

        # ----- New Edges -----
        # fmt: off
        #    src, dst, timestamp, src_easting, src_northing
        test_new_data = [
            # old only
            # both, same timestamp
            [2  , 2  ,  123456   , 50000      , 100000],
        ]
        # fmt: on
        test_new_schema = StructType(
            [
                StructField("src", IntegerType()),
                StructField("dst", IntegerType()),
                StructField("timestamp", LongType()),
                StructField("src_easting", IntegerType()),
                StructField("src_northing", IntegerType()),
            ]
        )
        test_new_df = test_session.createDataFrame(
            test_new_data, test_new_schema
        )
        test_stager.read_parquet.return_value = test_new_df

        # ----- Existing Edges -----
        # fmt: off
        #    src, dst, timestamp
        test_ex_data = [
            # old only
            [0  , 0  , 123456],
            # both, same timestamp
            [2  , 2  , 123456],
        ]
        # fmt: on
        test_ex_schema = StructType(
            [
                StructField("src", IntegerType()),
                StructField("dst", IntegerType()),
                StructField("timestamp", LongType()),
            ]
        )
        test_ex_df = test_session.createDataFrame(test_ex_data, test_ex_schema)
        test_stager.read_delta.return_value = test_ex_df

        # ----- Target -----
        # fmt: off
        #    src, dst, timestamp, src_easting, src_northing
        tgt_data = [
            # old only
            # both, same timestamp
        ]
        # fmt: on
        tgt_schema = StructType(
            [
                StructField("src", IntegerType()),
                StructField("dst", IntegerType()),
                StructField("timestamp", LongType()),
                StructField("src_easting", IntegerType()),
                StructField("src_northing", IntegerType()),
            ]
        )
        tgt_df = test_session.createDataFrame(tgt_data, tgt_schema)

        # Act #################################################################
        res_df = test_stager.get_edges_to_update()

        # Assert ##############################################################
        assertDataFrameEqual(res_df, tgt_df)
        assert test_stager.remaining_count == 0
        assert test_stager.updates_applied


@patch("fell_loader.staging.edges.lnd")
def test_read_elevation(mock_lnd: MagicMock, test_session: SparkSession):
    """Make sure the right elevation data is being read in based on the
    lidar bounds dataset
    """
    # Arrange #################################################################

    # ----- Mocking -----
    mock_spark = MagicMock()
    test_stager = MockEdgeStager(spark=mock_spark)
    test_stager.read_parquet = MagicMock()

    mock_lnd.LIDAR_SCHEMA = "LIDAR_SCHEMA"

    # ----- Edges -----

    # fmt: off
    #    src_easting, src_northing, dst_easting, dst_northing, inx
    test_edges_data = [
        # both outside
        [0          , 13          , 1          , 14          , 1],
        # easting inside, northing outside
        [7          , 13          , 8          , 14          , 2],
        # easting outside, northing inside
        [0          , 8           , 1          , 9           , 3],
        # both inside (src only)
        [7          , 8           , 1          , 14          , 4],
        # both inside (dst only)
        [0          , 13          , 17         , 18          , 5],
        # both inside (src and dst)
        [17         , 18          , 27         , 28          , 6],
    ]
    # fmt: on

    test_edges_schema = StructType(
        [
            StructField("src_easting", IntegerType()),
            StructField("src_northing", IntegerType()),
            StructField("dst_easting", IntegerType()),
            StructField("dst_northing", IntegerType()),
            StructField("inx", IntegerType()),
        ]
    )
    test_edges_df = test_session.createDataFrame(
        test_edges_data, test_edges_schema
    )
    # ----- Lidar Bounds -----
    # fmt: off
    #    file_id   , min_easting, max_easting, min_northing, max_northing
    test_bounds_data = [
        ['file_one'  , 5          , 10         , 5           , 10],
        ['file_two'  , 15         , 20         , 15          , 20],
        ['file_three', 25         , 30         , 25          , 30],
        ['file_four' , 35         , 40         , 35          , 40],
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
        "data_dir/landing/lidar/file_three.parquet",
    ]

    # Act #####################################################################

    result = test_stager.read_elevation(test_edges_df)

    # Assert ##################################################################

    test_stager.spark.read.schema.assert_called_once_with("LIDAR_SCHEMA")
    assert sorted(
        test_stager.spark.read.schema.return_value.parquet.call_args.args
    ) == sorted(tgt_paths)
    assert (
        result
        is test_stager.spark.read.schema.return_value.parquet.return_value
    )


@patch("fell_loader.staging.edges.ELEVATION_RES_M", new=10)
def test_calculate_step_metrics(test_session: SparkSession):
    """Make sure the correct number of steps are being assigned to each edge"""
    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # fmt: off
    _ = (
        ['src_easting', 'src_northing', 'dst_easting', 'dst_northing'])

    test_data = [
        # Greater than the configured step size
        [0            , 0             , 20           , 20],
        [10           , 10            , 25           , 25],
        # Equal to the configured step size
        [0            , 0             , 10           , 0],
        [0            , 0             , 0            , 10],
        # Less than the configured step size
        [0            , 0             , 3            , 3],
        [10           , 10            , 15           , 15],
        # Opposite direction
        [25           , 25            , 10           , 10]
    ]

    # fmt: on

    test_schema = StructType(
        [
            StructField("src_easting", IntegerType()),
            StructField("src_northing", IntegerType()),
            StructField("dst_easting", IntegerType()),
            StructField("dst_northing", IntegerType()),
        ]
    )

    test_df = test_session.createDataFrame(test_data, test_schema)

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ['src_easting', 'src_northing', 'dst_easting', 'dst_northing', 'edge_size_h', 'edge_size_v', 'edge_size', 'edge_steps'])

    target_data = [
        # Greater than the configured step size
        [0            , 0             , 20           , 20            , 20           , 20           , 800**0.5   , 4],
        [10           , 10            , 25           , 25            , 15           , 15           , 450**0.5   , 3],
        # Equal to the configured step size
        [0            , 0             , 10           , 0             , 10           , 0            , 10.0       , 2],
        [0            , 0             , 0            , 10            , 0            , 10           , 10.0       , 2],
        # Less than the configured step size
        [0            , 0             , 3            , 3             , 3            , 3            , 18**0.5    , 2],
        [10           , 10            , 15           , 15            , 5            , 5            , 50**0.5    , 2],
        # Opposite direction
        [25           , 25            , 10           , 10            , -15          , -15          , 450**0.5   , 3]
    ]

    # fmt: on

    target_schema = StructType(
        [
            StructField("src_easting", IntegerType()),
            StructField("src_northing", IntegerType()),
            StructField("dst_easting", IntegerType()),
            StructField("dst_northing", IntegerType()),
            StructField("edge_size_h", IntegerType()),
            StructField("edge_size_v", IntegerType()),
            StructField("edge_size", DoubleType()),
            StructField("edge_steps", IntegerType()),
        ]
    )

    target_df = test_session.createDataFrame(target_data, target_schema)
    target_df = target_df.select(*sorted(target_df.columns))

    # Act #####################################################################

    result_df = EdgeStager.calculate_step_metrics(test_df)
    result_df = result_df.select(*sorted(result_df.columns))

    # Assert ##################################################################
    assertDataFrameEqual(result_df, target_df)


def test_explode_edges(test_session: SparkSession):
    """Make sure edges are being split across the correct number of rows"""
    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # fmt: off
    _ = (
        ['src_easting', 'src_northing', 'dst_easting', 'dst_northing', 'edge_steps'])

    test_data = [
        # Greater than the configured step size
        [0            , 0             , 20          , 20             , 4],
        [10           , 10            , 25          , 25             , 3],
        # Equal to the configured step size
        [0            , 0             , 10          , 0              , 2],
        [0            , 0             , 0           , 10             , 2],
        # Less than the configured step size
        [0            , 0             , 3           , 3              , 2],
        [10           , 10            , 15          , 15             , 2],
        # Opposite direction
        [25           , 25            , 10          , 10             , 3]
    ]

    # fmt: on

    test_schema = StructType(
        [
            StructField("src_easting", IntegerType()),
            StructField("src_northing", IntegerType()),
            StructField("dst_easting", IntegerType()),
            StructField("dst_northing", IntegerType()),
            StructField("edge_steps", IntegerType()),
        ]
    )

    test_df = test_session.createDataFrame(test_data, test_schema)

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ['src_easting', 'src_northing', 'dst_easting', 'dst_northing', 'edge_steps', 'coords'])

    target_data = [
        # Greater than the configured step size
        [0            , 0             , 20          , 20             , 4           , {'edge_inx_arr': 0, 'easting_arr': 0.0, 'northing_arr': 0.0}],
        [0            , 0             , 20          , 20             , 4           , {'edge_inx_arr': 1, 'easting_arr': 20.0/3, 'northing_arr': 20.0/3}],
        [0            , 0             , 20          , 20             , 4           , {'edge_inx_arr': 2, 'easting_arr': 2*(20.0/3), 'northing_arr': 2*(20.0/3)}],
        [0            , 0             , 20          , 20             , 4           , {'edge_inx_arr': 3, 'easting_arr': 20.0, 'northing_arr': 20.0}],
        [10           , 10            , 25          , 25             , 3           , {'edge_inx_arr': 0, 'easting_arr': 10.0, 'northing_arr': 10.0}],
        [10           , 10            , 25          , 25             , 3           , {'edge_inx_arr': 1, 'easting_arr': 10.0+(15.0/2), 'northing_arr': 10.0+(15.0/2)}],
        [10           , 10            , 25          , 25             , 3           , {'edge_inx_arr': 2, 'easting_arr': 25.0, 'northing_arr': 25.0}],
        # Equal to the configured step size
        [0            , 0             , 10          , 0              , 2           , {'edge_inx_arr': 0, 'easting_arr': 0.0, 'northing_arr': 0.0}],
        [0            , 0             , 10          , 0              , 2           , {'edge_inx_arr': 1, 'easting_arr': 10.0, 'northing_arr': 0.0}],
        [0            , 0             , 0           , 10             , 2           , {'edge_inx_arr': 0, 'easting_arr': 0.0, 'northing_arr': 0.0}],
        [0            , 0             , 0           , 10             , 2           , {'edge_inx_arr': 1, 'easting_arr': 0.0, 'northing_arr': 10.0}],
        # Less than the configured step size
        [0            , 0             , 3           , 3              , 2           , {'edge_inx_arr': 0, 'easting_arr': 0.0, 'northing_arr': 0.0}],
        [0            , 0             , 3           , 3              , 2           , {'edge_inx_arr': 1, 'easting_arr': 3.0, 'northing_arr': 3.0}],
        [10           , 10            , 15          , 15             , 2           , {'edge_inx_arr': 0, 'easting_arr': 10.0, 'northing_arr': 10.0}],
        [10           , 10            , 15          , 15             , 2           , {'edge_inx_arr': 1, 'easting_arr': 15.0, 'northing_arr': 15.0}],
        # Opposite direction
        [25           , 25            , 10          , 10             , 3           , {'edge_inx_arr': 0, 'easting_arr': 25.0, 'northing_arr': 25.0}],
        [25           , 25            , 10          , 10             , 3           , {'edge_inx_arr': 1, 'easting_arr': 25.0+(-15.0/2),'northing_arr': 25.0+(-15.0/2)}],
        [25           , 25            , 10          , 10             , 3           , {'edge_inx_arr': 2, 'easting_arr': 10.0, 'northing_arr': 10.0}],
    ]

    # fmt: on

    target_schema = StructType(
        [
            StructField("src_easting", IntegerType()),
            StructField("src_northing", IntegerType()),
            StructField("dst_easting", IntegerType()),
            StructField("dst_northing", IntegerType()),
            StructField("edge_steps", IntegerType()),
            StructField(
                "coords",
                StructType(
                    [
                        StructField("edge_inx_arr", IntegerType()),
                        StructField("easting_arr", DoubleType()),
                        StructField("northing_arr", DoubleType()),
                    ]
                ),
            ),
        ]
    )

    target_df = test_session.createDataFrame(target_data, target_schema)
    target_df = target_df.select(*sorted(target_df.columns))

    # Act #####################################################################

    result_df = EdgeStager.explode_edges(test_df)
    result_df = result_df.select(*sorted(result_df.columns))

    # Assert ##################################################################
    assertDataFrameEqual(result_df, target_df)


def test_unpack_exploded_edges(test_session: SparkSession):
    """Make sure the coords structure is correctly unpacked"""
    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # fmt: off
    _ = (
        ['src', 'dst', 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'tags', 'timestamp', 'other', 'coords'])

    test_data = [
        ['src', 'dst', 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'tags', 'timestamp', 'other', {'edge_inx_arr': 'edge_inx_arr', 'easting_arr': 1.0, 'northing_arr': 2.0}]
    ]

    # fmt: on

    test_schema = StructType(
        [
            StructField("src", StringType()),
            StructField("dst", StringType()),
            StructField("src_lat", StringType()),
            StructField("src_lon", StringType()),
            StructField("dst_lat", StringType()),
            StructField("dst_lon", StringType()),
            StructField("src_easting", StringType()),
            StructField("src_northing", StringType()),
            StructField("way_id", StringType()),
            StructField("way_inx", StringType()),
            StructField("tags", StringType()),
            StructField("timestamp", StringType()),
            StructField("other", StringType()),
            StructField(
                "coords",
                StructType(
                    [
                        StructField("edge_inx_arr", StringType()),
                        StructField("easting_arr", StringType()),
                        StructField("northing_arr", StringType()),
                    ]
                ),
            ),
        ]
    )

    test_df = test_session.createDataFrame(test_data, test_schema)

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ['src', 'dst', 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'tags', 'timestamp', 'edge_inx'    , 'easting', 'northing'])

    target_data = [
        ['src', 'dst', 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'tags', 'timestamp', 'edge_inx_arr', 1        , 2]
    ]

    # fmt: on

    target_schema = StructType(
        [
            StructField("src", StringType()),
            StructField("dst", StringType()),
            StructField("src_lat", StringType()),
            StructField("src_lon", StringType()),
            StructField("dst_lat", StringType()),
            StructField("dst_lon", StringType()),
            StructField("src_easting", StringType()),
            StructField("src_northing", StringType()),
            StructField("way_id", StringType()),
            StructField("way_inx", StringType()),
            StructField("tags", StringType()),
            StructField("timestamp", StringType()),
            StructField("edge_inx", StringType()),
            StructField("easting", IntegerType()),
            StructField("northing", IntegerType()),
        ]
    )

    target_df = test_session.createDataFrame(target_data, target_schema)
    target_df = target_df.select(*sorted(target_df.columns))

    # Act #####################################################################

    result_df = EdgeStager.unpack_exploded_edges(test_df)
    result_df = result_df.select(*sorted(result_df.columns))

    # Assert ##################################################################
    assertDataFrameEqual(result_df, target_df)


def test_tag_exploded_edges(test_session: SparkSession):
    """Make sure the table join has been set up properly"""
    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # ----- Edges -----

    # fmt: off
    _ = (
        ['easting', 'northing', 'src' , 'dst' , 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'edge_inx', 'way_id', 'way_inx', 'tags', 'timestamp'])

    test_edge_data = [
        ['left'   , 'left'    , 'left', 'left', 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'edge_inx', 'way_id', 'way_inx', 'tags', 'timestamp'],
        ['both'   , 'both'    , 'both', 'both', 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'edge_inx', 'way_id', 'way_inx', 'tags', 'timestamp']
    ]
    # fmt: on

    test_edge_schema = StructType(
        [
            StructField("easting", StringType()),
            StructField("northing", StringType()),
            StructField("src", StringType()),
            StructField("dst", StringType()),
            StructField("src_lat", StringType()),
            StructField("src_lon", StringType()),
            StructField("dst_lat", StringType()),
            StructField("dst_lon", StringType()),
            StructField("src_easting", StringType()),
            StructField("src_northing", StringType()),
            StructField("edge_inx", StringType()),
            StructField("way_id", StringType()),
            StructField("way_inx", StringType()),
            StructField("tags", StringType()),
            StructField("timestamp", StringType()),
        ]
    )

    test_edge_df = test_session.createDataFrame(
        test_edge_data, test_edge_schema
    )

    # ----- Elevation -----

    # fmt: off
    _ = (
        ['easting', 'northing', 'elevation'])

    test_ele_data = [
        ['both'   , 'both'    , 'both'],
        ['right'  , 'right'   , 'right']
    ]
    # fmt: on

    test_ele_schema = StructType(
        [
            StructField("easting", StringType()),
            StructField("northing", StringType()),
            StructField("elevation", StringType()),
        ]
    )

    test_ele_df = test_session.createDataFrame(test_ele_data, test_ele_schema)

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ['src'  , 'dst' , 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'edge_inx', 'elevation', 'way_id', 'way_inx', 'tags', 'timestamp'])

    target_data = [
        ['left' , 'left', 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'edge_inx', None       , 'way_id', 'way_inx', 'tags', 'timestamp'],
        ['both' , 'both', 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'edge_inx', 'both'     , 'way_id', 'way_inx', 'tags', 'timestamp']
    ]
    # fmt: on

    target_schema = StructType(
        [
            StructField("src", StringType()),
            StructField("dst", StringType()),
            StructField("src_lat", StringType()),
            StructField("src_lon", StringType()),
            StructField("dst_lat", StringType()),
            StructField("dst_lon", StringType()),
            StructField("src_easting", StringType()),
            StructField("src_northing", StringType()),
            StructField("edge_inx", StringType()),
            StructField("elevation", StringType()),
            StructField("way_id", StringType()),
            StructField("way_inx", StringType()),
            StructField("tags", StringType()),
            StructField("timestamp", StringType()),
        ]
    )

    target_df = test_session.createDataFrame(target_data, target_schema)
    target_df = target_df.select(*sorted(target_df.columns))

    # Act #####################################################################

    result_df = EdgeStager.tag_exploded_edges(test_edge_df, test_ele_df)
    result_df = result_df.select(*sorted(result_df.columns))

    # Assert ##################################################################
    assertDataFrameEqual(result_df, target_df)


def test_implode_edges(test_session: SparkSession):
    """Make sure data is being aggregated properly"""
    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # fmt: off
    _ = (
        ['src', 'dst', 'edge_inx', 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'tags', 'timestamp', 'elevation', 'other'])

    test_data = [
        # Single record
        [0    , 1    , 1         , 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'tags', 'timestamp', 1.0        , 'other'],
        # Multiple records (input order shuffled)
        [1    , 2    , 1         , 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'tags', 'timestamp', 2.0        , 'other'],
        [1    , 2    , 3         , 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'tags', 'timestamp', 4.0        , 'other'],
        [1    , 2    , 2         , 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'tags', 'timestamp', 3.0        , 'other'],
        # Some data missing (edge only partially covered by available LIDAR files)
        [2    , 3    , 1         , 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'tags', 'timestamp', 5.0        , 'other'],
        [2    , 3    , 2         , 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'tags', 'timestamp', 6.0        , 'other'],
        [2    , 3    , 3         , 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'tags', 'timestamp', 7.0        , 'other'],
        [2    , 3    , 4         , 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'tags', 'timestamp', None       , 'other'],
    ]

    # fmt: on

    test_schema = StructType(
        [
            StructField("src", IntegerType()),
            StructField("dst", IntegerType()),
            StructField("edge_inx", IntegerType()),
            StructField("src_lat", StringType()),
            StructField("src_lon", StringType()),
            StructField("dst_lat", StringType()),
            StructField("dst_lon", StringType()),
            StructField("src_easting", StringType()),
            StructField("src_northing", StringType()),
            StructField("way_id", StringType()),
            StructField("way_inx", StringType()),
            StructField("tags", StringType()),
            StructField("timestamp", StringType()),
            StructField("elevation", DoubleType()),
            StructField("other", StringType()),
        ]
    )

    test_df = test_session.createDataFrame(test_data, test_schema)

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ['src', 'dst', 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'tags', 'timestamp', 'elevation'])

    target_data = [
        # Single record
        [0    , 1    , 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'tags', 'timestamp', [1.0]],
        # Multiple records
        [1    , 2    , 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'tags', 'timestamp', [2.0, 3.0, 4.0]],
        # Some data missing
        [2    , 3    , 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'tags', 'timestamp', None]
    ]

    # fmt: on

    target_schema = StructType(
        [
            StructField("src", IntegerType()),
            StructField("dst", IntegerType()),
            StructField("src_lat", StringType()),
            StructField("src_lon", StringType()),
            StructField("dst_lat", StringType()),
            StructField("dst_lon", StringType()),
            StructField("src_easting", StringType()),
            StructField("src_northing", StringType()),
            StructField("way_id", StringType()),
            StructField("way_inx", StringType()),
            StructField("tags", StringType()),
            StructField("timestamp", StringType()),
            StructField("elevation", ArrayType(DoubleType())),
        ]
    )

    target_df = test_session.createDataFrame(target_data, target_schema)
    target_df = target_df.select(*sorted(target_df.columns))

    # Act #####################################################################

    result_df = EdgeStager.implode_edges(test_df)
    result_df = result_df.select(*sorted(result_df.columns))

    # Assert ##################################################################
    assertDataFrameEqual(result_df, target_df)


@pytest.mark.skip("Minimal value, all operations must be mocked")
class TestApplyEdgeUpdates:
    """Make sure the edges output dataset is being updated properly"""

    def test_with_removals(self):
        """Check behaviour when no removals need to be applied"""
        raise AssertionError()

    def test_without_removals(self):
        """Check behaviour when removals do need to be applied"""
        raise AssertionError()


@patch("fell_loader.staging.edges.DeltaTable")
def test_optimise_edges_table(mock_deltatable: MagicMock):
    """Make sure the right calls are being generated to optimise the table"""
    # Arrange
    test_stager = MockEdgeStager()

    mock_tbl = MagicMock()
    mock_deltatable.forPath.return_value = mock_tbl

    # Act
    test_stager.optimise_edges_table()

    # Asser
    mock_deltatable.forPath.assert_called_once_with(
        test_stager.spark, "data_dir/staging/edges"
    )
    mock_tbl.optimize.return_value.executeCompaction.assert_called_once()
    mock_tbl.vacuum.assert_called_once()


@patch("pathlib.Path.write_text")
@patch("pathlib.Path.glob")
def test_record_lidar_files(mock_glob: MagicMock, mock_write_text: MagicMock):
    """Make sure the right data is being written out in the right format"""
    # Arrange
    mock_glob.return_value = [
        Path("file_2.parquet"),
        Path("file_1.parquet"),
        Path("file_3.parquet"),
    ]

    test_stager = MockEdgeStager()

    target_str = "['file_1.parquet', 'file_2.parquet', 'file_3.parquet']"

    # Act
    test_stager.record_lidar_files()

    # Assert
    mock_write_text.assert_called_once_with(target_str)


@pytest.mark.skip("High effort, low value")
def test_run():
    """Check that all expected function calls are generated"""
