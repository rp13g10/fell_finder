"""Unit tests for LIDAR file parsing"""

import os
from unittest.mock import patch, MagicMock, call

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
)
from pyspark.testing.utils import assertDataFrameEqual

from fell_finder.ingestion.lidar.parsing import LidarLoader


@patch("fell_finder.ingestion.lidar.parsing.rio.open")
@patch("fell_finder.ingestion.lidar.parsing.glob")
def test_load_lidar_from_folder(
    mock_glob: MagicMock, mock_rio_open: MagicMock
):
    """Check that data is passed through the function call as expected"""
    # Arrange
    mock_glob.return_value = ["file_one"]

    # Sets tif while inside the 'with X as tif' block
    mock_tif_inside = MagicMock()
    mock_tif_inside.read = MagicMock(return_value=[np.zeros(1)])

    # Sets tif while the 'with X as tif' statement is evaluated
    mock_tif_outside = MagicMock()
    mock_tif_outside.__enter__ = MagicMock(return_value=mock_tif_inside)

    mock_rio_open.return_value = mock_tif_outside

    test_lidar_dir = "/some/path"

    target_glob_call = os.path.join("/some/path", "*.tif")
    target_rio_call = "file_one"
    target_output = np.zeros(1)

    # Act
    result = LidarLoader.load_lidar_from_folder(test_lidar_dir)

    # Assert
    mock_glob.assert_called_once_with(target_glob_call)
    mock_rio_open.assert_called_once_with(target_rio_call)
    assert result == target_output


@patch("fell_finder.ingestion.lidar.parsing.open")
@patch("fell_finder.ingestion.lidar.parsing.glob")
def test_load_bbox_from_folder(mock_glob: MagicMock, mock_open: MagicMock):
    """Check that data is passed through the function call as expected"""
    # Arrange
    mock_glob.return_value = ["file_one"]

    # Mock out behaviour for data read
    mock_lines = [
        "1.0000000000\n",
        "0.0000000000\n",
        "0.0000000000\n",
        "-1.0000000000\n",
        "445000.5000000000\n",
        "119999.5000000000\n",
    ]
    mock_readlines = MagicMock(return_value=mock_lines, id="readlines")

    mock_fobj = MagicMock(id="fobj")
    mock_fobj.readlines = mock_readlines

    mock_handle = MagicMock(id="handle")
    mock_handle.__enter__ = MagicMock(return_value=mock_fobj, id="enter")

    mock_open.return_value = mock_handle

    # Set dummy file location
    test_lidar_dir = "/some/path"

    # Set expected outputs
    target_glob_call = os.path.join("/some/path", "*.tfw")
    target_open_args = ("file_one", "r")
    target_open_kwargs = {"encoding": "utf8"}

    target_output = np.array([445000, 115000, 450000, 120000])

    # Act
    result = LidarLoader.load_bbox_from_folder(test_lidar_dir)

    # Assert
    mock_glob.assert_called_once_with(target_glob_call)
    mock_open.assert_called_once_with(*target_open_args, **target_open_kwargs)
    assert (result == target_output).all()


def test_generate_file_id():
    """Check that file IDs are being generated properly"""
    # Arrange
    test_lidar_dir = "/some/folder/lidar_composite_dtm-2020-1-SU20ne"
    target = "SU20ne"

    # Act
    result = LidarLoader.generate_file_id(test_lidar_dir)

    # Assert
    assert result == target


def test_generate_data_from_lidar_array():
    """Check that LIDAR data is correctly being converted into records"""

    # Arrange
    test_bbox = np.array(
        [
            100000,  # Min easting
            200000,  # Min northing
            105000,  # Max easting
            205000,  # Max northing
        ]
    )

    # [0, 1, 2, ..., 24999999]
    test_lidar = np.array(range(0, 5000**2))
    # [[0, 1, ..., 4999], [5000, ..., 9999], ..., [24995000, ..., 24999999]]
    test_lidar = test_lidar.reshape(5000, 5000)

    # fmt: off
    target_head = {
        'easting': np.array([100000, 100001, 100002, 100003, 100004]).astype('int32'),
        'northing': np.array([204999, 204999, 204999, 204999, 204999]).astype('int32'),
        'elevation': np.array([0, 1, 2, 3, 4])
    }
    target_tail = {
        'easting': np.array([104995, 104996, 104997, 104998, 104999]).astype('int32'),
        'northing': np.array([200000, 200000, 200000, 200000, 200000]).astype('int32'),
        'elevation': np.array([24999995, 24999996, 24999997, 24999998, 24999999])
    }

    # fmt: on

    # Act
    result = LidarLoader.generate_data_from_lidar_array(test_lidar, test_bbox)

    result_head = {
        "easting": result["easting"][:5],
        "northing": result["northing"][:5],
        "elevation": result["elevation"][:5],
    }
    result_tail = {
        "easting": result["easting"][-5:],
        "northing": result["northing"][-5:],
        "elevation": result["elevation"][-5:],
    }

    # Assert
    assert (result_head["easting"] == target_head["easting"]).all()
    assert (result_head["northing"] == target_head["northing"]).all()
    assert (result_head["elevation"] == target_head["elevation"]).all()
    assert (result_tail["easting"] == target_tail["easting"]).all()
    assert (result_tail["northing"] == target_tail["northing"]).all()
    assert (result_tail["elevation"] == target_tail["elevation"]).all()


@patch("fell_finder.ingestion.lidar.parsing.get_available_folders")
def test_generate_dataframe_from_data(
    mock_get_available_folders: MagicMock, test_session: SparkSession
):
    """Make sure data is being read in properly"""

    # Arrange
    test_data = {
        "easting": np.array([0, 1, 2, 3, 4]).astype("int32"),
        "northing": np.array([5, 4, 3, 2, 1]).astype("int32"),
        "elevation": np.array([10.0, 11.0, 12.0, 13.0, 14.0]),
    }

    # fmt: off
    _ = (
        ['easting', 'northing', 'elevation']
    )

    target_data = [
        [0        , 5         , 10.0],
        [1        , 4         , 11.0],
        [2        , 3         , 12.0],
        [3        , 2         , 13.0],
        [4        , 1         , 14.0],
    ]
    # fmt: on

    target_schema = StructType(
        [
            StructField("easting", IntegerType()),
            StructField("northing", IntegerType()),
            StructField("elevation", DoubleType()),
        ]
    )

    target = test_session.createDataFrame(
        data=target_data, schema=target_schema
    )

    mock_get_available_folders.return_value = []
    mock_loader = LidarLoader(sql=test_session, data_dir="dummy")

    # Act
    result = mock_loader.generate_dataframe_from_data(test_data)

    # Assert
    assertDataFrameEqual(result, target)


@patch("fell_finder.ingestion.lidar.parsing.get_available_folders")
def test_add_file_ids(
    mock_get_available_folders: MagicMock, test_session: SparkSession
):
    """Check that file IDs are being added properly"""
    # Arrange #################################################################

    # ----- Test Data -----
    # fmt: off
    _ = (
        ['inx'])

    test_data = [
        [0]
    ]
    # fmt: on

    test_schema = StructType(
        [
            StructField("inx", IntegerType()),
        ]
    )

    test_df = test_session.createDataFrame(data=test_data, schema=test_schema)

    test_lidar_dir = "/some/folder/lidar_composite_dtm-2020-1-SU20ne"

    mock_get_available_folders.return_value = []
    mock_loader = LidarLoader(sql=test_session, data_dir="dummy")

    # ----- Target Data -----=
    # fmt: off
    _ = (
        ['inx', 'file_id'])

    tgt_data = [
        [0    , 'SU20ne']
    ]
    # fmt: on

    tgt_schema = StructType(
        [
            StructField("inx", IntegerType()),
            StructField("file_id", StringType(), False),
        ]
    )

    tgt_df = test_session.createDataFrame(data=tgt_data, schema=tgt_schema)

    # Act
    res_df = mock_loader.add_file_ids(test_df, test_lidar_dir)

    # Assert
    assertDataFrameEqual(tgt_df, res_df)


def test_set_output_schema(test_session: SparkSession):
    """Check that the correct output schema is being set"""
    # Arrange #################################################################

    # ----- Test Data -----
    # fmt: off
    test_cols = (
        ['easting', 'northing', 'elevation', 'file_id', 'easting_ptn', 'northing_ptn', 'other'])

    test_data = [
        [0] * len(test_cols)
    ]
    # fmt: on

    test_df = test_session.createDataFrame(data=test_data, schema=test_cols)

    # ----- Target Data -----=
    # fmt: off
    tgt_cols = (
        ['easting', 'northing', 'elevation', 'file_id', 'easting_ptn', 'northing_ptn'])

    tgt_data = [
        [0] * len(tgt_cols)
    ]
    # fmt: on

    tgt_df = test_session.createDataFrame(data=tgt_data, schema=tgt_cols)

    # Act
    res_df = LidarLoader.set_output_schema(test_df)

    # Assert
    assertDataFrameEqual(tgt_df, res_df)


@patch("fell_finder.ingestion.lidar.parsing.get_available_folders")
def test_write_df_to_parquet(
    mock_get_available_folders: MagicMock, test_session: SparkSession
):
    """Ensure the correct calls are made when writing data"""

    # Arrange
    mock_write = MagicMock()
    mock_parquet = MagicMock(return_value=mock_write)
    mock_partition_by = MagicMock(return_value=mock_write)
    mock_mode = MagicMock(return_value=mock_write)
    mock_write.partitionBy = mock_partition_by
    mock_write.parquet = mock_parquet
    mock_write.mode = mock_mode

    mock_df = MagicMock()
    mock_df.write = mock_write

    mock_loader = LidarLoader(test_session, "data_dir")

    mock_get_available_folders.return_value = []

    target_ptn_call = ("easting_ptn", "northing_ptn")
    target_pqt_call = "data_dir/parsed/lidar"
    target_mode_call = "overwrite"

    # Act
    mock_loader.write_df_to_parquet(mock_df)

    # Assert
    mock_partition_by.assert_called_once_with(*target_ptn_call)
    mock_parquet.assert_called_once_with(target_pqt_call)
    mock_mode.assert_called_once_with(target_mode_call)


@patch("fell_finder.ingestion.lidar.parsing.get_available_folders")
def test_parse_lidar_folder(
    mock_get_available_folders: MagicMock, test_session: SparkSession
):
    """Ensure the correct calls are made, and output is as expected"""

    # Arrange #################################################################

    # Mock Loader -------------------------------------------------------------

    mock_get_available_folders.return_value = []

    mock_load_lidar_from_folder = MagicMock()
    mock_load_bbox_from_folder = MagicMock()
    mock_generate_data_from_lidar_array = MagicMock()

    mock_loader = LidarLoader(test_session, "dummy")
    mock_loader.load_lidar_from_folder = mock_load_lidar_from_folder
    mock_loader.load_bbox_from_folder = mock_load_bbox_from_folder
    mock_loader.generate_data_from_lidar_array = (
        mock_generate_data_from_lidar_array
    )

    # Test Data ---------------------------------------------------------------

    # fmt: off
    _ = (
        ['easting', 'northing', 'elevation'])

    test_data = [
        [123456   , 987654    , 100.0]
    ]
    # fmt: on

    test_schema = StructType(
        [
            StructField("easting", IntegerType()),
            StructField("northing", IntegerType()),
            StructField("elevation", DoubleType()),
        ]
    )

    test_df = test_session.createDataFrame(data=test_data, schema=test_schema)

    mock_generate_dataframe_from_data = MagicMock(return_value=test_df)
    mock_loader.generate_dataframe_from_data = (
        mock_generate_dataframe_from_data
    )

    test_lidar_dir = "/some/folder/lidar_composite_dtm-2020-1-SU20ne"

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ['easting', 'northing', 'elevation', 'file_id', 'easting_ptn', 'northing_ptn'])

    tgt_data = [
        [123456   , 987654    , 100.0      , 'SU20ne' , 123          , 988]
    ]
    # fmt: on

    tgt_schema = StructType(
        [
            StructField("easting", IntegerType()),
            StructField("northing", IntegerType()),
            StructField("elevation", DoubleType()),
            StructField("file_id", StringType(), False),
            StructField("easting_ptn", IntegerType()),
            StructField("northing_ptn", IntegerType()),
        ]
    )

    tgt_df = test_session.createDataFrame(data=tgt_data, schema=tgt_schema)

    # Act #####################################################################

    result_df = mock_loader.parse_lidar_folder(test_lidar_dir)

    # Assert ##################################################################
    mock_load_lidar_from_folder.assert_called_once_with(test_lidar_dir)
    mock_load_bbox_from_folder.assert_called_once_with(test_lidar_dir)
    mock_generate_data_from_lidar_array.assert_called_once()

    assertDataFrameEqual(result_df, tgt_df)


@patch("fell_finder.ingestion.lidar.parsing.get_available_folders")
def test_load(
    mock_get_available_folders: MagicMock, test_session: SparkSession
):
    """Ensure the correct calls are made when writing data"""

    # Arrange
    mock_get_available_folders.return_value = ["file_one", "file_two"]

    mock_loader = LidarLoader(test_session, "dummy")

    mock_parse_lidar_folder = MagicMock(side_effect=["df_1", "df_2"])
    mock_loader.parse_lidar_folder = mock_parse_lidar_folder

    mock_write_df_to_parquet = MagicMock()
    mock_loader.write_df_to_parquet = mock_write_df_to_parquet

    tgt_parse_calls = [call("file_one"), call("file_two")]
    tgt_write_calls = [call("df_1"), call("df_2")]

    # Act
    mock_loader.load()

    # Assert
    mock_parse_lidar_folder.assert_has_calls(tgt_parse_calls)
    mock_write_df_to_parquet.assert_has_calls(tgt_write_calls)
