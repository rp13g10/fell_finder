"""Unit tests for the LIDAR parsing script"""

import os
from unittest.mock import MagicMock, call, patch

import numpy as np
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from fell_loader.parsing.lidar_loader import (
    LidarLoader,
    get_available_folders,
)
from fell_loader.utils.partitioning import get_partitions


class TestGetAvailableFolders:
    """Check behaviour is as-expected when searching for files"""

    @patch("fell_loader.parsing.lidar_loader.glob")
    def test_files_found(self, mock_glob: MagicMock):
        """Check expected output format when files are found"""
        # Arrange
        test_dir = "test/dir"
        mock_glob.return_value = ["item_one"]
        target_out = {"item_one"}
        target_call = "test/dir/extracts/lidar/lidar_composite_dtm-*"

        # Act
        result = get_available_folders(test_dir)

        # Assert
        assert result == target_out
        mock_glob.assert_called_once_with(target_call)

    @patch("fell_loader.parsing.lidar_loader.glob")
    def test_files_not_found(self, mock_glob: MagicMock):
        """Check exception is thrown when they aren't"""
        # Arrange
        test_dir = "dummy"
        mock_glob.return_value = {}

        # Act, Assert
        with pytest.raises(FileNotFoundError):
            _ = get_available_folders(test_dir)


@patch("fell_loader.parsing.lidar_loader.rio.open")
@patch("fell_loader.parsing.lidar_loader.glob")
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


@patch("fell_loader.parsing.lidar_loader.open")
@patch("fell_loader.parsing.lidar_loader.glob")
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


def test_generate_df_from_lidar_array():
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

    # [0, 1, 2, ..., 24999999]                                                 # noqa: ERA001
    test_lidar = np.array(range(0, 5000**2))
    # [[0, 1, ..., 4999], [5000, ..., 9999], ..., [24995000, ..., 24999999]]   # noqa: ERA001
    test_lidar = test_lidar.reshape(5000, 5000)

    # fmt: off
    target_head = pl.DataFrame({
        'easting': np.array([100000, 100001, 100002, 100003, 100004]).astype('int32'),
        'northing': np.array([204999, 204999, 204999, 204999, 204999]).astype('int32'),
        'elevation': np.array([0, 1, 2, 3, 4])
    },
            orient='row')
    target_tail = pl.DataFrame({
        'easting': np.array([104995, 104996, 104997, 104998, 104999]).astype('int32'),
        'northing': np.array([200000, 200000, 200000, 200000, 200000]).astype('int32'),
        'elevation': np.array([24999995, 24999996, 24999997, 24999998, 24999999])
    },
            orient='row')

    # fmt: on

    # Act
    result = LidarLoader.generate_df_from_lidar_array(test_lidar, test_bbox)

    result_head = result.head(5)
    result_tail = result.tail(5)

    # Assert
    assert_frame_equal(result_head, target_head)
    assert_frame_equal(result_tail, target_tail)


@patch("fell_loader.parsing.lidar_loader.get_available_folders")
def test_add_file_ids(mock_get_available_folders: MagicMock):
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

    test_schema = {"inx": pl.Int32()}

    test_df = pl.DataFrame(data=test_data, schema=test_schema, orient="row")

    test_lidar_dir = "/some/folder/lidar_composite_dtm-2020-1-SU20ne"

    mock_get_available_folders.return_value = []
    mock_loader = LidarLoader(data_dir="dummy")

    # ----- Target Data -----=
    # fmt: off
    _ = (
        ['inx', 'file_id'])

    tgt_data = [
        [0    , 'SU20ne']
    ]
    # fmt: on

    tgt_schema = {"inx": pl.Int32(), "file_id": pl.String()}

    tgt_df = pl.DataFrame(data=tgt_data, schema=tgt_schema, orient="row")

    # Act
    res_df = mock_loader.add_file_ids(test_df, test_lidar_dir)

    # Assert
    assert_frame_equal(tgt_df, res_df)


def test_set_output_schema():
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

    test_df = pl.DataFrame(data=test_data, schema=test_cols, orient="row")

    # ----- Target Data -----=
    # fmt: off
    tgt_cols = (
        ['easting', 'northing', 'elevation', 'file_id', 'easting_ptn', 'northing_ptn'])

    tgt_data = [
        [0] * len(tgt_cols)
    ]
    # fmt: on

    tgt_df = pl.DataFrame(data=tgt_data, schema=tgt_cols, orient="row")

    # Act
    res_df = LidarLoader.set_output_schema(test_df)

    # Assert
    assert_frame_equal(tgt_df, res_df)


@patch("fell_loader.parsing.lidar_loader.get_available_folders")
def test_write_df_to_parquet(mock_get_available_folders: MagicMock):
    """Ensure the correct calls are made when writing data"""

    # Arrange
    mock_write_parquet = MagicMock()
    mock_df = MagicMock()
    mock_df.write_parquet = mock_write_parquet

    mock_loader = LidarLoader("data_dir")

    mock_get_available_folders.return_value = []

    target_args = ["data_dir/parsed/lidar"]
    target_kwargs = {
        "use_pyarrow": True,
        "pyarrow_options": {"partition_cols": ["easting_ptn", "northing_ptn"]},
    }

    # Act
    mock_loader.write_df_to_parquet(mock_df)

    # Assert
    mock_write_parquet.assert_called_once_with(*target_args, **target_kwargs)


@patch("fell_loader.parsing.lidar_loader.get_available_folders")
def test_parse_lidar_folder(mock_get_available_folders: MagicMock):
    """Ensure the correct calls are made, and output is as expected"""

    # Arrange #################################################################

    # Mock Loader -------------------------------------------------------------

    mock_get_available_folders.return_value = []

    mock_load_lidar_from_folder = MagicMock()
    mock_load_bbox_from_folder = MagicMock()

    mock_loader = LidarLoader("dummy")
    mock_loader.load_lidar_from_folder = mock_load_lidar_from_folder
    mock_loader.load_bbox_from_folder = mock_load_bbox_from_folder

    # Test Data ---------------------------------------------------------------

    easting, northing = 123456, 987654

    # fmt: off
    _ = (
        ['easting', 'northing', 'elevation'])

    test_data = [
        [easting  , northing  , 100.0]
    ]
    # fmt: on

    test_schema = {
        "easting": pl.Int32(),
        "northing": pl.Int32(),
        "elevation": pl.Float64(),
    }

    test_df = pl.DataFrame(data=test_data, schema=test_schema, orient="row")

    mock_generate_df_from_lidar_array = MagicMock(return_value=test_df)
    mock_loader.generate_df_from_lidar_array = (
        mock_generate_df_from_lidar_array
    )

    test_lidar_dir = "/some/folder/lidar_composite_dtm-2020-1-SU20ne"

    # Target Data -------------------------------------------------------------

    easting_ptn, northing_ptn = get_partitions(easting, northing)

    # fmt: off
    _ = (
        ['easting', 'northing', 'elevation', 'file_id', 'easting_ptn', 'northing_ptn'])

    tgt_data = [
        [easting  , northing  , 100.0      , 'SU20ne' , easting_ptn  , northing_ptn]
    ]
    # fmt: on

    tgt_schema = {
        "easting": pl.Int32(),
        "northing": pl.Int32(),
        "elevation": pl.Float64(),
        "file_id": pl.String(),
        "easting_ptn": pl.Int32(),
        "northing_ptn": pl.Int32(),
    }

    tgt_df = pl.DataFrame(data=tgt_data, schema=tgt_schema, orient="row")

    # Act #####################################################################

    result_df = mock_loader.parse_lidar_folder(test_lidar_dir)

    # Assert ##################################################################
    mock_load_lidar_from_folder.assert_called_once_with(test_lidar_dir)
    mock_load_bbox_from_folder.assert_called_once_with(test_lidar_dir)
    mock_generate_df_from_lidar_array.assert_called_once()

    assert_frame_equal(result_df, tgt_df)


@patch("fell_loader.parsing.lidar_loader.get_available_folders")
def test_load(mock_get_available_folders: MagicMock):
    """Ensure the correct calls are made when writing data"""

    # Arrange
    mock_get_available_folders.return_value = ["file_one", "file_two"]

    mock_loader = LidarLoader("dummy")

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
