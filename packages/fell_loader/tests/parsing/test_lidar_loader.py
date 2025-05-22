"""Unit tests for the LIDAR parsing script"""

from unittest.mock import MagicMock, patch
from textwrap import dedent
from dataclasses import dataclass

import numpy as np
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from fell_loader.parsing.lidar_loader import (
    LidarLoader,
)


class MockLidarLoader(LidarLoader):
    """Mock implementation of the Lidar Loader which uses static values
    instead of fetching data from environment variables"""

    def __init__(self) -> None:
        self.data_dir = "data_dir"
        self.to_load = {"file_1"}


class TestGetAvailableFolders:
    """Check behaviour is as-expected when searching for files"""

    @patch("fell_loader.parsing.lidar_loader.glob")
    def test_files_found(self, mock_glob: MagicMock):
        """Check expected output format when files are found"""
        # Arrange
        mock_glob.return_value = ["item_one"]
        target_out = {"item_one"}
        target_call = "data_dir/extracts/lidar/*.zip"

        test_loader = MockLidarLoader()

        # Act
        result = test_loader.get_available_folders()

        # Assert
        assert result == target_out
        mock_glob.assert_called_once_with(target_call)

    @patch("fell_loader.parsing.lidar_loader.glob")
    def test_files_not_found(self, mock_glob: MagicMock):
        """Check exception is thrown when they aren't"""
        # Arrange
        mock_glob.return_value = {}

        test_loader = MockLidarLoader()

        # Act, Assert
        with pytest.raises(FileNotFoundError):
            _ = test_loader.get_available_folders()


def test_get_filenames_from_archive():
    """Check that the correct filenames are being retrieved from the archive"""

    @dataclass
    class MockFile:
        filename: str

    # Arrange
    mock_archive = MagicMock()
    mock_archive.filelist = [
        MockFile("file_one.other"),
        MockFile("file_two.other"),
        MockFile("file_three.tfw"),
        MockFile("file_five.other"),
        MockFile("folder/file_six.other"),
        MockFile("file_seven.tif"),
    ]

    target = ("file_seven.tif", "file_three.tfw")

    # Act
    result = LidarLoader._get_filenames_from_archive(mock_archive)

    # Assert
    assert result == target


def test_get_bbox_from_tfw():
    """Check that the contents of TFW files is being parsed correctly to
    generate bounding boxes for each file"""

    # Arrange
    test_tfw = dedent(
        """
        1.0000000000
        0.0000000000
        0.0000000000
        -1.0000000000
        445000.5000000000
        119999.5000000000
        """.strip()
    )
    test_loader = MockLidarLoader()

    target = np.array([445000, 115000, 450000, 120000])

    # Act
    result = test_loader._get_bbox_from_tfw(test_tfw)

    # Assert
    assert (result == target).all()


@patch("fell_loader.parsing.lidar_loader.rio")
@patch("fell_loader.parsing.lidar_loader.zipfile")
def test_load_lidar_and_bbox_from_folder(
    mock_zipfile: MagicMock, mock_rio: MagicMock
):
    """Check that the correct calls are being generated"""

    # Arrange #################################################################

    # Arguments to be provided
    test_lidar_dir = "lidar_dir"

    # Create handles for less verbose assertions
    mock_archive = MagicMock()
    mock_zipfile.ZipFile.return_value.__enter__.return_value = mock_archive

    mock_tif = MagicMock()
    mock_tif.read.return_value = ["lidar"]
    mock_rio.open.return_value.__enter__.return_value = mock_tif

    # Set mock output values for function calls
    mock_loader = MockLidarLoader()
    mock_loader._get_filenames_from_archive = MagicMock(
        return_value=("tif_loc", "tfw_loc")
    )
    mock_loader._get_bbox_from_tfw = MagicMock(return_value="bbox")

    # Set expected output
    tgt_lidar, tgt_bbox = "lidar", "bbox"

    # Act #####################################################################
    res_lidar, res_bbox = mock_loader.load_lidar_and_bbox_from_folder(
        test_lidar_dir
    )

    # Assert ##################################################################

    # Correct archive is opened
    mock_zipfile.ZipFile.assert_called_once_with("lidar_dir", mode="r")

    # Correct files are read
    mock_archive.open.assert_called_once_with("tif_loc")
    mock_archive.read.assert_called_once_with("tfw_loc")

    # Decoded TFW is transformed to bbox
    mock_loader._get_bbox_from_tfw.assert_called_once_with(
        mock_archive.read.return_value.decode.return_value
    )

    # Expected output is generated
    assert res_lidar == tgt_lidar
    assert res_bbox == tgt_bbox


def test_generate_file_id():
    """Check that file IDs are being generated properly"""
    # Arrange
    test_lidar_dir = "/some/folder/lidar_composite_dtm-2020-1-SU20ne.zip"
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
    target_head = pl.DataFrame(
            {
                'easting': np.array([100000, 100001, 100002, 100003, 100004]).astype('int32'),
                'northing': np.array([204999, 204999, 204999, 204999, 204999]).astype('int32'),
                'elevation': np.array([0, 1, 2, 3, 4])
            },
            orient='row'
    )
    target_tail = pl.DataFrame(
        {
            'easting': np.array([104995, 104996, 104997, 104998, 104999]).astype('int32'),
            'northing': np.array([200000, 200000, 200000, 200000, 200000]).astype('int32'),
            'elevation': np.array([24999995, 24999996, 24999997, 24999998, 24999999])
        },
        orient='row'
    )

    # fmt: on

    # Act
    result = LidarLoader.generate_df_from_lidar_array(test_lidar, test_bbox)

    result_head = result.head(5)
    result_tail = result.tail(5)

    # Assert
    assert_frame_equal(result_head, target_head)
    assert_frame_equal(result_tail, target_tail)


def test_add_file_ids():
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

    test_lidar_dir = "/some/folder/lidar_composite_dtm-2020-1-SU20ne.zip"

    mock_loader = MockLidarLoader()

    # ----- Target Data -----
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
        ['easting', 'northing', 'elevation', 'file_id', 'other'])

    test_data = [
        [0] * len(test_cols)
    ]
    # fmt: on

    test_df = pl.DataFrame(data=test_data, schema=test_cols, orient="row")

    # ----- Target Data -----=
    # fmt: off
    tgt_cols = (
        ['easting', 'northing', 'elevation', 'file_id'])

    tgt_data = [
        [0] * len(tgt_cols)
    ]
    # fmt: on

    tgt_df = pl.DataFrame(data=tgt_data, schema=tgt_cols, orient="row")

    # Act
    res_df = LidarLoader.set_output_schema(test_df)

    # Assert
    assert_frame_equal(tgt_df, res_df)


def test_parse_lidar_folder():
    """Ensure the correct calls are made, and output is as expected"""

    # Arrange #################################################################

    # Mock Loader -------------------------------------------------------------
    mock_load_lidar_and_bbox_from_folder = MagicMock(
        return_value=("lidar", "bbox")
    )

    mock_loader = MockLidarLoader()
    mock_loader.load_lidar_and_bbox_from_folder = (
        mock_load_lidar_and_bbox_from_folder
    )

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

    # Provided Arguments ------------------------------------------------------

    test_lidar_dir = "/some/folder/lidar_composite_dtm-2020-1-SU20ne.zip"

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ['easting', 'northing', 'elevation', 'file_id'])

    tgt_data = [
        [easting  , northing  , 100.0      , 'SU20ne']
    ]
    # fmt: on

    tgt_schema = {
        "easting": pl.Int32(),
        "northing": pl.Int32(),
        "elevation": pl.Float64(),
        "file_id": pl.String(),
    }

    tgt_df = pl.DataFrame(data=tgt_data, schema=tgt_schema, orient="row")

    # Act #####################################################################

    result_df = mock_loader.parse_lidar_folder(test_lidar_dir)

    # Assert ##################################################################
    mock_load_lidar_and_bbox_from_folder.assert_called_once_with(
        test_lidar_dir
    )
    mock_generate_df_from_lidar_array.assert_called_once()

    assert_frame_equal(result_df, tgt_df)


def test_write_df_to_parquet():
    """Ensure the correct calls are made when writing data"""

    # Arrange
    mock_write_parquet = MagicMock()
    mock_df = MagicMock()
    mock_df.write_parquet = mock_write_parquet

    mock_loader = MockLidarLoader()

    target_args = ["data_dir/parsed/lidar"]
    target_kwargs = {
        "use_pyarrow": True,
        "pyarrow_options": {
            "partition_cols": ["file_id"],
            "compression": "snappy",
        },
    }

    # Act
    mock_loader.write_df_to_parquet(mock_df)

    # Assert
    mock_write_parquet.assert_called_once_with(*target_args, **target_kwargs)


@patch("fell_loader.parsing.lidar_loader.os.path.exists")
class TestProcessLidarFile:
    """Make sure lidar files are being processed as-expected"""

    @patch("fell_loader.parsing.lidar_loader.open")
    def test_processing_success(
        self, mock_open: MagicMock, mock_exists: MagicMock
    ):
        """Happy path, no issues while processing"""

        # Arrange #############################################################

        # Provided Arguments --------------------------------------------------

        test_lidar_dir = "/some/folder/lidar_composite_dtm-2020-1-SU20ne.zip"

        # Mock Loader ---------------------------------------------------------

        mock_loader = MockLidarLoader()
        mock_loader.generate_file_id = MagicMock(return_value="file_id")
        mock_loader.parse_lidar_folder = MagicMock(return_value="lidar_df")
        mock_loader.write_df_to_parquet = MagicMock()

        # Other ---------------------------------------------------------------
        mock_exists.return_value = False

        # Act #################################################################

        mock_loader.process_lidar_file(test_lidar_dir)

        # Assert ##############################################################

        # Correct file was polled
        mock_exists.assert_called_once_with(
            "data_dir/parsed/lidar/file_id=file_id"
        )

        # Correct calls were generated
        mock_loader.parse_lidar_folder.assert_called_once_with(test_lidar_dir)
        mock_loader.write_df_to_parquet.assert_called_once_with("lidar_df")

        # Nothing was written to bad_files.txt
        mock_open.assert_not_called()

    def test_file_already_processed(self, mock_exists: MagicMock):
        """No action if file has already been processed"""

        # ----- Arrange -----
        mock_loader = MockLidarLoader()
        mock_loader.generate_file_id = MagicMock(return_value="file_id")
        mock_loader.parse_lidar_folder = MagicMock()

        mock_exists.return_value = True

        # ----- Act -----
        result = mock_loader.process_lidar_file("lidar_dir")

        # ----- Assert -----

        # Nothing was returned
        assert result is None

        # Function exited early
        mock_loader.parse_lidar_folder.assert_not_called()

    @patch("fell_loader.parsing.lidar_loader.open")
    def test_processing_failure(
        self, mock_open: MagicMock, mock_exists: MagicMock
    ):
        """Unhappy path, non-standard file"""

        # Arrange #############################################################

        # Provided Arguments --------------------------------------------------

        test_lidar_dir = "/some/folder/lidar_composite_dtm-2020-1-SU20ne.zip"

        # Mock Loader ---------------------------------------------------------

        mock_loader = MockLidarLoader()
        mock_loader.generate_file_id = MagicMock(return_value="file_id")
        mock_loader.parse_lidar_folder = MagicMock(
            side_effect=pl.exceptions.ShapeError
        )
        mock_loader.write_df_to_parquet = MagicMock()

        # Mock File Writer ----------------------------------------------------
        mock_write = MagicMock()
        mock_open.return_value.__enter__.return_value.write = mock_write

        # Other ---------------------------------------------------------------
        mock_exists.return_value = False

        # Act #################################################################

        mock_loader.process_lidar_file(test_lidar_dir)

        # Assert ##############################################################

        # Correct file was polled
        mock_exists.assert_called_once_with(
            "data_dir/parsed/lidar/file_id=file_id"
        )

        # Attempted file parsing
        mock_loader.parse_lidar_folder.assert_called_once_with(test_lidar_dir)

        # No parquet files generated
        mock_loader.write_df_to_parquet.assert_not_called()

        # Log file was opened, correct contents written
        mock_open.assert_called_once_with("bad_files.txt", "a")
        mock_write.assert_called_once_with(f"{test_lidar_dir}\n")


@patch("fell_loader.parsing.lidar_loader.process_map")
def test_load(mock_process_map: MagicMock):
    """Ensure the correct info is passed to process_map"""

    # Arrange
    mock_loader = MockLidarLoader()

    mock_process_lidar_file = MagicMock()
    mock_loader.process_lidar_file = mock_process_lidar_file

    # Act
    mock_loader.load()

    # Assert
    mock_process_map.assert_called_once_with(
        mock_process_lidar_file,
        {"file_1"},  # Set as part of MockLidarLoader definition
        desc="Parsing LIDAR data",
        chunksize=1,
    )
