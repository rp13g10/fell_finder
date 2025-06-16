"""Unit tests for the LIDAR parsing script"""

from dataclasses import dataclass
from textwrap import dedent
from unittest.mock import MagicMock, patch

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
        MockFile("file_three.xml"),
        MockFile("file_five.other"),
        MockFile("folder/file_six.other"),
        MockFile("file_seven.tif"),
    ]

    target = ("file_seven.tif", "file_three.xml")

    # Act
    result = LidarLoader._get_filenames_from_archive(mock_archive)

    # Assert
    assert result == target


def test_get_bbox_from_xml():
    """Check that the contents of XML files is being parsed correctly to
    generate bounding boxes for each file"""

    # Arrange
    test_xml = dedent(
        """
        <metadata xml:lang="en">
        <Esri>TRUNCATED</Esri>
        <dataIdInfo>TRUNCATED</dataIdInfo>
        <mdLang>TRUNCATED</mdLang>
        <distInfo>TRUNCATED</distInfo>
        <mdHrLv>TRUNCATED</mdHrLv>
        <mdHrLvName Sync="TRUE">dataset</mdHrLvName>
        <refSysInfo>TRUNCATED</refSysInfo>
        <spatRepInfo>
            <Georect>
                <cellGeo>
                    <CellGeoCd Sync="TRUE" value="002" />
                </cellGeo>
                <numDims Sync="TRUE">2</numDims>
                <tranParaAv Sync="TRUE">1</tranParaAv>
                <chkPtAv Sync="TRUE">0</chkPtAv>
                <cornerPts>
                    <pos Sync="TRUE">85000.000000 4000.000000</pos>
                </cornerPts>
                <cornerPts>
                    <pos Sync="TRUE">85000.000000 5000.000000</pos>
                </cornerPts>
                <cornerPts>
                    <pos Sync="TRUE">90000.000000 5000.000000</pos>
                </cornerPts>
                <cornerPts>
                    <pos Sync="TRUE">90000.000000 4000.000000</pos>
                </cornerPts>
                <centerPt>
                    <pos Sync="TRUE">87500.000000 4500.000000</pos>
                </centerPt>
                <axisDimension type="002">
                    <dimSize Sync="TRUE">5000</dimSize>
                    <dimResol>
                        <value Sync="TRUE" uom="m">1.000000</value>
                    </dimResol>
                </axisDimension>
                <axisDimension type="001">
                    <dimSize Sync="TRUE">1000</dimSize>
                    <dimResol>
                        <value Sync="TRUE" uom="m">1.000000</value>
                    </dimResol>
                </axisDimension>
                <ptInPixel>
                    <PixOrientCd Sync="TRUE" value="001" />
                </ptInPixel>
            </Georect>
        </spatRepInfo>
        <contInfo>TRUNCATED</contInfo>
        <mdDateSt Sync="TRUE">20220909</mdDateSt>
        <dqInfo>TRUNCATED</dqInfo>
        <eainfo>TRUNCATED</eainfo>
        <dataSetURI />
    </metadata>
        """.strip()
    )
    test_loader = MockLidarLoader()

    target = np.array([85000, 4000, 90000, 5000])

    # Act
    result = test_loader._get_bbox_from_xml(test_xml)

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
        return_value=("tif_loc", "xml_loc")
    )
    mock_loader._get_bbox_from_xml = MagicMock(return_value="bbox")

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
    mock_archive.read.assert_called_once_with("xml_loc")

    # Decoded TFW is transformed to bbox
    mock_loader._get_bbox_from_xml.assert_called_once_with(
        mock_archive.read.return_value.decode.return_value
    )

    # Expected output is generated
    assert res_lidar == tgt_lidar
    assert res_bbox == tgt_bbox


class TestGenerateDfFromLidarArray:
    """Check that LIDAR data is correctly being converted into records"""

    def test_square_bbox(self):
        """Check the standard case (5000x5000 grid)"""

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
        result = LidarLoader.generate_df_from_lidar_array(
            test_lidar, test_bbox
        )

        result_head = result.head(5)
        result_tail = result.tail(5)

        # Assert
        assert_frame_equal(result_head, target_head)
        assert_frame_equal(result_tail, target_tail)

    def test_rectangular_bbox(self):
        """Check the standard case (5000x5000 grid)"""

        # Arrange
        test_bbox = np.array(
            [
                655000,  # Min easting
                295000,  # Min northing
                656000,  # Max easting
                300000,  # Max northing
            ]
        )

        # [0, 1, 2, ..., 4999999]                                              # noqa: ERA001
        test_lidar = np.array(range(0, 1000 * 5000))
        # [[0, 1, ..., 999], [1000, ..., 1999], ..., [4999000, ..., 4999999]]  # noqa: ERA001
        test_lidar = test_lidar.reshape(5000, 1000)

        # fmt: off
        target_head = pl.DataFrame(
                {
                    'easting': np.array([655000, 655001, 655002, 655003, 655004]).astype('int32'),
                    'northing': np.array([299999, 299999, 299999, 299999, 299999]).astype('int32'),
                    'elevation': np.array([0, 1, 2, 3, 4])
                },
                orient='row'
        )
        target_tail = pl.DataFrame(
            {
                'easting': np.array([655995, 655996, 655997, 655998, 655999]).astype('int32'),
                'northing': np.array([295000, 295000, 295000, 295000, 295000]).astype('int32'),
                'elevation': np.array([4999995, 4999996, 4999997, 4999998, 4999999])
            },
            orient='row'
        )

        # fmt: on

        # Act
        result = LidarLoader.generate_df_from_lidar_array(
            test_lidar, test_bbox
        )

        result_head = result.head(5)
        result_tail = result.tail(5)

        # Assert
        assert_frame_equal(result_head, target_head)
        assert_frame_equal(result_tail, target_tail)


def test_set_output_schema():
    """Check that the correct output schema is being set"""
    # Arrange #################################################################

    # ----- Test Data -----
    # fmt: off
    test_cols = (
        ['easting', 'northing', 'elevation', 'ptn', 'other'])

    test_data = [
        [0] * len(test_cols)
    ]
    # fmt: on

    test_df = pl.DataFrame(data=test_data, schema=test_cols, orient="row")

    # ----- Target Data -----=
    # fmt: off
    tgt_cols = (
        ['easting', 'northing', 'elevation', 'ptn'])

    tgt_data = [
        [0] * len(tgt_cols)
    ]
    # fmt: on

    tgt_df = pl.DataFrame(data=tgt_data, schema=tgt_cols, orient="row")

    # Act
    res_df = LidarLoader.set_output_schema(test_df)

    # Assert
    assert_frame_equal(tgt_df, res_df)


@patch("fell_loader.utils.partitioning.PTN_EDGE_SIZE_M", 10000)
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
        ['easting', 'northing', 'elevation', 'ptn'])

    tgt_data = [
        [easting  , northing  , 100.0      , '12_98']
    ]
    # fmt: on

    tgt_schema = {
        "easting": pl.Int32(),
        "northing": pl.Int32(),
        "elevation": pl.Float64(),
        "ptn": pl.String(),
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
            "partition_cols": ["ptn"],
            "compression": "snappy",
        },
    }

    # Act
    mock_loader.write_df_to_parquet(mock_df)

    # Assert
    mock_write_parquet.assert_called_once_with(*target_args, **target_kwargs)


class TestProcessLidarFile:
    """Make sure lidar files are being processed as-expected"""

    @patch("fell_loader.parsing.lidar_loader.open")
    def test_processing_success(self, mock_open: MagicMock):
        """Happy path, no issues while processing"""

        # Arrange #############################################################

        # Provided Arguments --------------------------------------------------

        test_lidar_dir = "/some/folder/lidar_composite_dtm-2020-1-SU20ne.zip"

        # Mock Loader ---------------------------------------------------------

        mock_loader = MockLidarLoader()
        mock_loader.parse_lidar_folder = MagicMock(return_value="lidar_df")
        mock_loader.write_df_to_parquet = MagicMock()

        # Act #################################################################

        mock_loader.process_lidar_file(test_lidar_dir)

        # Assert ##############################################################

        # Correct calls were generated
        mock_loader.parse_lidar_folder.assert_called_once_with(test_lidar_dir)
        mock_loader.write_df_to_parquet.assert_called_once_with("lidar_df")

        # Nothing was written to bad_files.txt
        mock_open.assert_not_called()

    @patch("fell_loader.parsing.lidar_loader.open")
    def test_processing_failure(self, mock_open: MagicMock):
        """Unhappy path, non-standard file"""

        # Arrange #############################################################

        # Provided Arguments --------------------------------------------------

        test_lidar_dir = "/some/folder/lidar_composite_dtm-2020-1-SU20ne.zip"

        # Mock Loader ---------------------------------------------------------

        mock_loader = MockLidarLoader()
        mock_loader.parse_lidar_folder = MagicMock(
            side_effect=pl.exceptions.ShapeError
        )
        mock_loader.write_df_to_parquet = MagicMock()

        # Mock File Writer ----------------------------------------------------
        mock_write = MagicMock()
        mock_open.return_value.__enter__.return_value.write = mock_write

        # Act #################################################################

        mock_loader.process_lidar_file(test_lidar_dir)

        # Assert ##############################################################

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
