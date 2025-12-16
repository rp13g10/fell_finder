"""Tests for `fell_loader.landing.lidar`"""

from dataclasses import dataclass
from pathlib import Path
from textwrap import dedent
from unittest.mock import MagicMock, patch

import numpy as np
import polars as pl
import pytest
from fell_loader.landing.lidar import (
    LidarLoader,
)
from polars.testing import assert_frame_equal


class MockLidarLoader(LidarLoader):
    """Mock implementation of the Lidar Loader which uses static values
    instead of fetching data from environment variables
    """

    def __init__(self) -> None:
        self.data_dir = Path("data_dir")
        self.to_load = {"file_one": "path_one"}
        self.dc = {}


class TestGetFileIdFromPath:
    """Ensure file path parsing is working properly"""

    def test_valid_input(self):
        """Valid paths should return the ID"""
        # Arrange
        test_path = "data_dir/LIDAR-DTM-1m-2022-SU00ne.zip"
        target = "SU00ne"

        # Act
        result = LidarLoader._get_file_id_from_path(test_path)

        # Assert
        assert result == target

    def test_invalid_input(self):
        """Invalid inputs should raise an exception"""
        # Arrange
        test_path = "data_dir/some_other_file.zip"
        target_err = "Unable to extract file ID"

        # Act, Assert
        with pytest.raises(ValueError, match=target_err):
            _ = LidarLoader._get_file_id_from_path(test_path)


class TestGetAvailableFolders:
    """Check behaviour is as-expected when searching for files"""

    @patch("pathlib.Path.glob", autospec=True)
    def test_files_found(self, mock_glob: MagicMock):
        """Check expected output format when files are found"""
        # Arrange
        mock_glob.return_value = [
            Path("data_dir/LIDAR-DTM-1m-2022-SU00ne.zip")
        ]
        test_loader = MockLidarLoader()

        target_out = {"SU00ne": "data_dir/LIDAR-DTM-1m-2022-SU00ne.zip"}
        # NOTE: First arg is `self` for instance methods
        target_glob_args = [Path("data_dir/extracts/lidar"), "*.zip"]

        # Act
        result = test_loader.get_available_folders()

        # Assert
        assert result == target_out
        mock_glob.assert_called_once_with(*target_glob_args)

    @patch("pathlib.Path.glob", autospec=True)
    def test_files_not_found(self, mock_glob: MagicMock):
        """Check exception is thrown when they aren't"""
        # Arrange
        mock_glob.return_value = []
        test_loader = MockLidarLoader()

        target_err = "No .zip files found"

        # Act, Assert
        with pytest.raises(FileNotFoundError, match=target_err):
            _ = test_loader.get_available_folders()


class TestGetLoadedFolders:
    """Make sure detection of existing folders is working properly"""

    @patch("pathlib.Path.glob", autospec=True)
    def test_files_found(self, mock_glob: MagicMock):
        """Check expected output format when files are found"""
        # Arrange
        mock_glob.return_value = [
            Path("data_dir/landing/lidar/SU00ne.parquet")
        ]
        test_loader = MockLidarLoader()

        target_out = {"SU00ne": "data_dir/landing/lidar/SU00ne.parquet"}
        # NOTE: First arg is `self` for instance methods
        target_glob_args = [Path("data_dir/landing/lidar"), "*.parquet"]

        # Act
        result = test_loader.get_loaded_folders()

        # Assert
        assert result == target_out
        mock_glob.assert_called_once_with(*target_glob_args)

    @patch("pathlib.Path.glob", autospec=True)
    def test_files_not_found(self, mock_glob: MagicMock):
        """If no files are found, expect an empty dict (no exceptions)"""
        # Arrange
        mock_glob.return_value = []
        test_loader = MockLidarLoader()

        target = {}

        # Act
        result = test_loader.get_loaded_folders()

        # Assert
        assert result == target


class TestInitializeOutputFolder:
    """Make sure the correct calls are being generated"""

    def test_exists(self):
        """If folder exists, don't recreate it"""
        # Arrange
        test_loader = MockLidarLoader()

        mock_dir = MagicMock()

        # 'division' used by pathlib to navigate to subfolders, ensure the same
        # object is returned when doing this
        mock_dir.__truediv__.return_value = mock_dir

        mock_dir.exists.return_value = True
        test_loader.data_dir = mock_dir

        # Act
        test_loader.initialize_output_folder()

        # Assert
        mock_dir.mkdir.assert_not_called()

    def test_not_exists(self):
        """If folder doesn't exist, create it"""
        # Arrange
        test_loader = MockLidarLoader()

        mock_dir = MagicMock()
        mock_dir.__truediv__.return_value = mock_dir
        mock_dir.exists.return_value = False
        test_loader.data_dir = mock_dir

        # Act
        test_loader.initialize_output_folder()

        # Assert
        mock_dir.mkdir.assert_called_once_with(parents=True)


def test_get_folders_to_load():
    """Check that the dict of folders to load is being created properly"""
    # Arrange
    test_loader = MockLidarLoader()
    test_loader.get_available_folders = MagicMock(
        return_value={
            "file_one": "path_one",
            "file_two": "path_two",
            "file_three": "path_three",
        }
    )
    test_loader.get_loaded_folders = MagicMock(
        return_value={"file_three": "path_three", "file_four": "path_four"}
    )
    test_loader.initialize_output_folder = MagicMock()

    target = {
        "file_one": "path_one",
        "file_two": "path_two",
    }

    # Act
    result = test_loader.get_folders_to_load()

    # Assert
    test_loader.initialize_output_folder.assert_called_once()
    assert result == target


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


class TestGetBboxFromXml:
    """Make sure bounding boxes are being pulled through properly"""

    def test_valid_input(self):
        """Check that the contents of XML files is being parsed correctly to
        generate bounding boxes for each file
        """
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

    def test_invalid_corner(self):
        """Corner without text should raise an exception"""
        # Arrange
        test_xml = dedent(
            """
            <metadata xml:lang="en">
            <spatRepInfo>
                <Georect>
                    <cornerPts>
                        <pos Sync="TRUE"></pos>
                    </cornerPts>
                </Georect>
            </spatRepInfo>
            </metadata>
            """.strip()
        )
        test_loader = MockLidarLoader()

        # Act, Assert
        with pytest.raises(ValueError, match="boundaries not defined"):
            _ = test_loader._get_bbox_from_xml(test_xml)

    def test_missing_corner(self):
        """Missing corner should raise an exception"""
        # Arrange
        test_xml = dedent(
            """
            <metadata xml:lang="en">
            <spatRepInfo>
                <Georect>
                </Georect>
            </spatRepInfo>
            </metadata>
            """.strip()
        )
        test_loader = MockLidarLoader()

        # Act, Assert
        with pytest.raises(
            ValueError, match="unable to determine coordinates"
        ):
            _ = test_loader._get_bbox_from_xml(test_xml)


@patch("fell_loader.landing.lidar.rio")
@patch("fell_loader.landing.lidar.zipfile")
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
        ['easting', 'northing', 'elevation', 'other'])

    test_data = [
        [0] * len(test_cols)
    ]
    # fmt: on

    test_df = pl.DataFrame(data=test_data, schema=test_cols, orient="row")

    # ----- Target Data -----=
    # fmt: off
    #    easting, northing, elevation
    tgt_data = [
        [0      , 0       , 0.0]
    ]
    # fmt: on

    tgt_schema = pl.Schema(
        {
            "easting": pl.Int32(),
            "northing": pl.Int32(),
            "elevation": pl.Float64(),
        }
    )

    tgt_df = pl.DataFrame(data=tgt_data, schema=tgt_schema, orient="row")

    # Act
    res_df = LidarLoader.set_output_schema(test_df)

    # Assert
    assert_frame_equal(tgt_df, res_df)


def test_parse_lidar_folder():
    """Ensure the correct calls are made, and output is as expected"""
    # Arrange #################################################################

    # Mock Loader -------------------------------------------------------------
    mock_loader = MockLidarLoader()
    mock_loader.load_lidar_and_bbox_from_folder = MagicMock(
        return_value=("lidar", "bbox")
    )
    mock_loader.generate_df_from_lidar_array = MagicMock(
        return_value="lidar_df"
    )
    mock_loader.set_output_schema = MagicMock(return_value="output_df")

    # Provided Arguments ------------------------------------------------------
    test_lidar_dir = "lidar_dir"

    # Target ------------------------------------------------------------------
    target_df = "output_df"
    target_bbox = "bbox"

    # Act #####################################################################

    result_df, result_bbox = mock_loader.parse_lidar_folder(test_lidar_dir)

    # Assert ##################################################################
    assert result_df == target_df  # type: ignore
    assert result_bbox == target_bbox


def test_write_df_to_parquet():
    """Ensure the correct calls are made when writing data"""
    # Arrange
    mock_write_parquet = MagicMock()
    mock_df = MagicMock()
    mock_df.write_parquet = mock_write_parquet

    mock_loader = MockLidarLoader()

    test_file_id = "file_id"

    target_args = [Path("data_dir/landing/lidar/file_id.parquet")]
    target_kwargs = {"use_pyarrow": True, "compression": "snappy"}

    # Act
    mock_loader.write_df_to_parquet(test_file_id, mock_df)

    # Assert
    mock_write_parquet.assert_called_once_with(*target_args, **target_kwargs)


class TestProcessLidarFile:
    """Make sure lidar files are being processed as-expected"""

    def test_processing_success(self):
        """Happy path, no issues while processing"""
        # Arrange #############################################################

        # Provided Arguments --------------------------------------------------

        test_file_spec = (
            "file_id",
            "file_path",
        )

        # Mock Loader ---------------------------------------------------------

        mock_loader = MockLidarLoader()
        mock_loader.parse_lidar_folder = MagicMock(
            return_value=("lidar_df", "bbox")
        )
        mock_loader.write_df_to_parquet = MagicMock()

        # Target --------------------------------------------------------------
        target_dc = {"file_id": "bbox"}

        # Act #################################################################

        mock_loader.process_lidar_file(test_file_spec)

        # Assert ##############################################################

        # Correct calls were generated
        mock_loader.parse_lidar_folder.assert_called_once_with("file_path")
        mock_loader.write_df_to_parquet.assert_called_once_with(
            "file_id", "lidar_df"
        )
        assert mock_loader.dc == target_dc

    @patch("fell_loader.landing.lidar.logger")
    def test_processing_failure(self, mock_logger: MagicMock):
        """Unhappy path, non-standard file"""
        # Arrange #############################################################

        # Provided Arguments --------------------------------------------------

        test_file_spec = (
            "file_id",
            "file_path",
        )

        # Mock Loader ---------------------------------------------------------

        mock_loader = MockLidarLoader()
        mock_loader.parse_lidar_folder = MagicMock(
            side_effect=pl.exceptions.ShapeError
        )
        mock_loader.write_df_to_parquet = MagicMock()

        # Act #################################################################

        mock_loader.process_lidar_file(test_file_spec)

        # Assert ##############################################################

        # Attempted file parsing
        mock_loader.parse_lidar_folder.assert_called_once_with("file_path")

        # No parquet files generated
        mock_loader.write_df_to_parquet.assert_not_called()

        # Invalid file was logged
        mock_logger.warning.assert_called_once_with(
            "Unable to parse file file_path"
        )


@patch("fell_loader.landing.lidar.pl.read_parquet")
class TestGetUpdatedBounds:
    """Make sure bounds are being written correctly and the presence/absence
    of various files is handled gracefully
    """

    SCHEMA = pl.Schema(
        {
            "file_id": pl.String(),
            "min_easting": pl.Int32(),
            "min_northing": pl.Int32(),
            "max_easting": pl.Int32(),
            "max_northing": pl.Int32(),
        }
    )

    def test_old_and_new_present(self, mock_read_parquet: MagicMock):
        """Output combines both inputs"""
        # Arrange #############################################################

        # Loader --------------------------------------------------------------

        test_loader = MockLidarLoader()

        # New -----------------------------------------------------------------
        test_loader.dc = {"file_one": (0, 1, 2, 3), "file_two": (4, 5, 6, 7)}

        # Old -----------------------------------------------------------------
        mock_read_parquet.return_value = pl.from_dict(
            {
                "file_id": ["file_two", "file_three"],
                "min_easting": [4, 8],
                "min_northing": [5, 9],
                "max_easting": [6, 10],
                "max_northing": [7, 11],
            },
            schema=self.SCHEMA,
        )

        # Target --------------------------------------------------------------

        target = pl.from_dict(
            {
                "file_id": ["file_one", "file_two", "file_three"],
                "min_easting": [0, 4, 8],
                "min_northing": [1, 5, 9],
                "max_easting": [2, 6, 10],
                "max_northing": [3, 7, 11],
            },
            schema=self.SCHEMA,
        )

        # Act #################################################################
        result = test_loader.get_updated_bounds()

        # Assert ##############################################################
        assert_frame_equal(result, target)  # type: ignore

    def test_old_present(self, mock_read_parquet: MagicMock):
        """Output combines both inputs"""
        # Arrange #############################################################

        # Loader --------------------------------------------------------------

        test_loader = MockLidarLoader()

        # New -----------------------------------------------------------------
        test_loader.dc = {}

        # Old -----------------------------------------------------------------
        mock_read_parquet.return_value = pl.from_dict(
            {
                "file_id": ["file_two", "file_three"],
                "min_easting": [4, 8],
                "min_northing": [5, 9],
                "max_easting": [6, 10],
                "max_northing": [7, 11],
            },
            schema=self.SCHEMA,
        )

        # Act #################################################################
        result = test_loader.get_updated_bounds()

        # Assert ##############################################################
        assert result is None
        mock_read_parquet.assert_not_called()

    def test_new_present(self, mock_read_parquet: MagicMock):
        """No data written"""
        # Arrange #############################################################

        # Loader --------------------------------------------------------------

        test_loader = MockLidarLoader()

        # New -----------------------------------------------------------------
        test_loader.dc = {"file_one": (0, 1, 2, 3), "file_two": (4, 5, 6, 7)}

        # Old -----------------------------------------------------------------
        mock_read_parquet.side_effect = FileNotFoundError

        # Target --------------------------------------------------------------

        target = pl.from_dict(
            {
                "file_id": ["file_one", "file_two"],
                "min_easting": [0, 4],
                "min_northing": [1, 5],
                "max_easting": [2, 6],
                "max_northing": [3, 7],
            },
            schema=self.SCHEMA,
        )

        # Act #################################################################
        result = test_loader.get_updated_bounds()

        # Assert ##############################################################
        assert_frame_equal(result, target)  # type: ignore


def test_write_bounds():
    """Data written to the correct location"""
    # Arrange
    test_loader = MockLidarLoader()
    test_bounds = MagicMock()

    # Act
    test_loader.write_bounds(test_bounds)

    # Assert
    test_bounds.write_parquet.assert_called_once_with(
        Path("data_dir/landing/lidar_bounds.parquet")
    )


@patch("fell_loader.landing.lidar.MAX_WORKERS", new=42)
@patch("fell_loader.landing.lidar.process_map")
def test_run(mock_process_map: MagicMock):
    """Ensure the correct info is passed to process_map"""
    # Arrange
    mock_loader = MockLidarLoader()

    mock_process_lidar_file = MagicMock()
    mock_loader.process_lidar_file = mock_process_lidar_file

    # Act
    mock_loader.run()

    # Assert
    mock_process_map.assert_called_once_with(
        mock_process_lidar_file,
        {
            "file_one": "path_one"
        }.items(),  # Set as part of MockLidarLoader definition
        desc="Parsing LIDAR data",
        chunksize=1,
        max_workers=42,
    )
