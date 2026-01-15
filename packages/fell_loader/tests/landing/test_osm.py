"""Tests for `fell_loader.landing.osm`"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fell_loader.landing.osm import OsmLoader
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)
from pyspark.testing import assertDataFrameEqual


class MockOsmLoader(OsmLoader):
    """Mock implementation of the OSM loader class, uses static values instead
    of fetching info from environment variables
    """

    def __init__(self) -> None:
        self.data_dir = Path("data_dir")
        self.binary_loc = Path("binary_loc")
        self.skip_load = False
        self.spark = MagicMock()


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
        res_df = OsmLoader.map_to_schema(test_df, test_schema, cast=True)

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
        res_df = OsmLoader.map_to_schema(test_df, test_schema, cast=False)

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

        test_loader = MockOsmLoader()

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

        test_loader = MockOsmLoader()

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

        test_loader = MockOsmLoader()

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

        test_loader = MockOsmLoader()

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

        test_loader = MockOsmLoader()

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

        test_loader = MockOsmLoader()

        test_layer = "landing"
        test_dataset = "nodes"

        # Act, Assert
        with pytest.raises(FileNotFoundError):
            _ = test_loader.read_delta(test_layer, test_dataset)

        # Assert
        mock_deltatable.forPath.assert_not_called()


def test_drop_unused_nodes(test_session: SparkSession):
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
    res_df = OsmLoader.drop_unused_nodes(test_nodes_df, test_edges_df)

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


# MARK: Class Setup


class TestSetParquetLocs:
    """Make sure the locations of parquet files to be read are being generated
    properly
    """

    @patch("pathlib.Path.glob", autospec=True)
    def test_files_found(self, mock_glob: MagicMock):
        """If files are found, they should be saved down"""
        # Arrange
        mock_glob.side_effect = lambda self, pattern: iter(
            [self.as_posix() + pattern.replace("*", "/some_file")]
        )

        tgt_nodes_loc = "data_dir/extracts/osm/some_file.node.parquet"
        tgt_ways_loc = "data_dir/extracts/osm/some_file.way.parquet"

        test_loader = MockOsmLoader()

        # Act
        test_loader._set_parquet_locs()

        # Assert
        assert test_loader.nodes_loc == tgt_nodes_loc
        assert test_loader.ways_loc == tgt_ways_loc

    @patch("pathlib.Path.glob", autospec=True)
    def test_files_not_found(self, mock_glob: MagicMock):
        """If files are found, they should be saved down"""
        # Arrange
        mock_glob.return_value = iter([])
        test_loader = MockOsmLoader()

        # Act, Assert
        with pytest.raises(FileNotFoundError):
            test_loader._set_parquet_locs()


# MARK: File Unpacking


@patch("pathlib.Path.glob", autospec=True)
class TestUnpackOsmPbf:
    """Make sure the correct calls are generated in order to unpack
    OSM data into parquet files
    """

    def test_no_data(self, mock_glob: MagicMock):
        """If no data is found, an exception should be raised"""
        # Arrange
        mock_glob.return_value = iter([])
        test_loader = MockOsmLoader()

        # Act, Assert
        with pytest.raises(FileNotFoundError):
            test_loader._unpack_osm_pbf()

    def test_multiple_files(self, mock_glob: MagicMock):
        """If multiple files are found, an exception should be raised"""
        # Arrange
        mock_glob.return_value = iter([Path("file_1"), Path("file_2")])
        test_loader = MockOsmLoader()

        # Act, Assert
        with pytest.raises(ValueError):
            test_loader._unpack_osm_pbf()

    @patch("pathlib.Path.write_text", autospec=True)
    @patch("pathlib.Path.exists", autospec=True)
    @patch("fell_loader.landing.osm.os")
    def test_first_file(
        self,
        mock_os: MagicMock,
        mock_exists: MagicMock,
        mock_write_text: MagicMock,
        mock_glob: MagicMock,
    ):
        """If no files have previously been loaded, the map data should be
        unpacked
        """
        # Arrange
        # single osm file present
        mock_glob.return_value = iter([Path("file_1")])

        # no files previously loaded
        mock_exists.return_value = False

        # Create loader instance
        test_loader = MockOsmLoader()
        test_loader._set_parquet_locs = MagicMock()

        # Act
        test_loader._unpack_osm_pbf()

        # Assert
        assert not test_loader.skip_load
        mock_os.system.assert_called_once_with("java -jar binary_loc file_1")
        mock_write_text.assert_called_once_with(
            Path("data_dir/landing/latest_osm.txt"),  # self
            "file_1",  # text to write
        )

    @patch("pathlib.Path.read_text", autospec=True)
    @patch("pathlib.Path.write_text", autospec=True)
    @patch("pathlib.Path.exists", autospec=True)
    @patch("fell_loader.landing.osm.os")
    def test_new_file(
        self,
        mock_os: MagicMock,
        mock_exists: MagicMock,
        mock_write_text: MagicMock,
        mock_read_text: MagicMock,
        mock_glob: MagicMock,
    ):
        """If a new file is found, it should be unpacked"""
        # Arrange
        # single osm file present
        mock_glob.return_value = iter([Path("file_1")])

        # some other file previously loaded
        mock_exists.return_value = True
        mock_read_text.return_value = "file_2"

        # Create loader instance
        test_loader = MockOsmLoader()
        test_loader._set_parquet_locs = MagicMock()

        # Act
        test_loader._unpack_osm_pbf()

        # Assert
        assert not test_loader.skip_load
        mock_os.system.assert_called_once_with("java -jar binary_loc file_1")
        mock_write_text.assert_called_once_with(
            Path("data_dir/landing/latest_osm.txt"),  # self
            "file_1",  # text to write
        )

    @patch("pathlib.Path.read_text", autospec=True)
    @patch("pathlib.Path.exists", autospec=True)
    @patch("fell_loader.landing.osm.os")
    def test_old_file(
        self,
        mock_os: MagicMock,
        mock_exists: MagicMock,
        mock_read_text: MagicMock,
        mock_glob: MagicMock,
    ):
        """If the detected file has already been loaded, it should not be
        processed again
        """
        # Arrange
        # single osm file present
        mock_glob.return_value = iter([Path("file_1")])

        # some other file previously loaded
        mock_exists.return_value = True
        mock_read_text.return_value = "file_1"

        # Create loader instance
        test_loader = MockOsmLoader()
        test_loader._set_parquet_locs = MagicMock()

        # Act
        test_loader._unpack_osm_pbf()

        # Assert
        assert test_loader.skip_load
        mock_os.system.assert_not_called()


class TestReadOsmData:
    """Make sure the right data is being read in, and data is extracted
    on-demand
    """

    def test_no_skip_load(self):
        """If the skip_load flag is not set, the data should be read in"""
        # Arrange
        test_loader = MockOsmLoader()
        test_loader._set_parquet_locs = MagicMock()
        test_loader._unpack_osm_pbf = MagicMock()
        test_loader.skip_load = False
        test_loader.nodes_loc = Path("data_dir/nodes")
        test_loader.ways_loc = Path("data_dir/ways")
        test_loader.spark = MagicMock()
        test_loader.spark.read.parquet.side_effect = lambda x: f"{x}_df"

        # Act
        res_nodes, res_ways = test_loader.read_osm_data()

        # Assert
        assert test_loader.spark.read.parquet.call_count == 2
        assert res_nodes == "data_dir/nodes_df"
        assert res_ways == "data_dir/ways_df"

    def test_skip_load(self):
        """If the skip_load flag is set, no action should be taken"""
        # Arrange
        test_loader = MockOsmLoader()
        test_loader._set_parquet_locs = MagicMock()
        test_loader._unpack_osm_pbf = MagicMock()
        test_loader.skip_load = True
        test_loader.spark = MagicMock()
        test_loader.nodes_loc = Path("data_dir/nodes")
        test_loader.ways_loc = Path("data_dir/ways")
        test_loader.spark.read.parquet.side_effect = lambda x: f"{x}_df"

        # Act
        res_nodes, res_ways = test_loader.read_osm_data()

        # Assert
        test_loader.spark.read.parquet.assert_not_called()
        assert res_nodes is None
        assert res_ways is None


# MARK: Node Parsing


@patch("fell_loader.landing.osm.WGS84toOSGB36")
def test_assign_bng_coords(
    mock_wgs84_to_osgb36: MagicMock, test_session: SparkSession
):
    """Check that records are correctly being assigned BNG coordinates based
    on their latitude & longitude
    """
    # Arrange #################################################################

    # ----- Test Data -----

    # fmt: off
    #   inx, latitude, longitude
    test_data = [
        [0 , 50.0    , 10.0],
        [1 , 100.0   , 20.0],
        [2 , 66.6    , 33.3]
    ]
    # fmt: on

    test_schema = StructType(
        [
            StructField("inx", IntegerType()),
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType()),
        ]
    )

    test_df = test_session.createDataFrame(test_data, test_schema)

    # ----- Mocks -----

    mock_wgs84_to_osgb36.side_effect = lambda x, y: (x / 2, y / 2)

    # ----- Target Data -----

    # fmt: off
    #   inx, latitude, longitude, easting, northing
    tgt_data = [
        [0 , 50.0    , 10.0     , 25     , 5],
        [1 , 100.0   , 20.0     , 50     , 10],
        [2 , 66.6    , 33.3     , 33     , 16]
    ]
    # fmt: on

    tgt_schema = StructType(
        [
            StructField("inx", IntegerType()),
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType()),
            StructField("easting", IntegerType()),
            StructField("northing", IntegerType()),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, tgt_schema)

    # Act #####################################################################
    res_df = MockOsmLoader.assign_bng_coords(test_df)

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


def test_rename_coord_fields(test_session: SparkSession):
    """Make sure fields are being renamed properly"""
    # Arrange #################################################################

    # ----- Test Data -----

    test_cols = [
        "latitude",
        "longitude",
        "other",
    ]

    test_data = [[0 for _ in test_cols]]

    test_df = test_session.createDataFrame(test_data, test_cols)

    # ----- Target Data -----

    tgt_cols = [
        "lat",
        "lon",
        "other",
    ]

    tgt_data = [[0 for _ in tgt_cols]]

    tgt_df = test_session.createDataFrame(tgt_data, tgt_cols)

    # Act #####################################################################
    res_df = OsmLoader.rename_coord_fields(test_df)

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


# MARK: Edge Parsing


def test_unpack_tags(test_session: SparkSession):
    """Make sure tags are being processed properly"""
    # Arrange #################################################################

    # ----- Test Data -----

    # fmt: off
    #   inx, tags
    test_data = [
        [0 , [{"key": b"tag_1", "value": b"value_1"}, {"key": b"tag_2", "value": b"value_2"}]],
        [1 , [{"key": b"tag_3", "value": b"value_3"}]],
        [2 , []],
    ]
    # fmt: on

    test_schema = StructType(
        [
            StructField("inx", IntegerType()),
            StructField(
                "tags",
                ArrayType(
                    StructType(
                        [
                            StructField("key", BinaryType()),
                            StructField("value", BinaryType()),
                        ]
                    )
                ),
            ),
        ]
    )

    test_df = test_session.createDataFrame(test_data, test_schema)

    # ----- Target Data -----

    # fmt: off
    #   inx, tags
    tgt_data = [
        [0 , {"tag_1": "value_1", "tag_2": "value_2"}],
        [1 , {"tag_3": "value_3"}],
        [2 , {}],
    ]
    # fmt: on

    tgt_schema = StructType(
        [
            StructField("inx", IntegerType()),
            StructField("tags", MapType(StringType(), StringType())),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, tgt_schema)

    # Act #####################################################################
    res_df = OsmLoader.unpack_tags(test_df)

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


def test_get_roads_and_paths(test_session: SparkSession):
    """Make sure filtering out of non-traversible ways has been set up
    properly
    """
    # Arrange #################################################################

    # ----- Test Data -----

    # fmt: off
    #   inx, tags
    test_data = [
        [0 , {'highway': 'some'}],
        [1 , {}],
    ]
    # fmt: on

    test_schema = StructType(
        [
            StructField("inx", IntegerType()),
            StructField("tags", MapType(StringType(), StringType())),
        ]
    )

    test_df = test_session.createDataFrame(test_data, schema=test_schema)

    test_loader = MockOsmLoader()

    # ----- Target Data -----
    # fmt: off
    #   inx, tags
    tgt_data = [
        [0 , {'highway': 'some'}],
    ]
    # fmt: on

    tgt_schema = StructType(
        [
            StructField("inx", IntegerType()),
            StructField("tags", MapType(StringType(), StringType())),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, schema=tgt_schema)

    # Act #####################################################################
    res_df = test_loader.get_roads_and_paths(test_df)

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


def test_generate_edges(test_session: SparkSession):
    """Make sure ways are correctly converted into edges"""
    # Arrange #################################################################

    # ----- Test Data -----

    # fmt: off
    #   id, tags    , timestamp, nodes
    test_data = [
        [0, 'tags_0', 'ts_0'   , [{"index": 1, "nodeId": 11}, {"index": 2, "nodeId": 12}, {"index": 3, "nodeId": 13}]],
        [1, 'tags_1', 'ts_1'   , [{"index": 1, "nodeId": 21}, {"index": 2, "nodeId": 22}]],
    ]
    # fmt: on

    test_schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("tags", StringType()),
            StructField("timestamp", StringType()),
            StructField(
                "nodes",
                ArrayType(
                    StructType(
                        [
                            StructField("index", IntegerType()),
                            StructField("nodeId", LongType()),
                        ]
                    )
                ),
            ),
        ]
    )

    test_df = test_session.createDataFrame(test_data, schema=test_schema)

    # ----- Target Data -----

    # fmt: off
    #   way_id, way_inx, src, dst, tags    , timestamp
    tgt_data = [
        [0    , 1      , 11 , 12 , "tags_0", "ts_0"],
        [0    , 2      , 12 , 13 , "tags_0", "ts_0"],
        [1    , 1      , 21 , 22 , "tags_1", "ts_1"],
    ]
    # fmt: on

    tgt_schema = StructType(
        [
            StructField("way_id", IntegerType()),
            StructField("way_inx", IntegerType()),
            StructField("src", LongType()),
            StructField("dst", LongType()),
            StructField("tags", StringType()),
            StructField("timestamp", StringType()),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, schema=tgt_schema)

    # Act #####################################################################
    res_df = OsmLoader.generate_edges(test_df)

    # Assert ##################################################################
    assertDataFrameEqual(
        res_df.select(*sorted(res_df.columns)),
        tgt_df.select(*sorted(tgt_df.columns)),
    )


def test_get_edge_start_coords(test_session: SparkSession):
    """Check that node details are joining on correctly"""
    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # ----- Edges -----
    # fmt: off
    #   src, dst, other_edges
    test_edge_data = [
        # Left only
        [0  , 1, 'other_edges'],
        # Right only
        # Both
        [4  , 5, 'other_edges']
    ]
    # fmt: on

    test_edge_schema = StructType(
        [
            StructField("src", IntegerType()),
            StructField("dst", IntegerType()),
            StructField("other_edges", StringType()),
        ]
    )

    test_edge_df = test_session.createDataFrame(
        test_edge_data, schema=test_edge_schema
    )

    # ----- Nodes -----
    # fmt: off
    #   id, lat, lon, easting, northing, easting_ptn, northing_ptn, other_nodes
    test_node_data = [
        # Left only
        # Right only
        [2, 2  , 2  , 2      , 2       , 'other_nodes'],
        [3, 3  , 3  , 3      , 3       , 'other_nodes'],
        # Both
        [4, 4  , 4  , 4      , 4       , 'other_nodes'],
        [5, 5  , 5  , 5      , 5       , 'other_nodes'],
    ]
    # fmt: on

    test_node_schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("lat", IntegerType()),
            StructField("lon", IntegerType()),
            StructField("easting", IntegerType()),
            StructField("northing", IntegerType()),
            StructField("other_nodes", StringType()),
        ]
    )

    test_node_df = test_session.createDataFrame(
        test_node_data, schema=test_node_schema
    )

    # Target Data -------------------------------------------------------------
    # fmt: off
    #   src, dst, other_edges  , src_lat, src_lon, src_easting, src_northing
    tgt_data = [
        # Left only
        # Right only
        # Both
        [4 , 5  , 'other_edges', 4      , 4      , 4          , 4]
    ]
    # fmt: on

    tgt_schema = StructType(
        [
            StructField("src", IntegerType()),
            StructField("dst", IntegerType()),
            StructField("other_edges", StringType()),
            StructField("src_lat", IntegerType()),
            StructField("src_lon", IntegerType()),
            StructField("src_easting", IntegerType()),
            StructField("src_northing", IntegerType()),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, schema=tgt_schema)

    # Act #####################################################################
    res_df = OsmLoader.get_edge_start_coords(test_node_df, test_edge_df)

    # Assert ##################################################################
    assertDataFrameEqual(tgt_df, res_df)


def test_get_edge_end_coords(test_session: SparkSession):
    """Check that node details are joining on correctly"""
    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # ----- Edges -----
    # fmt: off
    #   src, dst, other_edges
    test_edge_data = [
        # Left only
        [0 , 1  , 'other_edges'],
        # Right only
        # Both
        [4 , 5  , 'other_edges']
    ]
    # fmt: on

    test_edge_schema = StructType(
        [
            StructField("src", IntegerType()),
            StructField("dst", IntegerType()),
            StructField("other_edges", StringType()),
        ]
    )

    test_edge_df = test_session.createDataFrame(
        test_edge_data, schema=test_edge_schema
    )

    # ----- Nodes -----
    # fmt: off
    #   id, lat, lon, easting, northing, easting_ptn, northing_ptn, other_nodes
    test_node_data = [
        # Right only
        [2, 2  , 2  , 2      , 2       , 'other_nodes'],
        [3, 3  , 3  , 3      , 3       , 'other_nodes'],
        # Both
        [4, 4  , 4  , 4      , 4       , 'other_nodes'],
        [5, 5  , 5  , 5      , 5       , 'other_nodes'],
    ]
    # fmt: on

    test_node_schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("lat", IntegerType()),
            StructField("lon", IntegerType()),
            StructField("easting", IntegerType()),
            StructField("northing", IntegerType()),
            StructField("other_nodes", StringType()),
        ]
    )

    test_node_df = test_session.createDataFrame(
        test_node_data, schema=test_node_schema
    )

    # Target Data -------------------------------------------------------------
    # fmt: off
    #   src, dst, other_edges  , dst_lat, dst_lon, dst_easting, dst_northing
    tgt_data = [
        # Left only
        # Right only
        # Both
        [4 , 5  , 'other_edges', 5      , 5      , 5          , 5],
    ]
    # fmt: on

    tgt_schema = StructType(
        [
            StructField("src", IntegerType()),
            StructField("dst", IntegerType()),
            StructField("other_edges", StringType()),
            StructField("dst_lat", IntegerType()),
            StructField("dst_lon", IntegerType()),
            StructField("dst_easting", IntegerType()),
            StructField("dst_northing", IntegerType()),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, schema=tgt_schema)

    # Act #####################################################################
    res_df = OsmLoader.get_edge_end_coords(test_node_df, test_edge_df)

    # Assert ##################################################################
    assertDataFrameEqual(
        tgt_df.select(*sorted(tgt_df.columns)),
        res_df.select(*sorted(res_df.columns)),
    )


# MARK: Main Runner


@patch("fell_loader.landing.osm.shutil.rmtree")
class TestClearTempFiles:
    """Make sure temp files are being cleaned up properly"""

    def test_files_found(self, mock_rmtree: MagicMock):
        """If temp files are present, they should be removed"""
        # Arrange
        test_loader = MockOsmLoader()

        # Act
        test_loader.clear_temp_files()

        # Assert
        mock_rmtree.assert_called_once_with("data_dir/temp/nodes")

    def test_files_not_found(self, mock_rmtree: MagicMock):
        """If no temp files are present, the raised exception should be
        suppressed
        """
        # Arrange
        test_loader = MockOsmLoader()
        mock_rmtree.side_effect = FileNotFoundError

        # Act
        test_loader.clear_temp_files()

    def test_suppression_setup(self, mock_rmtree: MagicMock):
        """Make sure that suppression is definitely working by demonstrating
        that other exception types do get raised. Without this, the above
        test is not sufficient to show the expected behaviour when files are
        not found. In isolation it doesn't prove that the side effect has been
        set up properly.
        """
        # Arrange
        test_loader = MockOsmLoader()
        mock_rmtree.side_effect = ValueError

        # Act, Assert
        with pytest.raises(ValueError):
            test_loader.clear_temp_files()


@pytest.mark.skip("High effort, low value")
def test_run():
    """This needs to be built out, preferably in a way which demonstrates
    that the E2E process works as expected, rather than simply showing that
    the correct calls are being generated.
    """
