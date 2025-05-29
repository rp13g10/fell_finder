"""Unit tests for the OSM loader"""

from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)
from pyspark.testing import assertDataFrameEqual

from fell_loader.parsing.osm_loader import OsmLoader


class MockOsmLoader(OsmLoader):
    """Mock implementation of the OSM loader class, uses static values instead
    of fetching info from environment variables"""

    def __init__(self) -> None:
        self.data_dir = "data_dir"
        self.binary_loc = "binary_loc"
        self.nodes_loc = ""
        self.ways_loc = ""


class TestSetParquetLocs:
    """Make sure the locations of parquet files to be read are being generated
    properly"""

    @patch("fell_loader.parsing.osm_loader.glob")
    def test_files_found(self, mock_glob: MagicMock):
        """If files are found, they should be saved down"""

        # Arrange
        mock_glob.side_effect = lambda x: [
            x.replace("*", "file_1"),
            x.replace("*", "file_2"),
        ]

        tgt_nodes_loc = "data_dir/extracts/osm/file_1.node.parquet"
        tgt_ways_loc = "data_dir/extracts/osm/file_1.way.parquet"

        test_loader = MockOsmLoader()

        # Act
        test_loader._set_parquet_locs()

        # Assert
        assert test_loader.nodes_loc == tgt_nodes_loc
        assert test_loader.ways_loc == tgt_ways_loc

    @patch("fell_loader.parsing.osm_loader.glob")
    def test_files_not_found(self, mock_glob: MagicMock):
        """If files are found, they should be saved down"""

        # Arrange
        mock_glob.return_value = []
        test_loader = MockOsmLoader()

        # Act, Assert
        with pytest.raises(FileNotFoundError):
            test_loader._set_parquet_locs()


@patch("fell_loader.parsing.osm_loader.glob")
class TestUnpackOsmPbf:
    """Make sure the correct calls are generated in order to unpack
    OSM data into parquet files"""

    def test_no_data(self, mock_glob: MagicMock):
        """If no data is found, an exception should be raised"""

        # Arrange
        mock_glob.return_value = []
        test_loader = MockOsmLoader()
        target_glob_call = "data_dir/extracts/osm/*.osm.pbf"

        # Act, Assert
        with pytest.raises(FileNotFoundError):
            test_loader._unpack_osm_pbf()
        mock_glob.assert_called_once_with(target_glob_call)

        # Act

        # Assert

    def test_multiple_files(self, mock_glob: MagicMock):
        """If multiple files are found, an exception should be raised"""

        # Arrange
        mock_glob.return_value = ["file_1", "file_2"]
        test_loader = MockOsmLoader()

        # Act, Assert
        with pytest.raises(RuntimeError):
            test_loader._unpack_osm_pbf()

        # Act

        # Assert

    @patch("fell_loader.parsing.osm_loader.os")
    def test_single_file(self, mock_os: MagicMock, mock_glob: MagicMock):
        """If a single file is found, it should be unpacked and the locations
        of the generated files stored internally"""

        # Arrange
        mock_glob.return_value = ["file_1"]

        test_loader = MockOsmLoader()
        test_loader._set_parquet_locs = MagicMock()

        # Act
        test_loader._unpack_osm_pbf()

        # Assert
        mock_os.system.assert_called_once_with("java -jar binary_loc file_1")
        test_loader._set_parquet_locs.assert_called_once()


class TestReadOsmData:
    """Make sure the right data is being read in, and data is extracted
    on-demand"""

    def test_data_ready(self):
        """If parquet files are present, read them directly"""

        # Arrange
        test_loader = MockOsmLoader()
        test_loader._set_parquet_locs = MagicMock()
        test_loader._unpack_osm_pbf = MagicMock()
        test_loader.spark = MagicMock()
        test_loader.nodes_loc = "nodes"
        test_loader.ways_loc = "ways"
        test_loader.spark.read.parquet.side_effect = lambda x: f"{x}_df"

        # Act
        res_nodes, res_ways = test_loader.read_osm_data()

        # Assert
        test_loader._set_parquet_locs.assert_called_once()
        test_loader._unpack_osm_pbf.assert_not_called()
        assert test_loader.spark.read.parquet.call_count == 2
        assert res_nodes == "nodes_df"
        assert res_ways == "ways_df"

    def test_data_not_ready(self):
        """If no parquet files are present, unpack the .osm.pbf file"""

        # Arrange
        test_loader = MockOsmLoader()
        test_loader._set_parquet_locs = MagicMock(
            side_effect=[FileNotFoundError, None]
        )
        test_loader._unpack_osm_pbf = MagicMock()
        test_loader.spark = MagicMock()
        test_loader.nodes_loc = "nodes"
        test_loader.ways_loc = "ways"
        test_loader.spark.read.parquet.side_effect = lambda x: f"{x}_df"

        # Act
        res_nodes, res_ways = test_loader.read_osm_data()

        # Assert
        assert test_loader._set_parquet_locs.call_count == 2
        test_loader._unpack_osm_pbf.assert_called_once()
        assert test_loader.spark.read.parquet.call_count == 2
        assert res_nodes == "nodes_df"
        assert res_ways == "ways_df"


@patch("fell_loader.parsing.osm_loader.WGS84toOSGB36")
def test_assign_bng_coords(
    mock_wgs84_to_osgb36: MagicMock, test_session: SparkSession
):
    """Check that records are correctly being assigned BNG coordinates based
    on their latitude & longitude"""
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


def test_set_node_output_schema(test_session: SparkSession):
    """Check that the correct schema is being set for the nodes dataset"""
    # Arrange #################################################################

    # ----- Test Data -----

    test_cols = [
        "id",
        "latitude",
        "longitude",
        "easting",
        "northing",
        "other",
    ]

    test_data = [[0 for _ in test_cols]]

    test_df = test_session.createDataFrame(test_data, test_cols)

    # ----- Target Data -----

    tgt_cols = [
        "id",
        "lat",
        "lon",
        "easting",
        "northing",
    ]

    tgt_data = [[0 for _ in tgt_cols]]

    tgt_df = test_session.createDataFrame(tgt_data, tgt_cols)

    # Act #####################################################################
    res_df = OsmLoader.set_node_output_schema(test_df)

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


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


def test_get_tag_as_column(test_session: SparkSession):
    """Make sure extraction of tags into columns is working properly"""

    # Arrange #################################################################

    # ----- Test Data -----

    # fmt: off
    #   inx, tags
    test_data = [
        [0, {'access': 'yes'}],
        [1, {'access': 'no'}],
        [2, {'access': 'permissive'}],
        [3, {'access': 'designated'}],
        [4, {'access': 'other'}],
        [5, {}],
    ]
    # fmt: on

    test_schema = StructType(
        [
            StructField("inx", IntegerType()),
            StructField("tags", MapType(StringType(), StringType())),
        ]
    )

    test_df = test_session.createDataFrame(test_data, test_schema)

    # ----- Target Data -----

    # fmt: off
    #   inx, tags                    , access
    tgt_data = [
        [0 , {'access': 'yes'}       , 'yes'],
        [1 , {'access': 'no'}        , 'no'],
        [2 , {'access': 'permissive'}, 'permissive'],
        [3 , {'access': 'designated'}, 'designated'],
        [4 , {'access': 'other'}     , 'other'],
        [5 , {}                      , None],
    ]
    # fmt: on

    tgt_schema = StructType(
        [
            StructField("inx", IntegerType()),
            StructField("tags", MapType(StringType(), StringType())),
            StructField("access", StringType()),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, tgt_schema)

    # Act #####################################################################
    res_df = OsmLoader.get_tag_as_column(test_df, "access")

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


def test_get_roads_and_paths(test_session: SparkSession):
    """Make sure filtering out of non-traversible ways has been set up
    properly"""
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
    #   inx, tags               , highway
    tgt_data = [
        [0 , {'highway': 'some'}, 'some'],
    ]
    # fmt: on

    tgt_schema = StructType(
        [
            StructField("inx", IntegerType()),
            StructField("tags", MapType(StringType(), StringType())),
            StructField("highway", StringType()),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, schema=tgt_schema)

    # Act #####################################################################
    res_df = test_loader.get_roads_and_paths(test_df)

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


def test_flag_explicit_footways(test_session: SparkSession):
    """Check that ways with explicit footways are being flagged correctly"""
    # Arrange #################################################################

    # ----- Test Data -----

    # fmt: off
    #   inx, tags
    test_data = [
        # Explicit yes
        [0 , {'foot': 'yes'}],
        [1 , {'sidewalk': 'yes'}],
        # Explicit no
        [2 , {'foot': 'no'}],
        # Multiple explicit yes
        [3 , {'foot': 'yes', 'sidwalk': 'yes'}],
        # Mixed
        [4 , {'foot': 'yes', 'sidewalk': 'no'}],
        # No tag set
        [4 , {}],
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
    #   inx, tags                             , explicit_footway
    tgt_data = [
        # Explicit yes
        [0 , {'foot': 'yes'}                  , True],
        [1 , {'sidewalk': 'yes'}              , True],
        # Explicit no
        [2 , {'foot': 'no'}                   , False],
        # Multiple explicit yes
        [3 , {'foot': 'yes', 'sidwalk': 'yes'}, True],
        # Mixed
        [4 , {'foot': 'yes', 'sidewalk': 'no'}, True],
        # No tag set
        [4 , {}                               , False],
    ]
    # fmt: on

    tgt_schema = StructType(
        [
            StructField("inx", IntegerType()),
            StructField("tags", MapType(StringType(), StringType())),
            StructField("explicit_footway", BooleanType()),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, schema=tgt_schema)

    # Act #####################################################################
    res_df = test_loader.flag_explicit_footways(test_df)

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


def test_remove_restricted_routes(test_session: SparkSession):
    """Check that restricted edges are being removed properly"""
    # Arrange #################################################################

    # ----- Test Data -----

    # fmt: off
    #   inx, tags                    , explicit_footway
    test_data = [
        [0 , {'access': 'yes'}       , False],
        [1 , {'access': 'no'}        , False],
        [2 , {'access': 'permissive'}, False],
        [3 , {'access': 'designated'}, False],
        [4 , {'access': 'other'}     , False],
        [5 , {}                      , False],
        [6 , {'access': 'no'}        , True],
    ]
    # fmt: on

    test_schema = StructType(
        [
            StructField("inx", IntegerType()),
            StructField("tags", MapType(StringType(), StringType())),
            StructField("explicit_footway", BooleanType()),
        ]
    )

    test_df = test_session.createDataFrame(test_data, schema=test_schema)

    test_loader = MockOsmLoader()

    # ----- Target Data -----
    # fmt: off
    #   inx, tags                    , explicit_footway
    tgt_data = [
        [0 , {'access': 'yes'}       , False],
        [2 , {'access': 'permissive'}, False],
        [3 , {'access': 'designated'}, False],
        [5 , {}                      , False],
        [6 , {'access': 'no'}        , True],
    ]
    # fmt: on

    tgt_schema = StructType(
        [
            StructField("inx", IntegerType()),
            StructField("tags", MapType(StringType(), StringType())),
            StructField("explicit_footway", BooleanType()),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, schema=tgt_schema)

    # Act #####################################################################
    res_df = test_loader.remove_restricted_routes(test_df)

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


def test_remove_unsafe_routes(test_session: SparkSession):
    """Check that unsafe roads are being removed properly"""
    # Arrange #################################################################

    # ----- Test Data -----

    # fmt: off
    #   inx, tags                      , highway   , explicit_footway
    test_data = [
        # Dropped, motorway
        [0 , {}                        , 'motorway', False],
        # Dropped, roundabout
        [1 , {'junction': 'roundabout'}, 'other'   , False],
        # Dropped, 60 mph, no footway
        [2 , {'maxspeed': '60 mph'}    , 'other'   , False],
        # Dropped, 70 mph, no footway
        [3 , {'maxspeed': '70 mph'}    , 'other'   , False],
        # Retained, 60 mph, footway
        [4 , {'maxspeed': '60 mph'}    , 'other'   , True],
        # Retained, 30 mph, no (explicit) footway
        [5 , {'maxspeed': '30 mph'}    , 'other'   , False],
        # Dropped, motorway (with speed limit tag)
        [6 , {'maxspeed': '70 mph'}    , 'motorway', False]
    ]
    # fmt: on

    test_schema = StructType(
        [
            StructField("inx", IntegerType()),
            StructField("tags", MapType(StringType(), StringType())),
            StructField("highway", StringType()),
            StructField("explicit_footway", BooleanType()),
        ]
    )

    test_df = test_session.createDataFrame(test_data, schema=test_schema)

    test_loader = MockOsmLoader()

    # ----- Target Data -----
    # fmt: off
    #   inx, tags                      , highway   , explicit_footway
    tgt_data = [
        # Dropped, motorway
        # Dropped, roundabout
        # Dropped, 60 mph, no footway
        # Dropped, 70 mph, no footway
        # Retained, 60 mph, footway
        [4 , {'maxspeed': '60 mph'}    , 'other'   , True],
        # Retained, 30 mph, no (explicit) footway
        [5 , {'maxspeed': '30 mph'}    , 'other'   , False],
        # Dropped, motorway (with speed limit tag)
    ]
    # fmt: on

    tgt_schema = StructType(
        [
            StructField("inx", IntegerType()),
            StructField("tags", MapType(StringType(), StringType())),
            StructField("highway", StringType()),
            StructField("explicit_footway", BooleanType()),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, schema=tgt_schema)

    # Act #####################################################################
    res_df = test_loader.remove_unsafe_routes(test_df)

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


def test_set_flat_flag(test_session: SparkSession):
    """Check that flat edges are being correctly flagged"""
    # Arrange #################################################################

    # ----- Test Data -----

    # fmt: off
    #   inx, tags
    test_data = [
        [0 , {}],
        [1 , {'bridge': 'some', 'tunnel': 'some'}],
        [2 , {'bridge': 'some'}],
        [3 , {'tunnel': 'some'}],
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
    #   inx, tags                               , is_flat
    tgt_data = [
        [0, {}                                  , False],
        [1, {'bridge': 'some', 'tunnel': 'some'}, True],
        [2, {'bridge': 'some'}                  , True],
        [3, {'tunnel': 'some'}                  , True],
    ]
    # fmt: on

    tgt_schema = StructType(
        [
            StructField("inx", IntegerType()),
            StructField("tags", MapType(StringType(), StringType())),
            StructField("is_flat", BooleanType()),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, schema=tgt_schema)

    # Act #####################################################################
    res_df = test_loader.set_flat_flag(test_df)

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


def test_set_oneway_flag(test_session: SparkSession):
    """Make sure the oneway flag is being set properly"""
    # Arrange #################################################################

    # ----- Test Data -----

    # fmt: off
    #   inx, tags
    test_data = [
        [0 , {}],
        [1 , {'oneway': 'yes'}],
        [2 , {'oneway': 'no'}],
        [3 , {'oneway': 'other'}],
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
    #   inx, tags               , oneway
    tgt_data = [
        [0 , {}                 , False],
        [1 , {'oneway': 'yes'}  , True],
        [2 , {'oneway': 'no'}   , False],
        [3 , {'oneway': 'other'}, True],
    ]
    # fmt: on

    tgt_schema = StructType(
        [
            StructField("inx", IntegerType()),
            StructField("tags", MapType(StringType(), StringType())),
            StructField("oneway", BooleanType()),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, schema=tgt_schema)

    # Act #####################################################################
    res_df = test_loader.set_oneway_flag(test_df)

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


def test_generate_edges(test_session: SparkSession):
    """Make sure ways are correctly converted into edges"""
    # Arrange #################################################################

    # ----- Test Data -----

    # fmt: off
    #   id, tags    , is_flat  , oneway  , highway  , surface  , nodes
    test_data = [
        [0, 'tags_0', 'is_flat', 'oneway', 'highway', 'surface', [{"index": 1, "nodeId": 11}, {"index": 2, "nodeId": 12}, {"index": 3, "nodeId": 13}]],
        [1, 'tags_1', 'is_flat', 'oneway', 'highway', 'surface', [{"index": 1, "nodeId": 21}, {"index": 2, "nodeId": 22}]],
    ]
    # fmt: on

    test_schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("tags", StringType()),
            StructField("is_flat", StringType()),
            StructField("oneway", StringType()),
            StructField("highway", StringType()),
            StructField("surface", StringType()),
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
    #   way_id, way_inx, src, dst, tags    , is_flat  , oneway  , highway  , surface
    tgt_data = [
        [0    , 1      , 11 , 12 , "tags_0", 'is_flat', 'oneway', 'highway', 'surface'],
        [0    , 2      , 12 , 13 , "tags_0", 'is_flat', 'oneway', 'highway', 'surface'],
        [1    , 1      , 21 , 22 , "tags_1", 'is_flat', 'oneway', 'highway', 'surface'],
    ]
    # fmt: on

    tgt_schema = StructType(
        [
            StructField("way_id", IntegerType()),
            StructField("way_inx", IntegerType()),
            StructField("src", LongType()),
            StructField("dst", LongType()),
            StructField("tags", StringType()),
            StructField("is_flat", StringType()),
            StructField("oneway", StringType()),
            StructField("highway", StringType()),
            StructField("surface", StringType()),
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


def test_add_reverse_edges(test_session: SparkSession):
    """Check that reverse edges are being added correctly"""
    # Arrange #################################################################

    # ----- Test Data -----
    # fmt: off
    #   way_id, way_inx, src, dst, oneway
    test_data = [
        # One way, should not be reversed
        [1    , 1      , 1  , 2  , True],
        [1    , 2      , 2  , 3  , True],
        # Bi-directional, should be reversed
        [2    , 3      , 3  , 4  , False],
        [2    , 4      , 4  , 5  , False],
    ]
    # fmt: on

    test_schema = StructType(
        [
            StructField("way_id", IntegerType()),
            StructField("way_inx", IntegerType()),
            StructField("src", IntegerType()),
            StructField("dst", IntegerType()),
            StructField("oneway", BooleanType()),
        ]
    )

    test_df = test_session.createDataFrame(test_data, schema=test_schema)

    # ----- Target Data -----
    # fmt: off
    #   way_id, way_inx, src, dst, oneway
    tgt_data = [
        # One way, should not be reversed
        [1    , 1      , 1  , 2  , True],
        [1    , 2      , 2  , 3  , True],
        # Bi-directional, should be reversed
        [2    , 3      , 3  , 4  , False],
        [2    , 4      , 4  , 5  , False],
        [-2   , -3     , 4  , 3  , False],
        [-2   , -4     , 5  , 4  , False],
    ]
    # fmt: on

    tgt_schema = StructType(
        [
            StructField("way_id", IntegerType()),
            StructField("way_inx", IntegerType()),
            StructField("src", IntegerType()),
            StructField("dst", IntegerType()),
            StructField("oneway", BooleanType()),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, schema=tgt_schema)

    # Act #####################################################################
    res_df = OsmLoader.add_reverse_edges(test_df)

    # Assert ##################################################################
    assertDataFrameEqual(tgt_df, res_df)


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


def test_set_edge_output_schema(test_session: SparkSession):
    """Check that the correct output schema is being set"""
    # Arrange #################################################################

    # ----- Test Data -----

    test_cols = [
        "src",
        "dst",
        "way_id",
        "way_inx",
        "highway",
        "surface",
        "is_flat",
        "src_lat",
        "src_lon",
        "dst_lat",
        "dst_lon",
        "src_easting",
        "src_northing",
        "dst_easting",
        "dst_northing",
        "other",
    ]

    test_data = [[0 for _ in test_cols]]

    test_df = test_session.createDataFrame(test_data, test_cols)

    # ----- Target Data -----

    tgt_cols = [
        "src",
        "dst",
        "way_id",
        "way_inx",
        "highway",
        "surface",
        "is_flat",
        "src_lat",
        "src_lon",
        "dst_lat",
        "dst_lon",
        "src_easting",
        "src_northing",
        "dst_easting",
        "dst_northing",
    ]

    tgt_data = [[0 for _ in tgt_cols]]

    tgt_df = test_session.createDataFrame(tgt_data, tgt_cols)

    # Act #####################################################################
    res_df = OsmLoader.set_edge_output_schema(test_df)

    # Assert ##################################################################
    assertDataFrameEqual(tgt_df, res_df)


def test_write_to_parquet():
    """Make sure the correct write calls are being generated"""
    # Arrange

    test_loader = MockOsmLoader()
    mock_df = MagicMock()

    test_target = "nodes"

    target_loc = "data_dir/parsed/nodes"

    # Act
    test_loader.write_to_parquet(mock_df, test_target)

    # Assert
    mock_df.write.parquet.assert_called_once_with(target_loc, mode="overwrite")


def test_read_from_parquet():
    """Make sure the correct read calls are being generated"""
    # Arrange
    test_loader = MockOsmLoader()
    test_loader.spark = MagicMock()

    test_target = "nodes"

    target_loc = "data_dir/parsed/nodes"

    # Act
    result = test_loader.read_from_parquet(test_target)

    # Assert
    test_loader.spark.read.parquet.assert_called_once_with(target_loc)
    assert result is test_loader.spark.read.parquet.return_value


@pytest.mark.skip
def test_load():
    """This needs to be built out, preferably in a way which demonstrates
    that the E2E process works as expected, rather than simply showing that
    the correct calls are being generated."""
