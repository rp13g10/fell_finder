"""Unit tests for the OSM loader"""

import json
from unittest.mock import MagicMock, patch

import daft
import pyarrow as pa
import pytest

from fell_loader.parsing.osm_loader import OsmLoader
from fell_loader.utils.testing import assert_frame_equal


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


@patch("fell_loader.parsing.osm_loader.daft")
class TestReadOsmData:
    """Make sure the right data is being read in, and data is extracted
    on-demand"""

    def test_data_ready(self, mock_daft: MagicMock):
        """If parquet files are present, read them directly"""

        # Arrange
        test_loader = MockOsmLoader()
        test_loader._set_parquet_locs = MagicMock()
        test_loader._unpack_osm_pbf = MagicMock()
        test_loader.nodes_loc = "nodes"
        test_loader.ways_loc = "ways"
        mock_daft.read_parquet.side_effect = lambda x: f"{x}_df"

        # Act
        res_nodes, res_ways = test_loader.read_osm_data()

        # Assert
        test_loader._set_parquet_locs.assert_called_once()
        test_loader._unpack_osm_pbf.assert_not_called()
        assert mock_daft.read_parquet.call_count == 2
        assert res_nodes == "nodes_df"
        assert res_ways == "ways_df"

    def test_data_not_ready(self, mock_daft: MagicMock):
        """If no parquet files are present, unpack the .osm.pbf file"""

        # Arrange
        test_loader = MockOsmLoader()
        test_loader._set_parquet_locs = MagicMock(
            side_effect=[FileNotFoundError, None]
        )
        test_loader._unpack_osm_pbf = MagicMock()
        test_loader.nodes_loc = "nodes"
        test_loader.ways_loc = "ways"
        mock_daft.read_parquet.side_effect = lambda x: f"{x}_df"

        # Act
        res_nodes, res_ways = test_loader.read_osm_data()

        # Assert
        assert test_loader._set_parquet_locs.call_count == 2
        test_loader._unpack_osm_pbf.assert_called_once()
        assert mock_daft.read_parquet.call_count == 2
        assert res_nodes == "nodes_df"
        assert res_ways == "ways_df"


@patch("fell_loader.parsing.osm_loader.WGS84toOSGB36")
def test_assign_bng_coords(mock_wgs84_to_osgb36: MagicMock):
    """Check that records are correctly being assigned BNG coordinates based
    on their latitude & longitude"""
    # Arrange #################################################################

    # ----- Test Data -----
    # fmt: off
    test_data = [
        {'inx': 0, 'latitude': 50.0 , 'longitude': 10.0},
        {'inx': 1, 'latitude': 100.0, 'longitude': 20.0},
        {'inx': 2, 'latitude': 66.6 , 'longitude': 33.3}
    ]
    # fmt: on

    test_schema = pa.schema(
        [
            pa.field("inx", pa.int32()),
            pa.field("latitude", pa.float64()),
            pa.field("longitude", pa.float64()),
        ]
    )

    test_df = daft.from_arrow(
        pa.Table.from_pylist(test_data, schema=test_schema)
    )

    # ----- Mocks -----

    mock_wgs84_to_osgb36.side_effect = lambda x, y: (x / 2, y / 2)

    # ----- Target Data ------

    # fmt: off
    tgt_data = [
        {'inx': 0, 'latitude': 50.0 , 'longitude': 10.0, 'easting': 25, 'northing': 5},
        {'inx': 1, 'latitude': 100.0, 'longitude': 20.0, 'easting': 50, 'northing': 10},
        {'inx': 2, 'latitude': 66.6 , 'longitude': 33.3, 'easting': 33, 'northing': 16}
    ]
    # fmt: on

    tgt_schema = pa.schema(
        [
            pa.field("inx", pa.int32()),
            pa.field("latitude", pa.float64()),
            pa.field("longitude", pa.float64()),
            pa.field("easting", pa.int32()),
            pa.field("northing", pa.int32()),
        ]
    )

    tgt_df = daft.from_arrow(pa.Table.from_pylist(tgt_data, schema=tgt_schema))

    # Act #####################################################################
    res_df = MockOsmLoader.assign_bng_coords(test_df)

    # Assert ##################################################################
    assert_frame_equal(res_df, tgt_df)


def test_set_node_output_schema():
    """Check that the correct schema is being set for the nodes dataset"""
    # Arrange #################################################################

    # ----- Test Data -----

    test_data = {
        col: [0]
        for col in [
            "id",
            "latitude",
            "longitude",
            "easting",
            "northing",
            "easting_ptn",
            "northing_ptn",
            "other",
        ]
    }

    test_df = daft.from_pydict(test_data)

    # ----- Target Data -----=
    tgt_data = {
        col: [0]
        for col in [
            "id",
            "lat",
            "lon",
            "easting",
            "northing",
            "easting_ptn",
            "northing_ptn",
        ]
    }

    tgt_df = daft.from_pydict(tgt_data)

    # Act #####################################################################
    res_df = OsmLoader.set_node_output_schema(test_df)

    # Assert ##################################################################
    assert_frame_equal(tgt_df, res_df)


def test_unpack_tags():
    """Make sure tags are being processed properly"""
    # Arrange #################################################################

    # ----- Test Data -----

    test_data = [
        {
            "inx": 0,
            "tags": [
                {"key": b"tag_1", "value": b"value_1"},
                {"key": b"tag_2", "value": b"value_2"},
            ],
        },
        {"inx": 1, "tags": [{"key": b"tag_3", "value": b"value_3"}]},
        {"inx": 2, "tags": []},
    ]

    test_schema = pa.schema(
        [
            pa.field("inx", pa.int32()),
            pa.field(
                "tags",
                pa.list_(
                    pa.struct(
                        [
                            pa.field("key", pa.binary()),
                            pa.field("value", pa.binary()),
                        ]
                    )
                ),
            ),
        ]
    )

    test_df = daft.from_arrow(
        pa.Table.from_pylist(test_data, schema=test_schema)
    )

    # ----- Target Data ------

    tgt_data = [
        {"inx": 0, "tags": '{"tag_1": "value_1", "tag_2": "value_2"}'},
        {"inx": 1, "tags": '{"tag_3": "value_3"}'},
        {"inx": 2, "tags": "{}"},
    ]

    tgt_schema = pa.schema(
        [pa.field("inx", pa.int32()), pa.field("tags", pa.string())]
    )

    tgt_df = daft.from_arrow(pa.Table.from_pylist(tgt_data, schema=tgt_schema))

    # Act #####################################################################
    res_df = OsmLoader.unpack_tags(test_df)

    # Assert ##################################################################
    assert_frame_equal(res_df, tgt_df)


class TestGetTagsAsColumn:
    """Make sure extraction of tags into columns is working properly"""

    # Arrange #################################################################

    # ----- Test Data -----

    # fmt: off
    test_data = [
        {'inx': 0, 'tags': json.dumps({'access': 'yes'})},
        {'inx': 1, 'tags': json.dumps({'access': 'no'})},
        {'inx': 2, 'tags': json.dumps({'access': 'permissive'})},
        {'inx': 3, 'tags': json.dumps({'access': 'designated'})},
        {'inx': 4, 'tags': json.dumps({'access': 'other'})},
        {'inx': 5, 'tags': json.dumps({})},
    ]
    # fmt: on

    test_schema = pa.schema(
        [
            pa.field("inx", pa.int32()),
            pa.field("tags", pa.string()),
            pa.field("access", pa.string()),
        ]
    )

    test_df = daft.from_arrow(
        pa.Table.from_pylist(test_data, schema=test_schema)
    )

    def test_without_nulls(self):
        """Test behaviour when null output is not requested"""

        # ----- Target Data -----

        # fmt: off
        tgt_data = [
            {'inx': 0, 'tags': json.dumps({'access': 'yes'})       , 'access': 'yes'},
            {'inx': 1, 'tags': json.dumps({'access': 'no'})        , 'access': 'no'},
            {'inx': 2, 'tags': json.dumps({'access': 'permissive'}), 'access': 'permissive'},
            {'inx': 3, 'tags': json.dumps({'access': 'designated'}), 'access': 'designated'},
            {'inx': 4, 'tags': json.dumps({'access': 'other'})     , 'access': 'other'},
            {'inx': 5, 'tags': json.dumps({})                      , 'access': 'null'},
        ]
        # fmt: on

        tgt_schema = pa.schema(
            [
                pa.field("inx", pa.int32()),
                pa.field("tags", pa.string()),
                pa.field("access", pa.string()),
            ]
        )

        tgt_df = daft.from_arrow(
            pa.Table.from_pylist(tgt_data, schema=tgt_schema)
        )

        # Act #####################################################################
        res_df = OsmLoader.get_tag_as_column(
            self.test_df, "access", with_nulls=False
        )

        # Assert ##################################################################
        assert_frame_equal(res_df, tgt_df)

    def test_with_nulls(self):
        """Test behaviour when null output is requested"""

        # ----- Target Data -----

        # fmt: off
        tgt_data = [
            {'inx': 0, 'tags': json.dumps({'access': 'yes'})       , 'access': 'yes'},
            {'inx': 1, 'tags': json.dumps({'access': 'no'})        , 'access': 'no'},
            {'inx': 2, 'tags': json.dumps({'access': 'permissive'}), 'access': 'permissive'},
            {'inx': 3, 'tags': json.dumps({'access': 'designated'}), 'access': 'designated'},
            {'inx': 4, 'tags': json.dumps({'access': 'other'})     , 'access': 'other'},
            {'inx': 5, 'tags': json.dumps({})                      , 'access': None},
        ]
        # fmt: on

        tgt_schema = pa.schema(
            [
                pa.field("inx", pa.int32()),
                pa.field("tags", pa.string()),
                pa.field("access", pa.string()),
            ]
        )

        tgt_df = daft.from_arrow(
            pa.Table.from_pylist(tgt_data, schema=tgt_schema)
        )

        # Act #####################################################################
        res_df = OsmLoader.get_tag_as_column(
            self.test_df, "access", with_nulls=True
        )

        # Assert ##################################################################
        assert_frame_equal(res_df, tgt_df)


def test_remove_restricted_routes():
    """Check that restricted edges are being removed properly"""
    # Arrange #################################################################

    # ----- Test Data -----

    # fmt: off
    test_data = [
        {'inx': 0, 'tags': json.dumps({'access': 'yes'})},
        {'inx': 1, 'tags': json.dumps({'access': 'no'})},
        {'inx': 2, 'tags': json.dumps({'access': 'permissive'})},
        {'inx': 3, 'tags': json.dumps({'access': 'designated'})},
        {'inx': 4, 'tags': json.dumps({'access': 'other'})},
        {'inx': 5, 'tags': json.dumps({})},
    ]
    # fmt: on

    test_schema = pa.schema(
        [
            pa.field("inx", pa.int32()),
            pa.field("tags", pa.string()),
        ]
    )

    test_df = daft.from_arrow(
        pa.Table.from_pylist(test_data, schema=test_schema)
    )

    test_loader = MockOsmLoader()

    # ----- Target Data -----=
    # fmt: off
    tgt_data = [
        {'inx': 0, 'tags': json.dumps({'access': 'yes'})},
        {'inx': 2, 'tags': json.dumps({'access': 'permissive'})},
        {'inx': 3, 'tags': json.dumps({'access': 'designated'})},
        {'inx': 5, 'tags': json.dumps({})},
    ]
    # fmt: on

    tgt_schema = pa.schema(
        [
            pa.field("inx", pa.int32()),
            pa.field("tags", pa.string()),
        ]
    )

    tgt_df = daft.from_arrow(pa.Table.from_pylist(tgt_data, schema=tgt_schema))

    # Act #####################################################################
    res_df = test_loader.remove_restricted_routes(test_df)

    # Assert ##################################################################
    assert_frame_equal(res_df, tgt_df)


def test_set_flat_flag():
    """Check that flat edges are being correctly flagged"""
    # Arrange #################################################################

    # ----- Test Data -----

    # fmt: off
    test_data = [
        {'inx': 0, 'tags': json.dumps({})},
        {'inx': 1, 'tags': json.dumps({'bridge': 'some', 'tunnel': 'some'})},
        {'inx': 2, 'tags': json.dumps({'bridge': 'some'})},
        {'inx': 3, 'tags': json.dumps({'tunnel': 'some'})},
    ]
    # fmt: on

    test_schema = pa.schema(
        [
            pa.field("inx", pa.int32()),
            pa.field("tags", pa.string()),
        ]
    )

    test_df = daft.from_arrow(
        pa.Table.from_pylist(test_data, schema=test_schema)
    )

    test_loader = MockOsmLoader()

    # ----- Target Data -----=
    # fmt: off
    tgt_data = [
        {'inx': 0, 'is_flat': False, 'tags': json.dumps({})},
        {'inx': 1, 'is_flat': True , 'tags': json.dumps({'bridge': 'some', 'tunnel': 'some'})},
        {'inx': 2, 'is_flat': True , 'tags': json.dumps({'bridge': 'some'})},
        {'inx': 3, 'is_flat': True , 'tags': json.dumps({'tunnel': 'some'})},
    ]
    # fmt: on

    tgt_schema = pa.schema(
        [
            pa.field("inx", pa.int32()),
            pa.field("is_flat", pa.bool_()),
            pa.field("tags", pa.string()),
        ]
    )

    tgt_df = daft.from_arrow(pa.Table.from_pylist(tgt_data, schema=tgt_schema))

    # Act #####################################################################
    res_df = test_loader.set_flat_flag(test_df)

    # Assert ##################################################################
    assert_frame_equal(res_df, tgt_df)


def test_set_oneway_flag():
    """Make sure the oneway flag is being set properly"""
    # Arrange #################################################################

    # ----- Test Data -----

    # fmt: off
    test_data = [
        {'inx': 0, 'tags': json.dumps({})},
        {'inx': 1, 'tags': json.dumps({'oneway': 'yes'})},
        {'inx': 2, 'tags': json.dumps({'oneway': 'no'})},
        {'inx': 3, 'tags': json.dumps({'oneway': 'other'})},
    ]
    # fmt: on

    test_schema = pa.schema(
        [
            pa.field("inx", pa.int32()),
            pa.field("tags", pa.string()),
        ]
    )

    test_df = daft.from_arrow(
        pa.Table.from_pylist(test_data, schema=test_schema)
    )

    test_loader = MockOsmLoader()

    # ----- Target Data -----=
    # fmt: off
    tgt_data = [
        {'inx': 0, 'oneway': False, 'tags': json.dumps({})},
        {'inx': 1, 'oneway': True , 'tags': json.dumps({'oneway': 'yes'})},
        {'inx': 2, 'oneway': False, 'tags': json.dumps({'oneway': 'no'})},
        {'inx': 3, 'oneway': True , 'tags': json.dumps({'oneway': 'other'})},    ]
    # fmt: on

    tgt_schema = pa.schema(
        [
            pa.field("inx", pa.int32()),
            pa.field("oneway", pa.bool_()),
            pa.field("tags", pa.string()),
        ]
    )

    tgt_df = daft.from_arrow(pa.Table.from_pylist(tgt_data, schema=tgt_schema))

    # Act #####################################################################
    res_df = test_loader.set_oneway_flag(test_df)

    # Assert ##################################################################
    assert_frame_equal(res_df, tgt_df)


def test_generate_edges():
    """Make sure ways are correctly converted into edges"""
    # Arrange #################################################################

    # ----- Test Data -----

    test_data = [
        {
            "id": 0,
            "nodes": [
                {"index": 1, "nodeId": 11},
                {"index": 2, "nodeId": 12},
                {"index": 3, "nodeId": 13},
            ],
            "tags": "way_0_tags",
        },
        {
            "id": 1,
            "nodes": [
                {"index": 1, "nodeId": 21},
                {"index": 2, "nodeId": 22},
            ],
            "tags": "way_1_tags",
        },
    ]

    test_schema = pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field(
                "nodes",
                pa.list_(
                    pa.struct(
                        [
                            pa.field("index", pa.int32()),
                            pa.field("nodeId", pa.int64()),
                        ]
                    )
                ),
            ),
        ]
    )

    test_df = daft.from_arrow(
        pa.Table.from_pylist(test_data, schema=test_schema)
    )

    # ----- Target Data ------

    # fmt: off
    tgt_data = [
        {"way_id": 0, "way_inx": 1, "src": 11, "dst": 12, "tags": "way_0_tags"},
        {"way_id": 0, "way_inx": 2, "src": 12, "dst": 13, "tags": "way_0_tags"},
        {"way_id": 1, "way_inx": 1, "src": 21, "dst": 22, "tags": "way_1_tags"},
    ]
    # fmt: off

    tgt_schema = pa.schema(
        [
            pa.field("way_id", pa.int32()),
            pa.field("way_inx", pa.int32()),
            pa.field("src", pa.int64()),
            pa.field("dst", pa.int64()),
        ]
    )

    tgt_df = daft.from_arrow(pa.Table.from_pylist(tgt_data, schema=tgt_schema))

    # Act #####################################################################
    res_df = OsmLoader.generate_edges(test_df)

    # Assert ##################################################################
    assert_frame_equal(res_df, tgt_df, order_by=['way_id', 'way_inx'])


def test_add_reverse_edges():
    """Check that reverse edges are being added correctly"""
    # Arrange #################################################################

    # ----- Test Data -----
    # fmt: off
    test_data = [
        # One way, should not be reversed
        {'way_id': 1, 'way_inx': 1, 'src': 1, 'dst': 2, 'oneway': True},
        {'way_id': 1, 'way_inx': 2, 'src': 2, 'dst': 3, 'oneway': True},
        # Bi-directional, should be reversed
        {'way_id': 2, 'way_inx': 3, 'src': 3, 'dst': 4, 'oneway': False},
        {'way_id': 2, 'way_inx': 4, 'src': 4, 'dst': 5, 'oneway': False},
    ]
    # fmt: on

    test_schema = pa.schema(
        [
            pa.field("way_id", pa.int32()),
            pa.field("way_inx", pa.int32()),
            pa.field("src", pa.int32()),
            pa.field("dst", pa.int32()),
            pa.field("oneway", pa.bool_()),
        ]
    )

    test_df = daft.from_arrow(
        pa.Table.from_pylist(test_data, schema=test_schema)
    )

    # ----- Target Data -----=
    # fmt: off
    tgt_data = [
        # One way, should not be reversed
        {'way_id': 1 , 'way_inx': 1 , 'src': 1, 'dst': 2, 'oneway': True},
        {'way_id': 1 , 'way_inx': 2 , 'src': 2, 'dst': 3, 'oneway': True},
        # Bi-directional, should be reversed
        {'way_id': 2 , 'way_inx': 3 , 'src': 3, 'dst': 4, 'oneway': False},
        {'way_id': 2 , 'way_inx': 4 , 'src': 4, 'dst': 5, 'oneway': False},
        {'way_id': -2, 'way_inx': -3, 'src': 4, 'dst': 3, 'oneway': False},
        {'way_id': -2, 'way_inx': -4, 'src': 5, 'dst': 4, 'oneway': False},
    ]
    # fmt: on

    tgt_schema = pa.schema(
        [
            pa.field("way_id", pa.int32()),
            pa.field("way_inx", pa.int32()),
            pa.field("src", pa.int32()),
            pa.field("dst", pa.int32()),
            pa.field("oneway", pa.bool_()),
        ]
    )

    tgt_df = daft.from_arrow(pa.Table.from_pylist(tgt_data, schema=tgt_schema))

    # Act #####################################################################
    res_df = OsmLoader.add_reverse_edges(test_df)

    # Assert ##################################################################
    assert_frame_equal(tgt_df, res_df, order_by=["way_id", "way_inx"])


def test_get_edge_start_coords():
    """Check that node details are joining on correctly"""
    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # ----- Edges -----
    # fmt: off
    test_edge_data = [
        # Left only
        {'src': 0, 'dst': 1, 'other_edges': 'other_edges'},
        # Right only
        # Both
        {'src': 4, 'dst': 5, 'other_edges': 'other_edges'}
    ]
    # fmt: on

    test_edge_schema = pa.schema(
        [
            pa.field("src", pa.int32()),
            pa.field("dst", pa.int32()),
            pa.field("other_edges", pa.string()),
        ]
    )

    test_edge_df = daft.from_arrow(
        pa.Table.from_pylist(test_edge_data, schema=test_edge_schema)
    )

    # ----- Nodes -----
    # fmt: off
    test_node_data = [
        # Left only
        # Right only
        {'id': 2, 'lat': 2, 'lon': 2, 'easting': 2, 'northing': 2, 'easting_ptn': 2, 'northing_ptn': 2, 'other_nodes': 'other_nodes'},
        {'id': 3, 'lat': 3, 'lon': 3, 'easting': 3, 'northing': 3, 'easting_ptn': 3, 'northing_ptn': 3, 'other_nodes': 'other_nodes'},
        # Both
        {'id': 4, 'lat': 4, 'lon': 4, 'easting': 4, 'northing': 4, 'easting_ptn': 4, 'northing_ptn': 4, 'other_nodes': 'other_nodes'},
        {'id': 5, 'lat': 5, 'lon': 5, 'easting': 5, 'northing': 5, 'easting_ptn': 5, 'northing_ptn': 5, 'other_nodes': 'other_nodes'},
    ]
    # fmt: on

    test_node_schema = pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field("lat", pa.int32()),
            pa.field("lon", pa.int32()),
            pa.field("easting", pa.int32()),
            pa.field("northing", pa.int32()),
            pa.field("easting_ptn", pa.int32()),
            pa.field("northing_ptn", pa.int32()),
            pa.field("other_nodes", pa.string()),
        ]
    )

    test_node_df = daft.from_arrow(
        pa.Table.from_pylist(test_node_data, schema=test_node_schema)
    )

    # Target Data -------------------------------------------------------------
    # fmt: off
    tgt_data = [
        # Left only
        # Right only
        # Both
        {'src': 4, 'dst': 5, 'other_edges': 'other_edges' , 'src_lat': 4, 'src_lon': 4, 'src_easting': 4, 'src_northing': 4, 'easting_ptn': 4, 'northing_ptn': 4}
    ]
    # fmt: on

    tgt_schema = pa.schema(
        [
            pa.field("src", pa.int32()),
            pa.field("dst", pa.int32()),
            pa.field("other_edges", pa.string()),
            pa.field("src_lat", pa.int32()),
            pa.field("src_lon", pa.int32()),
            pa.field("src_easting", pa.int32()),
            pa.field("src_northing", pa.int32()),
            pa.field("easting_ptn", pa.int32()),
            pa.field("northing_ptn", pa.int32()),
        ]
    )

    tgt_df = daft.from_arrow(pa.Table.from_pylist(tgt_data, schema=tgt_schema))
    # Act #####################################################################
    res_df = OsmLoader.get_edge_start_coords(test_node_df, test_edge_df)

    # Assert ##################################################################
    assert_frame_equal(tgt_df, res_df)


def test_get_edge_end_coords():
    """Check that node details are joining on correctly"""
    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # ----- Edges -----
    # fmt: off
    test_edge_data = [
        # Left only
        {'src': 0, 'dst': 1, 'other_edges': 'other_edges'},
        # Right only
        # Both
        {'src': 4, 'dst': 5, 'other_edges': 'other_edges'}
    ]
    # fmt: on

    test_edge_schema = pa.schema(
        [
            pa.field("src", pa.int32()),
            pa.field("dst", pa.int32()),
            pa.field("other_edges", pa.string()),
        ]
    )

    test_edge_df = daft.from_arrow(
        pa.Table.from_pylist(test_edge_data, schema=test_edge_schema)
    )

    # ----- Nodes -----
    # fmt: off
    test_node_data = [
        # Right only
        {'id': 2, 'lat': 2, 'lon': 2, 'easting': 2, 'northing': 2, 'easting_ptn': 2, 'northing_ptn': 2, 'other_nodes': 'other_nodes'},
        {'id': 3, 'lat': 3, 'lon': 3, 'easting': 3, 'northing': 3, 'easting_ptn': 3, 'northing_ptn': 3, 'other_nodes': 'other_nodes'},
        # Both
        {'id': 4, 'lat': 4, 'lon': 4, 'easting': 4, 'northing': 4, 'easting_ptn': 4, 'northing_ptn': 4, 'other_nodes': 'other_nodes'},
        {'id': 5, 'lat': 5, 'lon': 5, 'easting': 5, 'northing': 5, 'easting_ptn': 5, 'northing_ptn': 5, 'other_nodes': 'other_nodes'},
    ]
    # fmt: on

    test_node_schema = pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field("lat", pa.int32()),
            pa.field("lon", pa.int32()),
            pa.field("easting", pa.int32()),
            pa.field("northing", pa.int32()),
            pa.field("easting_ptn", pa.int32()),
            pa.field("northing_ptn", pa.int32()),
            pa.field("other_nodes", pa.string()),
        ]
    )

    test_node_df = daft.from_arrow(
        pa.Table.from_pylist(test_node_data, schema=test_node_schema)
    )

    # Target Data -------------------------------------------------------------
    # fmt: off
    tgt_data = [
        # Left only
        # Right only
        # Both
        {'src': 4, 'dst': 5, 'other_edges': 'other_edges', 'dst_lat': 5, 'dst_lon': 5, 'dst_easting': 5, 'dst_northing': 5},
    ]
    # fmt: on

    tgt_schema = pa.schema(
        [
            pa.field("src", pa.int32()),
            pa.field("dst", pa.int32()),
            pa.field("other_edges", pa.string()),
            pa.field("dst_lat", pa.int32()),
            pa.field("dst_lon", pa.int32()),
            pa.field("dst_easting", pa.int32()),
            pa.field("dst_northing", pa.int32()),
        ]
    )

    tgt_df = daft.from_arrow(pa.Table.from_pylist(tgt_data, schema=tgt_schema))

    # Act #####################################################################
    res_df = OsmLoader.get_edge_end_coords(test_node_df, test_edge_df)

    # Assert ##################################################################
    assert_frame_equal(tgt_df, res_df)


def test_set_edge_output_schema():
    """Check that the correct output schema is being set"""
    # Arrange #################################################################

    # ----- Test Data -----

    test_data = {
        col: [0]
        for col in [
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
            "easting_ptn",
            "northing_ptn",
            "other",
        ]
    }

    test_df = daft.from_pydict(test_data)

    # ----- Target Data -----=
    tgt_data = {
        col: [0]
        for col in [
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
            "easting_ptn",
            "northing_ptn",
        ]
    }

    tgt_df = daft.from_pydict(tgt_data)

    # Act #####################################################################
    res_df = OsmLoader.set_edge_output_schema(test_df)

    # Assert ##################################################################
    assert_frame_equal(tgt_df, res_df)


def test_write_nodes_to_parquet():
    """Make sure the correct write calls are being generated"""
    # Arrange

    test_loader = MockOsmLoader()

    mock_nodes = MagicMock()

    target_args = ["data_dir/parsed/nodes"]
    target_kwargs = {
        "partition_cols": ["easting_ptn", "northing_ptn"],
    }

    # Act
    test_loader.write_nodes_to_parquet(mock_nodes)

    # Assert
    mock_nodes.write_parquet.assert_called_once_with(
        *target_args, **target_kwargs
    )


def test_write_edges_to_parquet():
    """Make sure the correct write calls are being generated"""
    # Arrange

    test_loader = MockOsmLoader()

    mock_edges = MagicMock()

    target_args = ["data_dir/parsed/edges"]
    target_kwargs = {
        "partition_cols": ["easting_ptn", "northing_ptn"],
    }

    # Act
    test_loader.write_edges_to_parquet(mock_edges)

    # Assert
    mock_edges.write_parquet.assert_called_once_with(
        *target_args, **target_kwargs
    )


@pytest.mark.skip
def test_load():
    """This needs to be built out, preferably in a way which demonstrates
    that the E2E process works as expected, rather than simply showing that
    the correct calls are being generated."""
