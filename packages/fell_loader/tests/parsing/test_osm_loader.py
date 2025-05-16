"""Unit tests for the OSM loader"""

from unittest.mock import MagicMock, patch
from typing import Any
import pyarrow as pa
import json

import pandas as pd
import polars as pl
import daft
from pandas.testing import assert_frame_equal as pd_assert_frame_equal
from polars.testing import assert_frame_equal as pl_assert_frame_equal


from fell_loader.parsing.osm_loader import OsmLoader
from fell_loader.utils.testing import assert_frame_equal


class MockOsmLoader(OsmLoader):
    def __init__(self):
        self.data_dir = "data_dir"
        self.binary_loc = "binary_loc"
        self.nodes_loc = ""
        self.ways_loc = ""
        self.relations_loc = ""


def test_set_parquet_locs():
    raise AssertionError()


def test_unpack_osm_pbf():
    raise AssertionError()


def test_read_unpacked_data():
    raise AssertionError()


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
    res_df = MockOsmLoader.set_node_output_schema(test_df)

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
    _ = (
        ['src', 'dst', 'other_edges', 'src_lat', 'src_lon', 'src_easting', 'src_northing', 'easting_ptn', 'northing_ptn'])

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
    _ = (
        ['src', 'dst', 'other_edges'])

    test_edge_data = [
        # Left only
        [0    , 1    , 'other_edges'],
        # Right only
        # Both
        [4    , 5    , 'other_edges']
    ]
    # fmt: on

    test_edge_schema = {
        "src": pl.Int32(),
        "dst": pl.Int32(),
        "other_edges": pl.String(),
    }

    test_edge_df = pl.DataFrame(
        data=test_edge_data, schema=test_edge_schema, orient="row"
    )

    # ----- Nodes -----
    # fmt: off
    _ = (
        ['id', 'lat', 'lon', 'easting', 'northing', 'easting_ptn', 'northing_ptn', 'other_nodes'])

    test_node_data = [
        # Left only
        [0   , 0    , 0    , 0       , 0          , 0            , 0            , 'other_nodes'],
        # Right only
        [2   , 2    , 2    , 2       , 2          , 2            , 2            , 'other_nodes'],
        [3   , 3    , 3    , 3       , 3          , 3            , 3            , 'other_nodes'],
        # Both
        [4   , 4    , 4    , 4       , 4          , 4            , 4            , 'other_nodes'],
        [5   , 5    , 5    , 5       , 5          , 5            , 5            , 'other_nodes'],
    ]
    # fmt: on

    test_node_schema = {
        "id": pl.Int32(),
        "lat": pl.Int32(),
        "lon": pl.Int32(),
        "easting": pl.Int32(),
        "northing": pl.Int32(),
        "easting_ptn": pl.Int32(),
        "northing_ptn": pl.Int32(),
        "other_edges": pl.String(),
    }

    test_node_df = pl.DataFrame(
        data=test_node_data, schema=test_node_schema, orient="row"
    )

    # Target Data -------------------------------------------------------------
    # fmt: off
    _ = (
        ['src', 'dst', 'other_edges', 'dst_lat', 'dst_lon', 'dst_easting', 'dst_northing'])

    tgt_data = [
        # Left only
        # Right only
        # Both
        [4    , 5   , 'other_edges', 5         , 5        , 5           , 5]
    ]
    # fmt: on

    tgt_schema = {
        "src": pl.Int32(),
        "dst": pl.Int32(),
        "other_edges": pl.String(),
        "dst_lat": pl.Int32(),
        "dst_lon": pl.Int32(),
        "dst_easting": pl.Int32(),
        "dst_northing": pl.Int32(),
    }

    tgt_df = pl.DataFrame(data=tgt_data, schema=tgt_schema, orient="row")

    # Act #####################################################################
    res_df = OsmLoader.get_edge_end_coords(test_node_df, test_edge_df)

    res_df = res_df.sort("dst")
    tgt_df = tgt_df.sort("dst")

    # Assert ##################################################################
    pl_assert_frame_equal(tgt_df, res_df)


def test_set_edge_output_schema():
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
        "easting_ptn",
        "northing_ptn",
        "other",
    ]

    test_data = [[0] * len(test_cols)]

    test_df = pl.DataFrame(data=test_data, schema=test_cols, orient="row")

    # ----- Target Data -----=

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
        "easting_ptn",
        "northing_ptn",
    ]

    tgt_data = [[0] * len(tgt_cols)]

    tgt_df = pl.DataFrame(data=tgt_data, schema=tgt_cols, orient="row")

    # Act #####################################################################
    res_df = OsmLoader.set_edge_output_schema(test_df)

    # Assert ##################################################################
    pl_assert_frame_equal(tgt_df, res_df)


def test_write_nodes_to_parquet():
    """Make sure the correct write calls are being generated"""
    # Arrange

    test_loader = OsmLoader("data_dir")

    mock_nodes = MagicMock()

    target_args = ["data_dir/parsed/nodes"]
    target_kwargs = {
        "use_pyarrow": True,
        "pyarrow_options": {
            "partition_cols": ["easting_ptn", "northing_ptn"],
            "max_partitions": 4096,
        },
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

    test_loader = OsmLoader("data_dir")

    mock_edges = MagicMock()

    target_args = ["data_dir/parsed/edges"]
    target_kwargs = {
        "use_pyarrow": True,
        "pyarrow_options": {
            "partition_cols": ["easting_ptn", "northing_ptn"],
            "max_partitions": 4096,
        },
    }

    # Act
    test_loader.write_edges_to_parquet(mock_edges)

    # Assert
    mock_edges.write_parquet.assert_called_once_with(
        *target_args, **target_kwargs
    )


@patch("fell_loader.parsing.osm_loader.add_partitions_to_polars_df")
def test_load(mock_add_partitions_to_polars_df: MagicMock):
    """Make sure the correct methods are called. As each component is already
    tested it is not necessary to attempt to generate test/target data here."""

    # Arrange

    test_loader = OsmLoader("data_dir")

    mock_nodes = MagicMock()
    mock_edges = MagicMock()
    test_loader.read_osm_data = MagicMock(
        return_value=(mock_nodes, mock_edges)
    )

    test_loader.assign_bng_coords = MagicMock(side_effect=lambda x: x)
    mock_add_partitions_to_polars_df.side_effect = lambda x: x
    test_loader.set_node_output_schema = MagicMock(side_effect=lambda x: x)

    test_loader.tidy_edge_schema = MagicMock(side_effect=lambda x: x)
    test_loader.set_flat_flag = MagicMock(side_effect=lambda x: x)
    test_loader.add_reverse_edges = MagicMock(side_effect=lambda x: x)
    test_loader.derive_position_in_way = MagicMock(side_effect=lambda x: x)
    test_loader.remove_restricted_routes = MagicMock(side_effect=lambda x: x)

    test_loader.get_edge_start_coords = MagicMock(side_effect=lambda _, y: y)
    test_loader.get_edge_end_coords = MagicMock(side_effect=lambda _, y: y)
    test_loader.set_edge_output_schema = MagicMock(side_effect=lambda x: x)

    test_loader.write_nodes_to_parquet = MagicMock()
    test_loader.write_edges_to_parquet = MagicMock()

    # Act
    test_loader.load()

    # Assert
    test_loader.read_osm_data.assert_called_once()

    test_loader.assign_bng_coords.assert_called_once_with(mock_nodes)
    mock_add_partitions_to_polars_df.assert_called_once_with(mock_nodes)
    test_loader.set_node_output_schema.assert_called_once_with(mock_nodes)

    test_loader.tidy_edge_schema.assert_called_once_with(mock_edges)
    test_loader.set_flat_flag.assert_called_once_with(mock_edges)
    test_loader.add_reverse_edges.assert_called_once_with(mock_edges)
    test_loader.derive_position_in_way.assert_called_once_with(mock_edges)
    test_loader.remove_restricted_routes.assert_called_once_with(mock_edges)

    test_loader.get_edge_start_coords.assert_called_once_with(
        mock_nodes, mock_edges
    )
    test_loader.get_edge_end_coords.assert_called_once_with(
        mock_nodes, mock_edges
    )
    test_loader.set_edge_output_schema.assert_called_once_with(mock_edges)

    test_loader.write_nodes_to_parquet.assert_called_once_with(mock_nodes)
    test_loader.write_edges_to_parquet.assert_called_once_with(mock_edges)
