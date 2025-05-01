"""Unit tests for the OSM loader"""

from unittest.mock import MagicMock, patch

import pandas as pd
import polars as pl
from pandas.testing import assert_frame_equal as pd_assert_frame_equal
from polars.testing import assert_frame_equal as pl_assert_frame_equal

from fell_loader.parsing.osm_loader import OsmLoader


def test_subset_nodes():
    """Check that the correct fields are being brought through"""
    # Arrange #################################################################

    # ----- Test Data -----
    # fmt: off
    test_cols = (
        ['id', 'lat', 'lon', 'other'])

    test_data = [
        [0] * len(test_cols)
    ]
    # fmt: on

    test_df = pd.DataFrame(data=test_data, columns=test_cols)

    # ----- Target Data -----=
    # fmt: off
    tgt_cols = (
        ['id', 'lat', 'lon'])

    tgt_data = [
        [0] * len(tgt_cols)
    ]
    # fmt: on

    tgt_df = pd.DataFrame(data=tgt_data, columns=tgt_cols)

    # Act #####################################################################
    res_df = OsmLoader._subset_nodes(test_df)

    # Assert ##################################################################
    pd_assert_frame_equal(tgt_df, res_df)


def test_subset_edges():
    """Check that the correct fields are being brought through"""
    # Arrange #################################################################

    # ----- Test Data -----
    # fmt: off
    test_cols = (
        ['id', 'u', 'v', 'highway', 'surface', 'bridge', 'tunnel', 'oneway', 'access', 'other'])

    test_data = [
        ['id', 'u', 'v', 'highway', 'surface', 'bridge', 'tunnel', 'oneway', 'access', 'other'],
        ['id', 'u', 'v', 'highway', 'surface', 'bridge', 'tunnel', 'oneway', 'access', 'other'],
        ['id', 'u', 'v', 'highway', 'surface', 'bridge', 'tunnel', 'oneway', 'access', 'other']
    ]
    # fmt: on

    test_df = pd.DataFrame(data=test_data, columns=test_cols)

    # ----- Target Data -----=
    # fmt: off
    tgt_cols = (
        ['id', 'u', 'v', 'highway', 'surface', 'bridge', 'tunnel', 'oneway', 'access', 'row_no'])

    tgt_data = [
        ['id', 'u', 'v', 'highway', 'surface', 'bridge', 'tunnel', 'oneway', 'access', 0],
        ['id', 'u', 'v', 'highway', 'surface', 'bridge', 'tunnel', 'oneway', 'access', 1],
        ['id', 'u', 'v', 'highway', 'surface', 'bridge', 'tunnel', 'oneway', 'access', 2]
    ]
    # fmt: on

    tgt_df = pd.DataFrame(data=tgt_data, columns=tgt_cols)

    # Act #####################################################################
    res_df = OsmLoader._subset_edges(test_df)

    # Assert ##################################################################
    pd_assert_frame_equal(tgt_df, res_df)


@patch("fell_loader.parsing.osm_loader.OSM")
def test_read_osm_data(mock_osm: MagicMock):
    """Check that data is being read in, processed and converted into polars
    dataframes"""
    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------
    # ----- Nodes -----

    # fmt: off
    test_node_cols = (
        ['id', 'lat', 'lon', 'other'])

    test_node_data = [
        ['id', 'lat', 'lon', 'other']
    ]
    # fmt: on

    test_node_df = pd.DataFrame(data=test_node_data, columns=test_node_cols)

    # ----- Edges -----

    # fmt: off
    test_edge_cols = (
        ['id', 'u', 'v', 'highway', 'surface', 'bridge', 'tunnel', 'oneway', 'access', 'other'])

    test_edge_data = [
        ['id', 'u', 'v', 'highway', 'surface', 'bridge', 'tunnel', 'oneway', 'access', 'other'],
        ['id', 'u', 'v', 'highway', 'surface', 'bridge', 'tunnel', 'oneway', 'access', 'other'],
        ['id', 'u', 'v', 'highway', 'surface', 'bridge', 'tunnel', 'oneway', 'access', 'other']
    ]
    # fmt: on

    test_edge_df = pd.DataFrame(data=test_edge_data, columns=test_edge_cols)

    # ----- Initialize Class -----

    mock_osm_handle = MagicMock()
    mock_osm.return_value = mock_osm_handle
    mock_osm_handle.get_network.return_value = test_node_df, test_edge_df

    test_osm_loader = OsmLoader("data_dir")

    # Target Data -------------------------------------------------------------

    # ----- Nodes -----
    # fmt: off
    tgt_node_cols = (
        ['id', 'lat', 'lon'])

    tgt_node_data = [
        ['id', 'lat', 'lon']
    ]
    # fmt: on

    tgt_node_df = pl.DataFrame(
        data=tgt_node_data, schema=tgt_node_cols, orient="row"
    )

    # ----- Edges -----
    # fmt: off
    tgt_edge_cols = (
        ['id', 'u', 'v', 'highway', 'surface', 'bridge', 'tunnel', 'oneway', 'access', 'row_no'])

    tgt_edge_data = [
        ['id', 'u', 'v', 'highway', 'surface', 'bridge', 'tunnel', 'oneway', 'access', 0],
        ['id', 'u', 'v', 'highway', 'surface', 'bridge', 'tunnel', 'oneway', 'access', 1],
        ['id', 'u', 'v', 'highway', 'surface', 'bridge', 'tunnel', 'oneway', 'access', 2]
    ]
    # fmt: on

    tgt_edge_df = pl.DataFrame(
        data=tgt_edge_data, schema=tgt_edge_cols, orient="row"
    )

    # Expected Calls ----------------------------------------------------------

    tgt_osm_call = "data_dir/extracts/osm/hampshire-latest.osm.pbf"

    # Act #####################################################################
    res_node_df, res_edge_df = test_osm_loader.read_osm_data()

    # Assert ##################################################################
    mock_osm.assert_called_once_with(tgt_osm_call)
    pl_assert_frame_equal(res_node_df, tgt_node_df)
    pl_assert_frame_equal(res_edge_df, tgt_edge_df)


@patch("fell_loader.parsing.osm_loader.WGS84toOSGB36")
def test_assign_bng_coords(mock_wgs84_to_osgb36: MagicMock):
    """Check that records are correctly being assigned BNG coordinates based
    on their latitude & longitude"""
    # Arrange #################################################################

    # ----- Test Data -----
    # fmt: off
    _ = (
        ['inx', 'lat', 'lon'])

    test_data = [
        [0    , 50.0 , 10.0],
        [1    , 100.0, 20.0],
        [2    , 66.6 , 33.3]
    ]
    # fmt: on

    test_schema = {"inx": pl.Int32(), "lat": pl.Float64(), "lon": pl.Float64()}

    test_df = pl.DataFrame(data=test_data, schema=test_schema, orient="row")

    # ----- Mocks -----

    mock_wgs84_to_osgb36.side_effect = lambda x, y: (x / 2, y / 2)

    # ----- Target Data -----=
    # fmt: off
    _ = (
        ['inx', 'lat', 'lon', 'easting', 'northing'])

    tgt_data = [
        [0    , 50.0 , 10.0 , 25       , 5],
        [1    , 100.0, 20.0 , 50       , 10],
        [2    , 66.6 , 33.3 , 33       , 16]
    ]
    # fmt: on

    tgt_schema = {
        "inx": pl.Int32(),
        "lat": pl.Float64(),
        "lon": pl.Float64(),
        "easting": pl.Int32(),
        "northing": pl.Int32(),
    }

    tgt_df = pl.DataFrame(data=tgt_data, schema=tgt_schema, orient="row")

    # Act #####################################################################
    res_df = OsmLoader.assign_bng_coords(test_df)

    # Assert ##################################################################
    pl_assert_frame_equal(tgt_df, res_df)


def test_set_node_output_schema():
    """Check that the correct schema is being set for the nodes dataset"""
    # Arrange #################################################################

    # ----- Test Data -----
    # fmt: off
    test_cols = (
        ['id', 'lat', 'lon', 'easting', 'northing', 'easting_ptn', 'northing_ptn', 'other'])

    test_data = [
        [0] * len(test_cols)
    ]
    # fmt: on

    test_df = pl.DataFrame(data=test_data, schema=test_cols, orient="row")

    # ----- Target Data -----=
    # fmt: off
    tgt_cols = (
        ['id', 'lat', 'lon', 'easting', 'northing', 'easting_ptn', 'northing_ptn'])

    tgt_data = [
        [0] * len(tgt_cols)
    ]
    # fmt: on

    tgt_df = pl.DataFrame(data=tgt_data, schema=tgt_cols, orient="row")

    # Act #####################################################################
    res_df = OsmLoader.set_node_output_schema(test_df)

    # Assert ##################################################################
    pl_assert_frame_equal(tgt_df, res_df)


def test_tidy_edge_schema():
    """Check that the correct aliases are being set for the edges dataset"""
    # Arrange #################################################################

    # ----- Test Data -----
    # fmt: off
    test_cols = (
        ['u', 'v', 'id', 'highway', 'surface', 'bridge', 'tunnel', 'row_no', 'oneway', 'access', 'other'])

    test_data = [
        ['u', 'v', 'id', 'highway', 'surface', 'bridge', 'tunnel', 'row_no', 'oneway', 'access', 'other']
    ]
    # fmt: on

    test_df = pl.DataFrame(data=test_data, schema=test_cols, orient="row")

    # ----- Target Data -----=
    # fmt: off
    tgt_cols = (
        ['src', 'dst', 'way_id', 'highway', 'surface', 'bridge', 'tunnel', 'row_no', 'oneway', 'access'])

    tgt_data = [
        ['u'  , 'v'  , 'id'    , 'highway', 'surface', 'bridge', 'tunnel', 'row_no', 'oneway', 'access']
    ]
    # fmt: on

    tgt_df = pl.DataFrame(data=tgt_data, schema=tgt_cols, orient="row")

    # Act #####################################################################
    res_df = OsmLoader.tidy_edge_schema(test_df)

    # Assert ##################################################################
    pl_assert_frame_equal(tgt_df, res_df)


def test_set_flat_flag():
    """Check that flat edges are being correctly flagged"""
    # Arrange #################################################################

    # ----- Test Data -----
    # fmt: off
    _ = (
        ['inx', 'bridge', 'tunnel'])

    test_data = [
        [0    , None    , None],
        [1    , 'some'  , 'some'],
        [2    , 'some'  , None],
        [3    , None    , 'some']
    ]
    # fmt: on

    test_schema = {
        "inx": pl.Int32(),
        "bridge": pl.String(),
        "tunnel": pl.String(),
    }

    test_df = pl.DataFrame(data=test_data, schema=test_schema, orient="row")

    # ----- Target Data -----=
    # fmt: off
    _ = (
        ['inx', 'is_flat'])

    tgt_data = [
        [0    , False],
        [1    , True],
        [2    , True],
        [3    , True]
    ]
    # fmt: on

    tgt_schema = {
        "inx": pl.Int32(),
        "is_flat": pl.Boolean(),
    }

    tgt_df = pl.DataFrame(data=tgt_data, schema=tgt_schema, orient="row")

    # Act #####################################################################
    res_df = OsmLoader.set_flat_flag(test_df)

    # Assert ##################################################################
    pl_assert_frame_equal(tgt_df, res_df)


def test_add_reverse_edges():
    """Check that reverse edges are being added correctly"""
    # Arrange #################################################################

    # ----- Test Data -----
    # fmt: off
    _ = (
        ['way_id', 'row_no', 'src', 'dst', 'oneway'])

    test_data = [
        # One way, should not be reversed
        [1       , 1       , 1    , 2    , 'yes'],
        [1       , 2       , 2    , 3    , 'yes'],
        # Explicitly not a one-way street
        [2       , 3       , 3    , 4    , 'no'],
        [2       , 4       , 4    , 5    , 'no'],
        # Implicitly not a one-way street
        [3       , 5       , 5    , 6    , None],
        [3       , 6       , 6    , 7    , None],
    ]
    # fmt: on

    test_schema = {
        "way_id": pl.Int32(),
        "row_no": pl.Int32(),
        "src": pl.Int32(),
        "dst": pl.Int32(),
        "oneway": pl.String(),
    }

    test_df = pl.DataFrame(data=test_data, schema=test_schema, orient="row")

    # ----- Target Data -----=
    # fmt: off
    _ = (
        ['way_id', 'row_no', 'src', 'dst', 'oneway'])

    tgt_data = [
        # One way, should not be reversed
        [1       , 1       , 1    , 2    , 'yes'],
        [1       , 2       , 2    , 3    , 'yes'],
        # Explicitly not a one-way street
        [2       , 3       , 3    , 4    , 'no'],
        [2       , 4       , 4    , 5    , 'no'],
        [-2      , -4      , 5    , 4    , 'no'],
        [-2      , -3      , 4    , 3    , 'no'],
        # Implicitly not a one-way street
        [3       , 5       , 5    , 6    , None],
        [3       , 6       , 6    , 7    , None],
        [-3      , -6      , 7    , 6    , None],
        [-3      , -5      , 6    , 5    , None],
    ]
    # fmt: on

    tgt_schema = {
        "way_id": pl.Int32(),
        "row_no": pl.Int32(),
        "src": pl.Int32(),
        "dst": pl.Int32(),
        "oneway": pl.String(),
    }

    tgt_df = pl.DataFrame(data=tgt_data, schema=tgt_schema, orient="row")

    # Act #####################################################################
    res_df = OsmLoader.add_reverse_edges(test_df)

    res_df = res_df.sort(["way_id", "row_no"])
    tgt_df = tgt_df.sort(["way_id", "row_no"])

    # Assert ##################################################################
    pl_assert_frame_equal(tgt_df, res_df)


def test_derive_position_in_way():
    """Check that relative positions are being set correctly within each way"""
    # Arrange #################################################################

    # ----- Test Data -----
    # fmt: off
    _ = (
        ['way_id', 'row_no'])

    test_data = [
        # One node only
        [0       , 0],
        # Two nodes
        [1       , 2],
        [1       , 1],
        # More than two nodes
        [2       , 3],
        [2       , 5],
        [2       , 4],
        # Reversed edge (not one way)
        [-2      , -3],
        [-2      , -5],
        [-2      , -4]
    ]
    # fmt: on

    test_schema = {
        "way_id": pl.Int32(),
        "row_no": pl.Int32(),
    }

    test_df = pl.DataFrame(data=test_data, schema=test_schema, orient="row")

    # ----- Target Data -----=
    # fmt: off
    _ = (
        ['way_id', 'row_no', 'way_inx'])

    tgt_data = [
        # One node only
        [0       , 0       , 1],
        # Two nodes
        [1       , 2       , 2],
        [1       , 1       , 1],
        # More than two nodes
        [2       , 3       , 1],
        [2       , 5       , 3],
        [2       , 4       , 2],
        # Reversed edge (not one way)
        [-2      , -3      , 3],
        [-2      , -5      , 1],
        [-2      , -4      , 2]
    ]
    # fmt: on

    tgt_schema = {
        "way_id": pl.Int32(),
        "row_no": pl.Int32(),
        "way_inx": pl.UInt32(),
    }

    tgt_df = pl.DataFrame(data=tgt_data, schema=tgt_schema, orient="row")

    # Act #####################################################################
    res_df = OsmLoader.derive_position_in_way(test_df)

    res_df = res_df.sort(["way_id", "row_no"])
    tgt_df = tgt_df.sort(["way_id", "row_no"])

    # Assert ##################################################################
    pl_assert_frame_equal(tgt_df, res_df)


def test_remove_restricted_routes():
    """Check that restricted edges are being removed properly"""
    # Arrange #################################################################

    # ----- Test Data -----
    # fmt: off
    _ = (
        ['inx', 'access'])

    test_data = [
        [0    , 'yes'],
        [1    , 'no'],
        [2    , 'permissive'],
        [3    , 'designated'],
        [4    , None],
        [5    , 'other']
    ]
    # fmt: on

    test_schema = {
        "inx": pl.Int32(),
        "access": pl.String(),
    }

    test_df = pl.DataFrame(data=test_data, schema=test_schema, orient="row")

    # ----- Target Data -----=
    # fmt: off
    _ = (
        ['inx', 'access'])

    tgt_data = [
        [0    , 'yes'],
        [2    , 'permissive'],
        [3    , 'designated'],
        [4    , None],
    ]
    # fmt: on

    tgt_schema = {
        "inx": pl.Int32(),
        "access": pl.String(),
    }

    tgt_df = pl.DataFrame(data=tgt_data, schema=tgt_schema, orient="row")

    # Act #####################################################################
    res_df = OsmLoader.remove_restricted_routes(test_df)

    # Assert ##################################################################
    pl_assert_frame_equal(tgt_df, res_df)


def test_get_edge_start_coords():
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
        [1   , 1    , 1    , 1       , 1          , 1            , 1            , 'other_nodes'],
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
        ['src', 'dst', 'other_edges', 'src_lat', 'src_lon', 'src_easting', 'src_northing', 'easting_ptn', 'northing_ptn'])

    tgt_data = [
        # Left only
        # Right only
        # Both
        [4   , 5    , 'other_edges' , 4        , 4        , 4           , 4             , 4             , 4]
    ]
    # fmt: on

    tgt_schema = {
        "src": pl.Int32(),
        "dst": pl.Int32(),
        "other_edges": pl.String(),
        "src_lat": pl.Int32(),
        "src_lon": pl.Int32(),
        "src_easting": pl.Int32(),
        "src_northing": pl.Int32(),
        "easting_ptn": pl.Int32(),
        "northing_ptn": pl.Int32(),
    }

    tgt_df = pl.DataFrame(data=tgt_data, schema=tgt_schema, orient="row")

    # Act #####################################################################
    res_df = OsmLoader.get_edge_start_coords(test_node_df, test_edge_df)

    res_df = res_df.sort("src")
    tgt_df = tgt_df.sort("src")

    # Assert ##################################################################
    pl_assert_frame_equal(tgt_df, res_df)


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
