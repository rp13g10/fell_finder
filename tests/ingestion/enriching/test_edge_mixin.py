"""Tests for methods relating to enrichment of graph edges"""

from typing import Tuple
from unittest.mock import patch, MagicMock

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)
from pyspark.testing import assertDataFrameEqual

from fell_finder.ingestion.enriching.edge_mixin import EdgeMixin


class DummyEdgeMixin(EdgeMixin):
    """Dummy implementation of abstract base class for testing purposes"""

    def __init__(self) -> None:
        self.data_dir = "data_dir"
        self.spark = "spark"  # type: ignore
        self.edge_resolution_m = 10


def test_calculate_step_metrics(test_session: SparkSession):
    """Make sure the correct number of steps are being assigned to each edge"""
    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # fmt: off
    _ = (
        ['src_easting', 'src_northing', 'dst_easting', 'dst_northing'])

    test_data = [
        # Greater than the configured step size
        [0            , 0             , 20          , 20],
        [10           , 10            , 25          , 25],
        # Equal to the configured step size
        [0            , 0             , 10          , 0],
        [0            , 0             , 0           , 10],
        # Less than the configured step size
        [0            , 0             , 3           , 3],
        [10           , 10            , 15          , 15],
        # Opposite direction
        [25           , 25            , 10          , 10]
    ]

    # fmt: on

    test_schema = StructType(
        [
            StructField("src_easting", IntegerType()),
            StructField("src_northing", IntegerType()),
            StructField("dst_easting", IntegerType()),
            StructField("dst_northing", IntegerType()),
        ]
    )

    test_df = test_session.createDataFrame(test_data, test_schema)

    test_edge_mixin = DummyEdgeMixin()

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ['src_easting', 'src_northing', 'dst_easting', 'dst_northing', 'edge_size_h', 'edge_size_v', 'edge_size', 'edge_steps'])

    target_data = [
        # Greater than the configured step size
        [0            , 0             , 20          , 20             , 20           , 20           , 800**0.5   , 4],
        [10           , 10            , 25          , 25             , 15           , 15           , 450**0.5   , 3],
        # Equal to the configured step size
        [0            , 0             , 10          , 0              , 10           , 0            , 10.0       , 2],
        [0            , 0             , 0           , 10             , 0            , 10           , 10.0       , 2],
        # Less than the configured step size
        [0            , 0             , 3           , 3              , 3            , 3            , 18**0.5    , 2],
        [10           , 10            , 15          , 15             , 5            , 5            , 50**0.5    , 2],
        # Opposite direction
        [25           , 25            , 10          , 10             , -15          , -15          , 450**0.5   , 3]
    ]

    # fmt: on

    target_schema = StructType(
        [
            StructField("src_easting", IntegerType()),
            StructField("src_northing", IntegerType()),
            StructField("dst_easting", IntegerType()),
            StructField("dst_northing", IntegerType()),
            StructField("edge_size_h", IntegerType()),
            StructField("edge_size_v", IntegerType()),
            StructField("edge_size", DoubleType()),
            StructField("edge_steps", IntegerType()),
        ]
    )

    target_df = test_session.createDataFrame(target_data, target_schema)
    target_df = target_df.select(*sorted(target_df.columns))

    # Act #####################################################################

    result_df = test_edge_mixin.calculate_step_metrics(test_df)
    result_df = result_df.select(*sorted(result_df.columns))

    # Assert ##################################################################
    assertDataFrameEqual(result_df, target_df)


def test_explode_edges(test_session: SparkSession):
    """Make sure edges are being split across the correct number of rows"""
    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # fmt: off
    _ = (
        ['src_easting', 'src_northing', 'dst_easting', 'dst_northing', 'edge_steps'])

    test_data = [
        # Greater than the configured step size
        [0            , 0             , 20          , 20             , 4],
        [10           , 10            , 25          , 25             , 3],
        # Equal to the configured step size
        [0            , 0             , 10          , 0              , 2],
        [0            , 0             , 0           , 10             , 2],
        # Less than the configured step size
        [0            , 0             , 3           , 3              , 2],
        [10           , 10            , 15          , 15             , 2],
        # Opposite direction
        [25           , 25            , 10          , 10             , 3]
    ]

    # fmt: on

    test_schema = StructType(
        [
            StructField("src_easting", IntegerType()),
            StructField("src_northing", IntegerType()),
            StructField("dst_easting", IntegerType()),
            StructField("dst_northing", IntegerType()),
            StructField("edge_steps", IntegerType()),
        ]
    )

    test_df = test_session.createDataFrame(test_data, test_schema)

    test_edge_mixin = DummyEdgeMixin()

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ['src_easting', 'src_northing', 'dst_easting', 'dst_northing', 'edge_steps', 'coords'])

    target_data = [
        # Greater than the configured step size
        [0            , 0             , 20          , 20             , 4           , {'inx_arr': 0, 'easting_arr': 0.0, 'northing_arr': 0.0}],
        [0            , 0             , 20          , 20             , 4           , {'inx_arr': 1, 'easting_arr': 20.0/3, 'northing_arr': 20.0/3}],
        [0            , 0             , 20          , 20             , 4           , {'inx_arr': 2, 'easting_arr': 2*(20.0/3), 'northing_arr': 2*(20.0/3)}],
        [0            , 0             , 20          , 20             , 4           , {'inx_arr': 3, 'easting_arr': 20.0, 'northing_arr': 20.0}],
        [10           , 10            , 25          , 25             , 3           , {'inx_arr': 0, 'easting_arr': 10.0, 'northing_arr': 10.0}],
        [10           , 10            , 25          , 25             , 3           , {'inx_arr': 1, 'easting_arr': 10.0+(15.0/2), 'northing_arr': 10.0+(15.0/2)}],
        [10           , 10            , 25          , 25             , 3           , {'inx_arr': 2, 'easting_arr': 25.0, 'northing_arr': 25.0}],
        # Equal to the configured step size
        [0            , 0             , 10          , 0              , 2           , {'inx_arr': 0, 'easting_arr': 0.0, 'northing_arr': 0.0}],
        [0            , 0             , 10          , 0              , 2           , {'inx_arr': 1, 'easting_arr': 10.0, 'northing_arr': 0.0}],
        [0            , 0             , 0           , 10             , 2           , {'inx_arr': 0, 'easting_arr': 0.0, 'northing_arr': 0.0}],
        [0            , 0             , 0           , 10             , 2           , {'inx_arr': 1, 'easting_arr': 0.0, 'northing_arr': 10.0}],
        # Less than the configured step size
        [0            , 0             , 3           , 3              , 2           , {'inx_arr': 0, 'easting_arr': 0.0, 'northing_arr': 0.0}],
        [0            , 0             , 3           , 3              , 2           , {'inx_arr': 1, 'easting_arr': 3.0, 'northing_arr': 3.0}],
        [10           , 10            , 15          , 15             , 2           , {'inx_arr': 0, 'easting_arr': 10.0, 'northing_arr': 10.0}],
        [10           , 10            , 15          , 15             , 2           , {'inx_arr': 1, 'easting_arr': 15.0, 'northing_arr': 15.0}],
        # Opposite direction
        [25           , 25            , 10          , 10             , 3           , {'inx_arr': 0, 'easting_arr': 25.0, 'northing_arr': 25.0}],
        [25           , 25            , 10          , 10             , 3           , {'inx_arr': 1, 'easting_arr': 25.0+(-15.0/2),'northing_arr': 25.0+(-15.0/2)}],
        [25           , 25            , 10          , 10             , 3           , {'inx_arr': 2, 'easting_arr': 10.0, 'northing_arr': 10.0}],
    ]

    # fmt: on

    target_schema = StructType(
        [
            StructField("src_easting", IntegerType()),
            StructField("src_northing", IntegerType()),
            StructField("dst_easting", IntegerType()),
            StructField("dst_northing", IntegerType()),
            StructField("edge_steps", IntegerType()),
            StructField(
                "coords",
                StructType(
                    [
                        StructField("inx_arr", IntegerType()),
                        StructField("easting_arr", DoubleType()),
                        StructField("northing_arr", DoubleType()),
                    ]
                ),
            ),
        ]
    )

    target_df = test_session.createDataFrame(target_data, target_schema)
    target_df = target_df.select(*sorted(target_df.columns))

    # Act #####################################################################

    result_df = test_edge_mixin.explode_edges(test_df)
    result_df = result_df.select(*sorted(result_df.columns))

    # Assert ##################################################################
    assertDataFrameEqual(result_df, target_df)


def test_unpack_exploded_edges(test_session: SparkSession):
    """Make sure the coords structure is correctly unpacked"""
    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # fmt: off
    _ = (
        ['src', 'dst', 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'highway', 'surface', 'bridge', 'other', 'coords'])

    test_data = [
        ['src', 'dst', 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'highway', 'surface', 'bridge', 'other', {'inx_arr': 'inx_arr', 'easting_arr': 1.0, 'northing_arr': 2.0}]
    ]

    # fmt: on

    test_schema = StructType(
        [
            StructField("src", StringType()),
            StructField("dst", StringType()),
            StructField("src_lat", StringType()),
            StructField("src_lon", StringType()),
            StructField("dst_lat", StringType()),
            StructField("dst_lon", StringType()),
            StructField("src_easting", StringType()),
            StructField("src_northing", StringType()),
            StructField("way_id", StringType()),
            StructField("way_inx", StringType()),
            StructField("highway", StringType()),
            StructField("surface", StringType()),
            StructField("bridge", StringType()),
            StructField("other", StringType()),
            StructField(
                "coords",
                StructType(
                    [
                        StructField("inx_arr", StringType()),
                        StructField("easting_arr", StringType()),
                        StructField("northing_arr", StringType()),
                    ]
                ),
            ),
        ]
    )

    test_df = test_session.createDataFrame(test_data, test_schema)

    test_edge_mixin = DummyEdgeMixin()

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ['src', 'dst', 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'highway', 'surface', 'bridge', 'inx'    , 'easting', 'northing'])

    target_data = [
        ['src', 'dst', 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'highway', 'surface', 'bridge', 'inx_arr', 1        , 2]
    ]

    # fmt: on

    target_schema = StructType(
        [
            StructField("src", StringType()),
            StructField("dst", StringType()),
            StructField("src_lat", StringType()),
            StructField("src_lon", StringType()),
            StructField("dst_lat", StringType()),
            StructField("dst_lon", StringType()),
            StructField("src_easting", StringType()),
            StructField("src_northing", StringType()),
            StructField("way_id", StringType()),
            StructField("way_inx", StringType()),
            StructField("highway", StringType()),
            StructField("surface", StringType()),
            StructField("bridge", StringType()),
            StructField("inx", StringType()),
            StructField("easting", IntegerType()),
            StructField("northing", IntegerType()),
        ]
    )

    target_df = test_session.createDataFrame(target_data, target_schema)
    target_df = target_df.select(*sorted(target_df.columns))

    # Act #####################################################################

    result_df = test_edge_mixin.unpack_exploded_edges(test_df)
    result_df = result_df.select(*sorted(result_df.columns))

    # Assert ##################################################################
    assertDataFrameEqual(result_df, target_df)


def test_tag_exploded_edges(test_session: SparkSession):
    """Make sure the table join has been set up properly"""
    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # ----- Edges -----

    # fmt: off
    _ = (
        ['easting', 'northing', 'easting_ptn', 'northing_ptn', 'src' , 'dst' , 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'inx', 'way_id', 'way_inx', 'highway', 'surface', 'bridge'])

    test_edge_data = [
        ['left'   , 'left'    , 'left'       , 'left'        , 'left', 'left', 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'inx', 'way_id', 'way_inx', 'highway', 'surface', 'bridge'],
        ['both'   , 'both'    , 'both'       , 'both'        , 'both', 'both', 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'inx', 'way_id', 'way_inx', 'highway', 'surface', 'bridge']
    ]
    # fmt: on

    test_edge_schema = StructType(
        [
            StructField("easting", StringType()),
            StructField("northing", StringType()),
            StructField("easting_ptn", StringType()),
            StructField("northing_ptn", StringType()),
            StructField("src", StringType()),
            StructField("dst", StringType()),
            StructField("src_lat", StringType()),
            StructField("src_lon", StringType()),
            StructField("dst_lat", StringType()),
            StructField("dst_lon", StringType()),
            StructField("src_easting", StringType()),
            StructField("src_northing", StringType()),
            StructField("inx", StringType()),
            StructField("way_id", StringType()),
            StructField("way_inx", StringType()),
            StructField("highway", StringType()),
            StructField("surface", StringType()),
            StructField("bridge", StringType()),
        ]
    )

    test_edge_df = test_session.createDataFrame(
        test_edge_data, test_edge_schema
    )

    # ----- Elevation -----

    # fmt: off
    _ = (
        ['easting', 'northing', 'easting_ptn', 'northing_ptn', 'elevation'])

    test_ele_data = [
        ['both'   , 'both'    , 'both'       , 'both'        , 'both'],
        ['right'  , 'right'   , 'right'      , 'right'       , 'right']
    ]
    # fmt: on

    test_ele_schema = StructType(
        [
            StructField("easting", StringType()),
            StructField("northing", StringType()),
            StructField("easting_ptn", StringType()),
            StructField("northing_ptn", StringType()),
            StructField("elevation", StringType()),
        ]
    )

    test_ele_df = test_session.createDataFrame(test_ele_data, test_ele_schema)

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ['src'  , 'dst' , 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'inx', 'elevation', 'way_id', 'way_inx', 'highway', 'surface', 'bridge'])

    target_data = [
        ['both' , 'both', 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'inx', 'both'     , 'way_id', 'way_inx', 'highway', 'surface', 'bridge']
    ]
    # fmt: on

    target_schema = StructType(
        [
            StructField("src", StringType()),
            StructField("dst", StringType()),
            StructField("src_lat", StringType()),
            StructField("src_lon", StringType()),
            StructField("dst_lat", StringType()),
            StructField("dst_lon", StringType()),
            StructField("src_easting", StringType()),
            StructField("src_northing", StringType()),
            StructField("inx", StringType()),
            StructField("elevation", StringType()),
            StructField("way_id", StringType()),
            StructField("way_inx", StringType()),
            StructField("highway", StringType()),
            StructField("surface", StringType()),
            StructField("bridge", StringType()),
        ]
    )

    target_df = test_session.createDataFrame(target_data, target_schema)
    target_df = target_df.select(*sorted(target_df.columns))

    # Act #####################################################################

    result_df = EdgeMixin.tag_exploded_edges(test_edge_df, test_ele_df)
    result_df = result_df.select(*sorted(result_df.columns))

    # Assert ##################################################################
    assertDataFrameEqual(result_df, target_df)


def test_calculate_elevation_changes(test_session: SparkSession):
    """Make sure elevation changes are correctly calculated"""
    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # fmt: off
    _ = (
        ['src', 'dst', 'inx', 'elevation', 'bridge'])

    test_data = [
        # No bridge (implicit)
        [0    , 1    , 0    , 5.0        , None],
        [0    , 1    , 1    , 10.0       , None],
        # Bridge (explicit)
        [1    , 2    , 0    , 5.0        , 'yes'],
        [1    , 2    , 1    , 10.0       , 'yes'],
        # No bridge (explicit)
        [2    , 3    , 0    , 5.0        , 'no'],
        [2    , 3    , 1    , 10.0       , 'no'],
        # Up at each point
        [3    , 4    , 0    , 5.0        , None],
        [3    , 4    , 1    , 10.0       , None],
        [3    , 4    , 2    , 15.0       , None],
        # Down at each point
        [4    , 5    , 0    , 15.0       , None],
        [4    , 5    , 1    , 10.0       , None],
        [4    , 5    , 2    , 5.0        , None],
        # Up then down
        [5    , 6    , 0    , 10.0       , None],
        [5    , 6    , 1    , 15.0       , None],
        [5    , 6    , 2    , 5.0        , None],
        # No change
        [6    , 7    , 0    , 5.0        , None],
        [6    , 7    , 0    , 5.0        , None]
    ]

    # fmt: on

    test_schema = StructType(
        [
            StructField("src", IntegerType()),
            StructField("dst", IntegerType()),
            StructField("inx", IntegerType()),
            StructField("elevation", DoubleType()),
            StructField("bridge", StringType()),
        ]
    )

    test_df = test_session.createDataFrame(test_data, test_schema)

    test_edge_mixin = DummyEdgeMixin()

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ['src', 'dst', 'inx', 'elevation', 'bridge', 'last_elevation', 'delta', 'elevation_gain', 'elevation_loss'])

    target_data = [
        # No bridge (implicit)
        [0    , 1    , 0    , 5.0        , None    , None            , None   , 0.0             , 0.0],
        [0    , 1    , 1    , 10.0       , None    , 5.0             , 5.0    , 5.0             , 0.0],
        # Bridge (explicit)
        [1    , 2    , 0    , 5.0        , 'yes'   , None            , None   , 0.0             , 0.0],
        [1    , 2    , 1    , 10.0       , 'yes'   , 5.0             , 5.0    , 0.0             , 0.0],
        # No bridge (explicit)
        [2    , 3    , 0    , 5.0        , 'no'    , None            , None   , 0.0             , 0.0],
        [2    , 3    , 1    , 10.0       , 'no'    , 5.0             , 5.0    , 5.0             , 0.0],
        # Up at each point
        [3    , 4    , 0    , 5.0        , None    , None            , None   , 0.0             , 0.0],
        [3    , 4    , 1    , 10.0       , None    , 5.0             , 5.0    , 5.0             , 0.0],
        [3    , 4    , 2    , 15.0       , None    , 10.0            , 5.0    , 5.0             , 0.0],
        # Down at each point
        [4    , 5    , 0    , 15.0       , None    , None            , None   , 0.0             , 0.0],
        [4    , 5    , 1    , 10.0       , None    , 15.0            , -5.0   , 0.0            , 5.0],
        [4    , 5    , 2    , 5.0        , None    , 10.0            , -5.0   , 0.0            , 5.0],
        # Up then down
        [5    , 6    , 0    , 10.0       , None    , None            , None   , 0.0             , 0.0],
        [5    , 6    , 1    , 15.0       , None    , 10.0            , 5.0    , 5.0             , 0.0],
        [5    , 6    , 2    , 5.0        , None    , 15.0            , -10.0  , 0.0            , 10.0],
        # No change
        [6    , 7    , 0    , 5.0        , None    , None            , None   , 0.0             , 0.0],
        [6    , 7    , 0    , 5.0        , None    , 5.0             , 0.0    , 0.0             , 0.0]
    ]

    # fmt: on

    target_schema = StructType(
        [
            StructField("src", IntegerType()),
            StructField("dst", IntegerType()),
            StructField("inx", IntegerType()),
            StructField("elevation", DoubleType()),
            StructField("bridge", StringType()),
            StructField("last_elevation", DoubleType()),
            StructField("delta", DoubleType()),
            StructField("elevation_gain", DoubleType()),
            StructField("elevation_loss", DoubleType()),
        ]
    )

    target_df = test_session.createDataFrame(target_data, target_schema)
    target_df = target_df.select(*sorted(target_df.columns))

    # Act #####################################################################

    result_df = test_edge_mixin.calculate_elevation_changes(test_df)
    result_df = result_df.select(*sorted(result_df.columns))

    # Assert ##################################################################
    assertDataFrameEqual(result_df, target_df)


def test_implode_edges(test_session: SparkSession):
    """Make sure data is being aggregated properly"""

    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # fmt: off
    _ = (
        ['src', 'dst', 'inx', 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'highway', 'surface', 'elevation_gain', 'elevation_loss', 'other'])

    test_data = [
        # Single record
        [0    , 1    , 1    , 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'highway', 'surface', 1.0             , 1.0             , 'other'],
        # Multiple records
        [1    , 2    , 1    , 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'highway', 'surface', 1.0             , 1.0             , 'other'],
        [1    , 2    , 2    , 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'highway', 'surface', 1.0             , 1.0             , 'other'],
    ]

    # fmt: on

    test_schema = StructType(
        [
            StructField("src", IntegerType()),
            StructField("dst", IntegerType()),
            StructField("inx", IntegerType()),
            StructField("src_lat", StringType()),
            StructField("src_lon", StringType()),
            StructField("dst_lat", StringType()),
            StructField("dst_lon", StringType()),
            StructField("src_easting", StringType()),
            StructField("src_northing", StringType()),
            StructField("way_id", StringType()),
            StructField("way_inx", StringType()),
            StructField("highway", StringType()),
            StructField("surface", StringType()),
            StructField("elevation_gain", DoubleType()),
            StructField("elevation_loss", DoubleType()),
            StructField("other", StringType()),
        ]
    )

    test_df = test_session.createDataFrame(test_data, test_schema)

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ['src', 'dst', 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'highway', 'surface', 'elevation_gain', 'elevation_loss'])

    target_data = [
        # Single record
        [0    , 1    , 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'highway', 'surface', 1.0             , 1.0],
        # Multiple records
        [1    , 2    , 'src_lat', 'src_lon', 'dst_lat', 'dst_lon', 'src_easting', 'src_northing', 'way_id', 'way_inx', 'highway', 'surface', 2.0             , 2.0]

    ]

    # fmt: on

    target_schema = StructType(
        [
            StructField("src", IntegerType()),
            StructField("dst", IntegerType()),
            StructField("src_lat", StringType()),
            StructField("src_lon", StringType()),
            StructField("dst_lat", StringType()),
            StructField("dst_lon", StringType()),
            StructField("src_easting", StringType()),
            StructField("src_northing", StringType()),
            StructField("way_id", StringType()),
            StructField("way_inx", StringType()),
            StructField("highway", StringType()),
            StructField("surface", StringType()),
            StructField("elevation_gain", DoubleType()),
            StructField("elevation_loss", DoubleType()),
        ]
    )

    target_df = test_session.createDataFrame(target_data, target_schema)
    target_df = target_df.select(*sorted(target_df.columns))

    # Act #####################################################################

    result_df = EdgeMixin.implode_edges(test_df)
    result_df = result_df.select(*sorted(result_df.columns))

    # Assert ##################################################################
    assertDataFrameEqual(result_df, target_df)


@patch("fell_finder.ingestion.enriching.edge_mixin.distance")
def test_calculate_edge_distances(
    mock_distance: MagicMock, test_session: SparkSession
):
    """Make sure the distance calculation is being applied properly"""

    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # fmt: off
    _ = (
        ['src_lat', 'src_lon', 'dst_lat', 'dst_lon'])

    test_data = [
        [0.0      , 0.0      , 5.0      , 5.0]
    ]

    # fmt: on

    test_schema = StructType(
        [
            StructField("src_lat", DoubleType()),
            StructField("src_lon", DoubleType()),
            StructField("dst_lat", DoubleType()),
            StructField("dst_lon", DoubleType()),
        ]
    )

    test_df = test_session.createDataFrame(test_data, test_schema)

    def side_effect(
        source: Tuple[float, float], destination: Tuple[float, float]
    ) -> float:
        """Mock behaviour for geopy.distance"""
        src_lat, src_lon = source
        dst_lat, dst_lon = destination
        lat_dist = dst_lat - src_lat
        lon_dist = dst_lon - src_lon
        dist_m = ((lat_dist**2) + (lon_dist**2)) ** 0.5
        dist = MagicMock()
        dist.meters = dist_m
        return dist

    mock_distance.side_effect = side_effect

    # Target Data -------------------------------------------------------------

    # fmt: off
    _ = (
        ['src_lat', 'src_lon', 'dst_lat', 'dst_lon'])

    target_data = [
        [0.0      , 0.0      , 5.0      , 5.0, 50**0.5]
    ]

    # fmt: on

    target_schema = StructType(
        [
            StructField("src_lat", DoubleType()),
            StructField("src_lon", DoubleType()),
            StructField("dst_lat", DoubleType()),
            StructField("dst_lon", DoubleType()),
            StructField("distance", DoubleType()),
        ]
    )

    target_df = test_session.createDataFrame(target_data, target_schema)
    target_df = target_df.select(*sorted(target_df.columns))

    # Act #####################################################################

    result_df = EdgeMixin.calculate_edge_distances(test_df)
    result_df = result_df.select(*sorted(result_df.columns))

    # Assert ##################################################################
    assertDataFrameEqual(result_df, target_df)


def test_set_edge_output_schema(test_session: SparkSession):
    """Make sure the output schema is being set correctly"""

    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    test_cols = [
        "src",
        "dst",
        "src_lat",
        "src_lon",
        "dst_lat",
        "dst_lon",
        "way_id",
        "way_inx",
        "highway",
        "surface",
        "distance",
        "elevation_gain",
        "elevation_loss",
        "easting_ptn",
        "northing_ptn",
        "other",
    ]

    test_data = [[0 for _ in test_cols]]

    test_df = test_session.createDataFrame(test_data, test_cols)

    # Target Data -------------------------------------------------------------

    target_cols = [
        "src",
        "dst",
        "src_lat",
        "src_lon",
        "dst_lat",
        "dst_lon",
        "way_id",
        "way_inx",
        "highway",
        "surface",
        "distance",
        "elevation_gain",
        "elevation_loss",
        "easting_ptn",
        "northing_ptn",
    ]

    target_data = [[0 for _ in target_cols]]

    target_df = test_session.createDataFrame(target_data, target_cols)
    target_df = target_df.select(*sorted(target_df.columns))

    # Act #####################################################################

    result_df = EdgeMixin.set_edge_output_schema(test_df)
    result_df = result_df.select(*sorted(result_df.columns))

    # Assert ##################################################################
    assertDataFrameEqual(result_df, target_df)
