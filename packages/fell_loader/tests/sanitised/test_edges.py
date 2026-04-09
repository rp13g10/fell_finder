"""Tests for fell_loader.sanitised.edges"""

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from fell_loader.sanitised.edges import EdgeSanitiser
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)
from pyspark.testing import assertDataFrameEqual


class MockEdgeSanitiser(EdgeSanitiser):
    """Mock implementation of the edge sanitiser class, uses static values
    of fetching info from environment variables
    """

    def __init__(self, spark: SparkSession | None = None) -> None:
        # Attrs from base
        self.data_dir = Path("data_dir")
        self.skip_load = False
        self.spark: Any = spark if spark is not None else MagicMock()


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
        res_df = EdgeSanitiser.map_to_schema(test_df, test_schema, cast=True)

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
        res_df = EdgeSanitiser.map_to_schema(test_df, test_schema, cast=False)

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

        test_loader = MockEdgeSanitiser()

        test_df = MagicMock()
        test_layer = "landing"
        test_dataset = "edges"

        # Act
        test_loader.write_parquet(test_df, test_layer, test_dataset)

        # Assert
        mock_mkdir.assert_not_called()
        test_df.write.parquet.assert_called_once_with(
            "data_dir/landing/edges", mode="overwrite"
        )

    def test_target_dir_does_not_exist(
        self, mock_exists: MagicMock, mock_mkdir: MagicMock
    ):
        """If the target folder does not exist, it should be created"""
        # Arrange
        mock_exists.return_value = False

        test_loader = MockEdgeSanitiser()

        test_df = MagicMock()
        test_layer = "landing"
        test_dataset = "edges"

        # Act
        test_loader.write_parquet(test_df, test_layer, test_dataset)

        # Assert
        mock_mkdir.assert_called_once_with(
            Path("data_dir/landing/edges"), parents=True
        )
        test_df.write.parquet.assert_called_once_with(
            "data_dir/landing/edges", mode="overwrite"
        )


@patch("pathlib.Path.exists", autospec=True)
class TestReadParquet:
    """Make sure parquet reads have been set up properly"""

    def test_target_exists(self, mock_exists: MagicMock):
        """If the file exists, read it"""
        # Arrange
        mock_exists.return_value = True

        test_loader = MockEdgeSanitiser()

        test_layer = "landing"
        test_dataset = "edges"

        # Act
        result = test_loader.read_parquet(test_layer, test_dataset)

        # Assert
        test_loader.spark.read.parquet.assert_called_once_with(
            "data_dir/landing/edges"
        )
        assert result is test_loader.spark.read.parquet.return_value

    def test_target_does_not_exist(self, mock_exists: MagicMock):
        """If the file does not exist, an exception should be raised"""
        # Arrange
        mock_exists.return_value = False

        test_loader = MockEdgeSanitiser()

        test_layer = "landing"
        test_dataset = "edges"

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

        test_loader = MockEdgeSanitiser()

        test_layer = "landing"
        test_dataset = "edges"

        # Act
        result = test_loader.read_delta(test_layer, test_dataset)

        # Assert
        mock_deltatable.forPath.assert_called_once_with(
            test_loader.spark, "data_dir/landing/edges"
        )
        assert result is mock_deltatable.forPath.return_value.toDF.return_value

    def test_target_does_not_exist(
        self, mock_exists: MagicMock, mock_deltatable: MagicMock
    ):
        """If the file does not exist, an exception should be raised"""
        # Arrange
        mock_exists.return_value = False

        test_loader = MockEdgeSanitiser()

        test_layer = "landing"
        test_dataset = "edges"

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
    res_df = EdgeSanitiser.drop_unused_nodes(test_nodes_df, test_edges_df)

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


# MARK: Implementation Specific


def test_drop_edges_without_elevation(test_session: SparkSession):
    """Make sure data is being filtered properly"""
    # Arrange

    test_data = [[0.0], [None]]
    test_schema = StructType([StructField("elevation", DoubleType())])
    test_df = test_session.createDataFrame(test_data, test_schema)

    tgt_data = [[0.0]]
    tgt_schema = StructType([StructField("elevation", DoubleType())])
    tgt_df = test_session.createDataFrame(tgt_data, tgt_schema)

    # Act
    res_df = EdgeSanitiser.drop_edges_without_elevation(test_df)

    # Assert
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
    res_df = EdgeSanitiser._get_tag_as_column(test_df, "access")

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


def test_flag_footways(test_session: SparkSession):
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
        [3 , {'foot': 'yes', 'sidewalk': 'yes'}],
        # Mixed
        [4 , {'foot': 'yes', 'sidewalk': 'no'}],
        # No tag set
        [4 , {}],
        # Footpath mapped separately
        [5 , {'sidewalk': 'separate'}],
        [6 , {'foot': 'yes', 'sidewalk': 'separate'}]
    ]
    # fmt: on

    test_schema = StructType(
        [
            StructField("inx", IntegerType()),
            StructField("tags", MapType(StringType(), StringType())),
        ]
    )

    test_df = test_session.createDataFrame(test_data, schema=test_schema)

    test_loader = MockEdgeSanitiser()

    # ----- Target Data -----
    # fmt: off
    #   inx, tags                                   , explicit_footway, separate_footway
    tgt_data = [
        # Explicit yes
        [0 , {'foot': 'yes'}                        , True            , False],
        [1 , {'sidewalk': 'yes'}                    , True            , False],
        # Explicit no
        [2 , {'foot': 'no'}                         , False           , False],
        # Multiple explicit yes
        [3 , {'foot': 'yes', 'sidewalk': 'yes'}     , True            , False],
        # Mixed
        [4 , {'foot': 'yes', 'sidewalk': 'no'}      , True            , False],
        # No tag set
        [4 , {}                                     , False           , False],
        # Footpath mapped separately
        [5 , {'sidewalk': 'separate'}               , False           , True],
        [6 , {'foot': 'yes', 'sidewalk': 'separate'}, True            , True]

    ]
    # fmt: on

    tgt_schema = StructType(
        [
            StructField("inx", IntegerType()),
            StructField("tags", MapType(StringType(), StringType())),
            StructField("explicit_footway", BooleanType()),
            StructField("separate_footway", BooleanType()),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, schema=tgt_schema)

    # Act #####################################################################
    res_df = test_loader.flag_footways(test_df)

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

    test_loader = MockEdgeSanitiser()

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
    #   inx, tags                                         , explicit_footway, separate_footway
    test_data = [
        # Dropped, motorway
        [0 , {'highway': 'motorway'}                      , False           , False],
        # Dropped, roundabout
        [1 , {'junction': 'roundabout'}                   , False           , False],
        # Dropped, 60 mph, no footway
        [2 , {'maxspeed': '60 mph'}                       , False           , False],
        # Dropped, 70 mph, no footway
        [3 , {'maxspeed': '70 mph'}                       , False           , False],
        # Retained, 60 mph, footway
        [4 , {'maxspeed': '60 mph', 'highway': 'other'}   , True            , False],
        # Retained, 30 mph, no (explicit) footway
        [5 , {'maxspeed': '30 mph'}                       , False           , False],
        # Dropped, motorway (with speed limit tag)
        [6 , {'maxspeed': '70 mph', 'highway': 'motorway'}, False           , False],
        # Dropped, 60 mph, separate footway
        [7 , {'maxspeed': '60 mph'}                       , False            , True]
    ]
    # fmt: on

    test_schema = StructType(
        [
            StructField("inx", IntegerType()),
            StructField("tags", MapType(StringType(), StringType())),
            StructField("explicit_footway", BooleanType()),
            StructField("separate_footway", BooleanType()),
        ]
    )

    test_df = test_session.createDataFrame(test_data, schema=test_schema)

    test_loader = MockEdgeSanitiser()

    # ----- Target Data -----
    # fmt: off
    #   inx, tags                                      , explicit_footway, separate_footway, highway
    tgt_data = [
        # Dropped, motorway
        # Dropped, roundabout
        # Dropped, 60 mph, no footway
        # Dropped, 70 mph, no footway
        # Retained, 60 mph, footway
        [4 , {'maxspeed': '60 mph', 'highway': 'other'}, True            , False           , 'other'],
        # Retained, 30 mph, no (explicit) footway
        [5 , {'maxspeed': '30 mph'}                    , False           , False           , None],
        # Dropped, motorway (with speed limit tag)
        # Dropped, 60 mph, separate footway
    ]
    # fmt: on

    tgt_schema = StructType(
        [
            StructField("inx", IntegerType()),
            StructField("tags", MapType(StringType(), StringType())),
            StructField("explicit_footway", BooleanType()),
            StructField("separate_footway", BooleanType()),
            StructField("highway", StringType()),
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

    test_loader = MockEdgeSanitiser()

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

    test_loader = MockEdgeSanitiser()

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


def test_calculate_elevation_changes(test_session: SparkSession):
    """Make sure elevation changes are correctly calculated"""
    # Arrange #################################################################

    # Test Data ---------------------------------------------------------------

    # NOTE: Records with NULL elevation explicitly dropped earlier in the
    #       pipeline

    # fmt: off
    #    src, dst, is_flat, elevation
    test_data = [
        # Single entry (hypothetical, shouldn't happen)
        [0  , 1  , False  , [1.0]],
        # Two entries
        [1  , 2  , False  , [2.0, 3.0]],
        # Three entries
        [2  , 3  , False  , [3.0, 4.0, 5.0]],
        # Four entries
        [3  , 4  , False  , [5.0, 6.0, 3.0, 4.0]],
        # Three entries, marked as flat
        [4  , 5  , True   , [3.0, 4.0, 5.0]],
    ]

    # fmt: on

    test_schema = StructType(
        [
            StructField("src", IntegerType()),
            StructField("dst", IntegerType()),
            StructField("is_flat", BooleanType()),
            StructField("elevation", ArrayType(DoubleType())),
        ]
    )

    test_df = test_session.createDataFrame(test_data, test_schema)

    test_edge_mixin = MockEdgeSanitiser()

    # Target Data -------------------------------------------------------------

    # fmt: off
    #    src, dst, is_flat, elevation_gain, elevation_loss
    target_data = [
        # Single entry (hypothetical, shouldn't happen)
        [0  , 1  , False  , 0.0           , 0.0],
        # Two entries
        [1  , 2  , False  , 1.0           , 0.0],
        # Three entries
        [2  , 3  , False  , 2.0           , 0.0],
        # Four entries
        [3  , 4  , False  , 2.0           , 3.0],
        # Three entries, marked as flat
        [4  , 5  , True   , 0.0           , 0.0],
    ]

    # fmt: on

    target_schema = StructType(
        [
            StructField("src", IntegerType()),
            StructField("dst", IntegerType()),
            StructField("is_flat", BooleanType()),
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


@patch("fell_loader.sanitised.edges.distance")
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
        source: tuple[float, float], destination: tuple[float, float]
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

    result_df = EdgeSanitiser.calculate_edge_distances(test_df)
    result_df = result_df.select(*sorted(result_df.columns))

    # Assert ##################################################################
    assertDataFrameEqual(result_df, target_df)


def test_add_reverse_edges(test_session: SparkSession):
    """Check that reverse edges are being added correctly"""
    # Arrange #################################################################

    # ----- Test Data -----
    # fmt: off
    #   way_id, way_inx, src, dst, src_lat, dst_lat, src_lon, dst_lon, oneway, elevation_gain, elevation_loss
    test_data = [
        # One way, should not be reversed
        [1    , 1      , 1  , 2  , 10.0   , 11.0   , 12.0   , 13.0   , True  , 1.0           , 2.0],
        [1    , 2      , 2  , 3  , 20.0   , 21.0   , 22.0   , 23.0   , True  , 1.0           , 2.0],
        # Bi-directional, should be reversed
        [2    , 3      , 3  , 4  , 30.0   , 31.0   , 32.0   , 33.0   , False , 1.0           , 2.0],
        [2    , 4      , 4  , 5  , 40.0   , 41.0   , 42.0   , 43.0   , False , 1.0           , 2.0],
    ]
    # fmt: on

    test_schema = StructType(
        [
            StructField("way_id", IntegerType()),
            StructField("way_inx", IntegerType()),
            StructField("src", IntegerType()),
            StructField("dst", IntegerType()),
            StructField("src_lat", DoubleType()),
            StructField("dst_lat", DoubleType()),
            StructField("src_lon", DoubleType()),
            StructField("dst_lon", DoubleType()),
            StructField("oneway", BooleanType()),
            StructField("elevation_gain", DoubleType()),
            StructField("elevation_loss", DoubleType()),
        ]
    )

    test_df = test_session.createDataFrame(test_data, schema=test_schema)

    # ----- Target Data -----
    # fmt: off
    #   way_id, way_inx, src, dst, src_lat, dst_lat, src_lon, dst_lon, oneway, elevation_gain, elevation_loss
    tgt_data = [
        # One way, should not be reversed
        [1    , 1      , 1  , 2  , 10.0   , 11.0   , 12.0   , 13.0   , True  , 1.0           , 2.0],
        [1    , 2      , 2  , 3  , 20.0   , 21.0   , 22.0   , 23.0   , True  , 1.0           , 2.0],
        # Bi-directional, should be reversed
        [2    , 3      , 3  , 4  , 30.0   , 31.0   , 32.0   , 33.0   , False , 1.0           , 2.0],
        [2    , 4      , 4  , 5  , 40.0   , 41.0   , 42.0   , 43.0   , False , 1.0           , 2.0],
        [-2   , -3     , 4  , 3  , 31.0   , 30.0   , 33.0   , 32.0   , False , 2.0           , 1.0],
        [-2   , -4     , 5  , 4  , 41.0   , 40.0   , 43.0   , 42.0   , False , 2.0           , 1.0],
    ]
    # fmt: on

    tgt_schema = StructType(
        [
            StructField("way_id", IntegerType()),
            StructField("way_inx", IntegerType()),
            StructField("src", IntegerType()),
            StructField("dst", IntegerType()),
            StructField("src_lat", DoubleType()),
            StructField("dst_lat", DoubleType()),
            StructField("src_lon", DoubleType()),
            StructField("dst_lon", DoubleType()),
            StructField("oneway", BooleanType()),
            StructField("elevation_gain", DoubleType()),
            StructField("elevation_loss", DoubleType()),
        ]
    )

    tgt_df = test_session.createDataFrame(tgt_data, schema=tgt_schema)

    test_edge_sanitiser = MockEdgeSanitiser()

    # Act #####################################################################
    res_df = test_edge_sanitiser.add_reverse_edges(test_df)

    # Assert ##################################################################
    assertDataFrameEqual(res_df, tgt_df)


@pytest.mark.skip("High effort, low value")
def test_run():
    """Check that all expected function calls are generated"""
