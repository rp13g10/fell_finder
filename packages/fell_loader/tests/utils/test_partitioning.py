"""Unit tests for functions relating to partitioning of data"""

from unittest.mock import MagicMock, patch

import polars as pl
from polars.testing import assert_frame_equal
from pyspark.sql import SQLContext
from pyspark.sql.types import IntegerType, StructField, StructType
from pyspark.testing.utils import assertDataFrameEqual

from fell_loader.utils.partitioning import (
    add_partitions_to_polars_df,
    add_partitions_to_spark_df,
    get_coordinates,
    get_partitions,
)


@patch("fell_loader.utils.partitioning.WGS84toOSGB36")
def test_get_coordinates(mock_wgs84toosgb36: MagicMock):
    """Check that the expected function output is returned and rounded
    correctly"""

    # Arrange
    mock_wgs84toosgb36.side_effect = lambda x, y: (x, y)

    test_lat = 50.1
    test_lon = 50.9

    target_easting = 50
    target_northing = 51

    # Act
    result_easting, result_northing = get_coordinates(test_lat, test_lon)

    # Assert
    assert result_easting == target_easting
    assert result_northing == target_northing


class TestGetPartitions:
    """Make sure partitions are correctly generated in all cases"""

    def test_standard_behaviour(self):
        """Check that the partitions are being generated correctly"""
        # Arrange
        test_easting = 617280
        test_northing = 4938270

        target_easting_ptn = 123
        target_northing_ptn = 988

        # Act
        result_easting_ptn, result_northing_ptn = get_partitions(
            test_easting, test_northing
        )

        # Assert
        assert result_easting_ptn == target_easting_ptn
        assert result_northing_ptn == target_northing_ptn

    def test_midpoint_behaviour(self):
        """Check that the partitions are being generated correctly"""
        # Arrange
        test_easting = 502500
        test_northing = 507500

        target_easting_ptn = 101
        target_northing_ptn = 102

        # Act
        result_easting_ptn, result_northing_ptn = get_partitions(
            test_easting, test_northing
        )

        # Assert
        assert result_easting_ptn == target_easting_ptn
        assert result_northing_ptn == target_northing_ptn


def test_add_partitions_to_spark_df(test_session: SQLContext):
    """Check that partitions are correctly added to spark dataframes"""
    # Arrange #################################################################

    # ----- Test Data -----
    # fmt: off
    _ = (
        ['inx', 'easting', 'northing'])

    test_data = [
        # Standard behaviour
        [0    , 617280   , 4938270],
        [1    , 4938270  , 617280],
        # Midpoint behaviour
        [2    , 502500   , 507500],
        # Missing data
        [3    , None     , None]
    ]
    # fmt: on

    test_schema = StructType(
        [
            StructField("inx", IntegerType()),
            StructField("easting", IntegerType()),
            StructField("northing", IntegerType()),
        ]
    )

    test_df = test_session.createDataFrame(data=test_data, schema=test_schema)

    # ----- Target Data -----=
    # fmt: off
    _ = (
        ['inx', 'easting', 'northing', 'easting_ptn', 'northing_ptn'])

    tgt_data = [
        [0    , 617280   , 4938270    , 123          , 988],
        [1    , 4938270  , 617280    , 988          , 123],
        [2    , 502500   , 507500    , 101          , 102],
        [3    , None     , None      , None         , None]
    ]
    # fmt: on

    tgt_schema = StructType(
        [
            StructField("inx", IntegerType()),
            StructField("easting", IntegerType()),
            StructField("northing", IntegerType()),
            StructField("easting_ptn", IntegerType()),
            StructField("northing_ptn", IntegerType()),
        ]
    )

    tgt_df = test_session.createDataFrame(data=tgt_data, schema=tgt_schema)

    # Act
    res_df = add_partitions_to_spark_df(test_df)

    # Assert
    assertDataFrameEqual(tgt_df, res_df)


def test_add_partitions_to_polars_df():
    """Check that partitions are correctly added to polars dataframes"""
    # Arrange #################################################################

    # ----- Test Data -----

    # fmt: off
    _ = (
        ['inx', 'easting', 'northing'])

    test_data = [
        # Standard behaviour
        [0    , 617280   , 4938270],
        [1    , 4938270  , 617280],
        # Midpoint behaviour
        [2    , 502500   , 507500],
        # Missing data
        [3    , None     , None]
    ]
    # fmt: on

    test_schema = {
        "inx": pl.Int32(),
        "easting": pl.Int32(),
        "northing": pl.Int32(),
    }

    test_df = pl.DataFrame(data=test_data, schema=test_schema, orient="row")

    # ----- Target Data -----=
    # fmt: off
    _ = (
        ['inx', 'easting', 'northing', 'easting_ptn', 'northing_ptn'])

    tgt_data = [
        [0    , 617280   , 4938270   , 123          , 988],
        [1    , 4938270  , 617280    , 988          , 123],
        [2    , 502500   , 507500    , 101          , 102],
        [3    , None     , None      , None         , None]
    ]
    # fmt: on

    tgt_schema = {
        "inx": pl.Int32(),
        "easting": pl.Int32(),
        "northing": pl.Int32(),
        "easting_ptn": pl.Int32(),
        "northing_ptn": pl.Int32(),
    }

    tgt_df = pl.DataFrame(data=tgt_data, schema=tgt_schema, orient="row")

    # Act
    res_df = add_partitions_to_polars_df(test_df)

    # Assert
    assert_frame_equal(tgt_df, res_df)
