"""Unit tests for functions relating to partitioning of data"""

from chispa import assert_df_equality
from unittest.mock import patch, MagicMock
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, IntegerType

from fell_finder.utils.partitioning import (
    get_coordinates,
    get_partitions,
    add_partitions_to_df,
)


@patch("fell_finder.utils.partitioning.WGS84toOSGB36")
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
        test_easting = 123456
        test_northing = 987654

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
        test_easting = 100500
        test_northing = 101500

        target_easting_ptn = 100
        target_northing_ptn = 102

        # Act
        result_easting_ptn, result_northing_ptn = get_partitions(
            test_easting, test_northing
        )

        # Assert
        assert result_easting_ptn == target_easting_ptn
        assert result_northing_ptn == target_northing_ptn


def test_add_partitions_to_df(test_session: SQLContext):
    """Check that partitions are correctly added to spark dataframes"""
    # Arrange #################################################################

    # ----- Test Data -----
    # fmt: off
    _ = (
        ['inx', 'easting', 'northing'])

    test_data = [
        # Standard behaviour
        [0    , 123456   , 987654],
        [1    , 987654   , 123456],
        # Midpoint behaviour
        [2    , 100500   , 101500],
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
        [0    , 123456   , 987654    , 123          , 988],
        [1    , 987654   , 123456    , 988          , 123],
        [2    , 100500   , 101500    , 100          , 102],
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
    res_df = add_partitions_to_df(test_df)

    # Assert
    assert_df_equality(tgt_df, res_df)
