"""Unit tests for functions relating to partitioning of data"""

from unittest.mock import patch

import polars as pl
from polars.testing import assert_frame_equal
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.testing.utils import assertDataFrameEqual

from fell_loader.utils.partitioning import (
    add_bng_partition_to_polars_df,
    add_bng_partition_to_spark_df,
    add_coords_partition_to_spark_df,
)


@patch("fell_loader.utils.partitioning.PTN_EDGE_SIZE_M", 10000)
def test_add_bng_partition_to_spark_df(test_session: SparkSession):
    """Check that partitions are correctly added to spark dataframes"""
    # Arrange #################################################################

    # ----- Test Data -----
    # fmt: off
    _ = (
        ['inx', 'easting', 'northing'])

    test_data = [
        [0    , 617280   , 4938270],
        [1    , 4938270  , -617280],
        [2    , -502500  , 507500],
        [3    , -502500  , -507500],
        [4    , None     , None]
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
        ['inx', 'easting', 'northing', 'ptn'])

    tgt_data = [
        [0    , 617280   , 4938270   , '61_493'],
        [1    , 4938270  , -617280   , '493_n61'],
        [2    , -502500  , 507500    , 'n50_50'],
        [3    , -502500  , -507500   , 'n50_n50'],
        [4    , None     , None      , None]
    ]
    # fmt: on

    tgt_schema = StructType(
        [
            StructField("inx", IntegerType()),
            StructField("easting", IntegerType()),
            StructField("northing", IntegerType()),
            StructField("ptn", StringType()),
        ]
    )

    tgt_df = test_session.createDataFrame(data=tgt_data, schema=tgt_schema)

    # Act
    res_df = add_bng_partition_to_spark_df(test_df)

    # Assert
    assertDataFrameEqual(tgt_df, res_df)


@patch("fell_loader.utils.partitioning.PTN_EDGE_SIZE_M", 10000)
def test_add_bng_partition_to_polars_df():
    """Check that partitions are correctly added to polars dataframes"""
    # Arrange #################################################################

    # ----- Test Data -----

    # fmt: off
    _ = (
        ['inx', 'easting', 'northing'])

    test_data = [
        [0    , 617280   , 4938270],
        [1    , 4938270  , -617280],
        [2    , -502500  , 507500],
        [3    , -502500  , -507500],
        [4    , None     , None]
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
        ['inx', 'easting', 'northing', 'ptn'])

    tgt_data = [
        [0    , 617280   , 4938270   , '61_493'],
        [1    , 4938270  , -617280   , '493_n61'],
        [2    , -502500  , 507500    , 'n50_50'],
        [3    , -502500  , -507500   , 'n50_n50'],
        [4    , None     , None      , None]
    ]
    # fmt: on

    tgt_schema = {
        "inx": pl.Int32(),
        "easting": pl.Int32(),
        "northing": pl.Int32(),
        "ptn": pl.String(),
    }

    tgt_df = pl.DataFrame(data=tgt_data, schema=tgt_schema, orient="row")

    # Act
    res_df = add_bng_partition_to_polars_df(test_df)

    # Assert
    assert_frame_equal(tgt_df, res_df)


@patch("fell_loader.utils.partitioning.PTN_EDGE_SIZE_M", 10000)
def test_add_coords_partition_to_spark_df(test_session: SparkSession):
    """Check that partitions are correctly added to polars dataframes"""
    # Arrange #################################################################

    # ----- Test Data -----
    # fmt: off
    _ = (
        ['inx', 'lat', 'lon'])

    test_data = [
        [0    , 50.0 , 50.0],
        [1    , 45.3 , -45.3],
        [2    , -40.6, 40.6],
        [3    , -35.5, -35.5],
        [4    , None , None]
    ]
    # fmt: on

    test_schema = StructType(
        [
            StructField("inx", IntegerType()),
            StructField("lat", DoubleType()),
            StructField("lon", DoubleType()),
        ]
    )

    test_df = test_session.createDataFrame(data=test_data, schema=test_schema)

    # ----- Target Data -----=
    # fmt: off
    _ = (
        ['inx', 'lat', 'lon', 'ptn'])

    tgt_data = [
        [0    , 50.0 , 50.0 , '50_50'],
        [1    , 45.3 , -45.3, '45_n45'],
        [2    , -40.6, 40.6 , 'n40_40'],
        [3    , -35.5, -35.5, 'n35_n35'],
        [4    , None , None , None]
    ]
    # fmt: on

    tgt_schema = StructType(
        [
            StructField("inx", IntegerType()),
            StructField("lat", DoubleType()),
            StructField("lon", DoubleType()),
            StructField("ptn", StringType()),
        ]
    )

    tgt_df = test_session.createDataFrame(data=tgt_data, schema=tgt_schema)

    # Act
    res_df = add_coords_partition_to_spark_df(test_df)

    # Assert
    assertDataFrameEqual(tgt_df, res_df)
