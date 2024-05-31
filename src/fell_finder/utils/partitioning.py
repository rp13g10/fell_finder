"""Utility functions which are used across the different modules within this
package"""

from typing import Tuple

import polars as pl
from bng_latlon import WGS84toOSGB36
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# NOTE: Polars doesn't seem to support bankers rounding, so all of these
#       functions use half-up rounding


def _round_half_up(num: int | float) -> int:
    """Only works for positive numbers, but that's fine for our use case"""
    return int(num + 0.5)


def get_coordinates(lat: float, lon: float) -> Tuple[int, int]:
    """For a given latitude and longitude, fetch the corresponding easting and
    northing. Coordinates are returned as integers, as this is how the LIDAR
    data is stored.

    Args:
        lat (float): The latitude for the target point
        lon (float): The longitude for the target point

    Returns:
        Tuple[int, int]: The easting and northing for the target point
    """

    easting, northing = WGS84toOSGB36(lat, lon)
    easting = _round_half_up(easting)
    northing = _round_half_up(northing)
    return easting, northing


def get_partitions(easting: int, northing: int) -> Tuple[int, int]:
    """For a given easting and northing, fetch the corresponding easting and
    northing partitions.

    Args:
        easting (int): The easting for the target point
        northing (int): The northing for the target point

    Returns:
        Tuple[int, int]: The easting partition and the northing partition
    """

    easting_ptn = _round_half_up(easting / 1000)
    northing_ptn = _round_half_up(northing / 1000)

    return easting_ptn, northing_ptn


def add_partitions_to_spark_df(df: DataFrame) -> DataFrame:
    """For a provided dataframe with both easting and northing columns, assign
    each record to corresponding easting and northing partitions. These will be
    stored in the easting_ptn and northing_ptn columns respectively.

    Args:
        df (DataFrame): A dataframe containing both `easting` and `northing`
          columns

    Returns:
        DataFrame: A copy of the input dataset with additional `easting_ptn`
          and `northing_ptn` columns
    """
    df = df.withColumn(
        "easting_ptn", F.round(F.col("easting") / 1000).astype(IntegerType())
    )

    df = df.withColumn(
        "northing_ptn",
        F.round(F.col("northing") / 1000).astype(IntegerType()),
    )

    return df


def add_partitions_to_polars_df(df: pl.DataFrame) -> pl.DataFrame:
    """For a provided dataframe with both easting and northing columns, assign
    each record to corresponding easting and northing partitions. These will be
    stored in the easting_ptn and northing_ptn columns respectively.

    Args:
        df (pl.DataFrame): A dataframe containing both `easting` and `northing`
          columns

    Returns:
        pl.DataFrame: A copy of the input dataset with additional `easting_ptn`
          and `northing_ptn` columns
    """
    df = df.with_columns(
        (pl.col("easting") / 1000)
        .round()
        .cast(pl.Int32())
        .alias("easting_ptn"),
        (pl.col("northing") / 1000)
        .round()
        .cast(pl.Int32())
        .alias("northing_ptn"),
    )

    return df
