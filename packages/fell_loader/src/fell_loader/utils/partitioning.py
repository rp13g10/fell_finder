"""Utility functions which are used across the different modules within this
package."""

import os

import polars as pl
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType

PTN_EDGE_SIZE_M = int(os.environ["FF_ING_PTN_SIZE"])


def add_bng_partition_to_spark_df(df: DataFrame) -> DataFrame:
    """Create a new ptn column based on the contents of the easting and
    northing columns in the provided dataframe. The size of the partition will
    be determined by the FF_ING_PTN_SIZE environment variable. This sets the
    size of the grid square which defines a partition. E.g. a value of 10000
    would set each partition to a square of side 10000m.

    Args:
        df: A dataframe containing an easting column and a northing column

    Returns:
        A copy of the input dataset with an additional ptn column

    """
    easting_ptn = (
        (F.col("easting") / PTN_EDGE_SIZE_M)
        .astype(IntegerType())
        .astype(StringType())
    )
    northing_ptn = (
        (F.col("northing") / PTN_EDGE_SIZE_M)
        .astype(IntegerType())
        .astype(StringType())
    )

    df = df.withColumn("ptn", F.concat(easting_ptn, F.lit("_"), northing_ptn))
    df = df.withColumn("ptn", F.replace(F.col("ptn"), F.lit("-"), F.lit("n")))

    return df


def add_bng_partition_to_polars_df(df: pl.DataFrame) -> pl.DataFrame:
    """Create a new ptn column based on the contents of the easting and
    northing columns in the provided dataframe. The size of the partition will
    be determined by the FF_ING_PTN_SIZE environment variable. This sets the
    size of the grid square which defines a partition. E.g. a value of 10000
    would set each partition to a square of side 10000m.

    Args:
        df: A dataframe containing an easting column and a northing column

    Returns:
        A copy of the input dataset with an additional ptn column

    """
    easting_ptn = (
        (pl.col("easting") / PTN_EDGE_SIZE_M)
        .cast(pl.Int32())
        .cast(pl.String())
    )
    northing_ptn = (
        (pl.col("northing") / PTN_EDGE_SIZE_M)
        .cast(pl.Int32())
        .cast(pl.String())
    )

    df = df.with_columns(
        pl.concat_str([easting_ptn, pl.lit("_"), northing_ptn]).alias("ptn")
    )
    df = df.with_columns(
        pl.col("ptn").str.replace("-", "n", literal=True, n=2).alias("ptn")
    )

    return df


def add_coords_partition_to_spark_df(
    df: DataFrame, lat_col: str = "lat", lon_col: str = "lon"
) -> DataFrame:
    """Create a new ptn column based on the contents of the easting and
    northing columns in the provided dataframe. The size of the partition will
    be determined by the FF_ING_PTN_SIZE environment variable. This sets the
    size of the grid square which defines a partition. E.g. a value of 10000
    would set each partition to a square of side 10000m.

    Args:
        df: A dataframe containing an easting column and a northing column
        lat_col: Column containing latitudes. Defaults to 'lat'.
        lon_col: Column containing longitudes. Defaults to 'lon'.

    Returns:
        A copy of the input dataset with an additional ptn column

    """
    lat_ptn = F.col(lat_col).astype(IntegerType()).astype(StringType())
    lon_ptn = F.col(lon_col).astype(IntegerType()).astype(StringType())

    df = df.withColumn("ptn", F.concat(lat_ptn, F.lit("_"), lon_ptn))
    df = df.withColumn("ptn", F.replace(F.col("ptn"), F.lit("-"), F.lit("n")))

    return df
