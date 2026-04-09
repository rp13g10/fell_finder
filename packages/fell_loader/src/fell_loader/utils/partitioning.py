"""Utility functions which are used across the different modules within this
package.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType


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
