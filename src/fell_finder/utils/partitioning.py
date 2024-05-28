"""Utility functions which are used across the different modules within this
package"""

from typing import Tuple

from pyspark.sql import DataFrame, functions as F
from bng_latlon import WGS84toOSGB36


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
    easting = round(easting)
    northing = round(northing)

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

    easting_ptn = round(easting / 1000)
    northing_ptn = round(northing / 1000)

    return easting_ptn, northing_ptn


def add_partitions_to_df(df: DataFrame) -> DataFrame:
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
    df = df.withColumn("easting_ptn", F.round(F.col("easting") / 1000))

    df = df.withColumn("northing_ptn", F.round(F.col("northing") / 1000))

    return df


# def get_partitions_for_df(df: pd.DataFrame) -> pd.DataFrame:
#     """For a pandas dataframe containg 'easting' and 'northing' columns,
#     create the corresponding 'easting_ptn' and 'northing_ptn' columns.
#     The values created in these colummns correspond to the output from
#     get_partitions, but this implementation should be faster than using
#     pd.DataFrame.apply.

#     Args:
#         df (pd.DataFrame): A dataframe containing and 'easting' and a
#           'northing' column

#     Returns:
#         pd.DataFrame: A copy of the input dataset with additional
#           'easting_ptn' and 'northing_ptn' columns"""

#     df.loc[:, "easting_ptn"] = (df["easting"] / 1000).round().astype(int)
#     df.loc[:, "northing_ptn"] = (df["northing"] / 1000).round().astype(int)

#     return df
