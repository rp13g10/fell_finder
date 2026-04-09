"""Unit tests for functions relating to partitioning of data"""

from fell_loader.utils.partitioning import (
    add_coords_partition_to_spark_df,
)
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.testing.utils import assertDataFrameEqual


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
