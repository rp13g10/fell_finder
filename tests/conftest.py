"""Configuration file for pytest"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture
def test_session() -> SparkSession:
    """Creates a local SparkSession which can be accessed as a fixture
    across all tests defined in this directory.

    Returns:
        A local SparkSession
    """
    spark = (
        SparkSession.builder.appName(  # type: ignore
            "fell_finder_tests"
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.rdd.compress", "False")
        .config("spark.rdd.compress", "False")
        .config("spark.shuffle.compress", "False")
        .config("spark.shuffle.spill.compress", "False")
        .config("spark.dynamicAllocation.enabled", "False")
        .config(
            "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
        )
        .getOrCreate()
    )

    return spark
