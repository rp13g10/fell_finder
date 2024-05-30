import pytest
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession


@pytest.fixture
def test_session() -> SparkSession:
    spark = SparkSession.builder.appName(  # type: ignore
        "fell_finder_tests"
    ).getOrCreate()

    return spark
