import pytest
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext


@pytest.fixture
def test_session() -> SQLContext:
    conf = SparkConf()
    conf = conf.setAppName("refinement")
    conf = conf.setMaster("local[10]")
    conf = conf.set("spark.driver.memory", "2g")
    conf = conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
    conf = conf.set("spark.sql.files.maxPartitionBytes", "1048576")

    context = SparkContext(conf=conf)
    context.setLogLevel("WARN")
    spark = context.getOrCreate()
    sql = SQLContext(spark)

    return sql
