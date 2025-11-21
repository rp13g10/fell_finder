"""Initial checks to see if any duplicates have been introduced during the
initial loading of node/edge data
"""

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

builder = (
    SparkSession.builder.appName("fell_loader")
    .config("spark.master", "local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.driver.memoryOverhead", "1g")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.log.level", "WARN")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

edges_tbl = DeltaTable.forPath(
    spark, "/home/ross/repos/fell_finder/data/staging/edges"
).alias("edges")

edges_df = edges_tbl.toDF()

edges_df.groupBy("src", "dst").agg(
    F.count(F.lit(1)).alias("num_records")
).groupBy("num_records").agg(F.count(F.lit(1)).alias("num_edges")).orderBy(
    "num_records"
).show()
