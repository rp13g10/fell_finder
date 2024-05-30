"""Primary execution script (for now). Triggers ingestion of LIDAR and OSM
data, joins the two datasets together to create a single augmented graph."""

from pyspark.sql import SparkSession

from fell_finder.ingestion import LidarLoader

DATA_DIR = "/home/ross/repos/fell_finder/data"

# Generate internal sparkcontext
spark = (
    SparkSession.builder.appName("fell_finder")  # type: ignore
    .config("spark.driver.memory", "2g")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
    .config("spark.sql.files.maxPartitionBytes", "1048576")
    .getOrCreate()
)

lidar_parser = LidarLoader(spark, DATA_DIR)
lidar_parser.load()
