"""Primary execution script (for now). Triggers ingestion of LIDAR and OSM
data, joins the two datasets together to create a single augmented graph."""

from pyspark.sql import SparkSession

from fell_finder.ingestion import LidarLoader, OsmLoader
from fell_finder.enrichment import GraphEnricher

DATA_DIR = "/home/ross/repos/fell_finder/data"

lidar_loader = LidarLoader(DATA_DIR)
lidar_loader.load()
del lidar_loader

osm_loader = OsmLoader(DATA_DIR)
osm_loader.load()
del osm_loader

# Config set for testing on personal laptop, will need tuning for cloud envs
spark = (
    SparkSession.builder.appName(  # type: ignore
        "fell_finder"
    )
    .config("spark.master", "local[10]")
    .config("spark.driver.memory", "2g")
    .config("spark.driver.memoryOverhead", "1g")
    .config("spark.executor.memory", "4g")
    .config("spark.executor.memoryOverhead", "1g")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
    .config("spark.sql.files.maxPartitionBytes", "1048576")
    .getOrCreate()
)

graph_enricher = GraphEnricher(DATA_DIR, spark)
graph_enricher.enrich()
