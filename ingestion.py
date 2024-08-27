"""Primary execution script (for now). Triggers ingestion of LIDAR and OSM
data, joins the two datasets together to create a single augmented graph."""

import os
from pyspark.sql import SparkSession

from fell_finder.ingestion import (
    LidarLoader,
    OsmLoader,
    GraphEnricher,
    GraphContractor,
)
from fell_finder import app_config

DATA_DIR = app_config["data_dir"]

lidar_loader = LidarLoader(DATA_DIR)
lidar_loader.load()
del lidar_loader

osm_loader = OsmLoader(DATA_DIR)
osm_loader.load()
del osm_loader

# Config set for testing on personal laptop, will need tuning for cloud envs
spark = (
    SparkSession.builder.appName("fell_finder")  # type: ignore
    .config("spark.master", "local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.driver.memoryOverhead", "2g")
    .config("spark.executor.memory", "8g")
    .config("spark.executor.memoryOverhead", "2g")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
    .config("spark.sql.files.maxPartitionBytes", "1048576")
    .config("spark.sql.hive.filesourcePartitionFileCacheSize", "1048576000")
    .config("spark.local.dir", os.path.join(DATA_DIR, "temp"))
    .getOrCreate()
)

graph_enricher = GraphEnricher(DATA_DIR, spark)
graph_enricher.enrich()
del graph_enricher

graph_contractor = GraphContractor(DATA_DIR, spark)
graph_contractor.contract()
del graph_contractor

spark.stop()
