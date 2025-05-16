"""Primary execution script (for now). Triggers ingestion of LIDAR and OSM
data, joins the two datasets together to create a single augmented graph."""

import os

from fell_loader import (
    BelterLoader,
    GraphContractor,
    GraphEnricher,
    LidarLoader,
    OsmLoader,
)
from pyspark.sql import SparkSession

if __name__ == "__main__":
    DATA_DIR = os.environ["FF_DATA_DIR"]

    # Raw Data ################################################################
    # lidar_loader = LidarLoader(DATA_DIR)
    # lidar_loader.load()
    # del lidar_loader

    osm_loader = OsmLoader()
    osm_loader.load()
    del osm_loader

    # Combine Datasets ########################################################
    # Config set for testing on personal laptop, will need tuning for the cloud
    # spark = (
    #     SparkSession.builder.appName("fell_finder")  # type: ignore
    #     .config("spark.master", "local[*]")
    #     .config("spark.driver.memory", "8g")
    #     .config("spark.driver.memoryOverhead", "2g")
    #     .config("spark.executor.memory", "40g")
    #     .config("spark.executor.memoryOverhead", "10g")
    #     .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
    #     .config("spark.sql.files.maxPartitionBytes", "1048576")
    #     .config(
    #         "spark.sql.hive.filesourcePartitionFileCacheSize", "1048576000"
    #     )
    #     .config("spark.local.dir", os.path.join(DATA_DIR, "temp"))
    #     .config("spark.log.level", "WARN")
    #     .getOrCreate()
    # )

    # Optimise Graph ##########################################################
    # graph_enricher = GraphEnricher(DATA_DIR, spark)
    # graph_enricher.enrich()
    # del graph_enricher

    # graph_contractor = GraphContractor(DATA_DIR, spark)
    # graph_contractor.contract()
    # del graph_contractor

    # spark.stop()

    # Load to Postgres ########################################################

    # db_loader = BelterLoader(DATA_DIR)
    # db_loader.init_db()
    # db_loader.load_nodes()
    # db_loader.load_edges()
