"""Primary execution script (for now). Triggers ingestion of LIDAR and OSM
data, joins the two datasets together to create a single augmented graph."""

# ruff: noqa: ERA001, F401, E501

import os
from fell_loader import (
    BelterLoader,
    GraphContractor,
    GraphEnricher,
    LidarLoader,
    OsmLoader,
)
from pyspark.sql import SparkSession

# TODO: Build in some more detailed logging throughout

if __name__ == "__main__":
    # Raw Data ################################################################

    lidar_loader = LidarLoader()
    self = lidar_loader
    lidar_loader.load()
    del lidar_loader

    # Config set for execution on personal devices, not tuned for cloud
    spark = (
        SparkSession.builder.appName("fell_finder")  # type: ignore
        .config("spark.master", "local[*]")
        .config("spark.driver.memory", "32g")
        .config("spark.driver.memoryOverhead", "8g")
        .config("spark.sql.files.maxPartitionBytes", "67108864")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "512")
        .config(
            "spark.local.dir", os.path.join(os.environ["FF_DATA_DIR"], "temp")
        )
        .config("spark.log.level", "WARN")
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
        .getOrCreate()
    )

    osm_loader = OsmLoader(spark)
    osm_loader.load()
    del osm_loader

    # Combine Datasets ########################################################
    graph_enricher = GraphEnricher(spark)
    graph_enricher.enrich()
    del graph_enricher

    # Optimise Graph ##########################################################
    graph_contractor = GraphContractor(spark)
    graph_contractor.contract()
    del graph_contractor

    spark.stop()

    # Load to Postgres ########################################################

    db_loader = BelterLoader()
    db_loader.init_db()
    db_loader.load_nodes()
    db_loader.load_edges()
