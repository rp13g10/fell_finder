"""Primary execution script (for now). Triggers ingestion of LIDAR and OSM
data, joins the two datasets together to create a single augmented graph.
"""

# ruff: noqa: ERA001, E501, F401

import logging
import os

from delta import configure_spark_with_delta_pip
from fell_loader.landing import (
    LidarLoader,
    OsmLoader,
)
from fell_loader.staging import EdgeStager, NodeStager
from pyspark.sql import SparkSession

# TODO: Build in some more detailed logging throughout

if __name__ == "__main__":
    # Logging #################################################################
    logging.getLogger("py4j").setLevel(logging.ERROR)
    if os.path.exists("fell_loader.log"):
        os.remove("fell_loader.log")
    logging.basicConfig(
        filename="fell_loader.log",
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )

    # Other Setup #############################################################
    DATA_DIR = os.environ["FF_DATA_DIR"]

    # Landing #################################################################

    lidar_loader = LidarLoader()
    self = lidar_loader
    lidar_loader.load()
    del lidar_loader

    # Config set for execution on personal devices, not tuned for cloud
    builder = (
        SparkSession.builder.appName("fell_loader")
        .config("spark.master", "local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.memoryOverhead", "1g")
        .config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.log.level", "WARN")
        .config("spark.local.dir", os.path.join(DATA_DIR, "temp", "spark"))
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # osm_loader = OsmLoader(spark)
    # osm_loader.load()
    # del osm_loader

    # Staging #################################################################

    node_stager = NodeStager(spark)
    node_stager.load()
    del node_stager

    edge_stager = EdgeStager(spark)
    edge_stager.load()
    del edge_stager

    # # Combine Datasets ########################################################
    # graph_enricher = GraphEnricher(spark)
    # graph_enricher.enrich()
    # del graph_enricher

    # # Optimise Graph ##########################################################
    # graph_contractor = GraphContractor(spark)
    # graph_contractor.contract()
    # del graph_contractor

    # spark.stop()

    # # Load to Postgres ########################################################

    # db_loader = BelterLoader()
    # db_loader.init_db()
    # db_loader.load_nodes()
    # db_loader.load_edges()
