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
from fell_loader.optimising import GraphOptimiser
from fell_loader.sanitising import EdgeSanitiser, NodeSanitiser
from fell_loader.staging import EdgeStager, NodeStager
from fell_loader.uploading import GraphUploader
from pyspark.sql import SparkSession

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

    # lidar_loader = LidarLoader()
    # self = lidar_loader
    # lidar_loader.load()
    # del lidar_loader

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

    # TODO: Set this to skip load if osm file has not changed since last run

    # osm_loader = OsmLoader(spark)
    # osm_loader.load()
    # del osm_loader

    # Staging #################################################################

    # node_stager = NodeStager(spark)
    # node_stager.load()
    # del node_stager

    # edge_stager = EdgeStager(spark)
    # edge_stager.load()
    # del edge_stager

    # Sanitising ##############################################################

    # edge_sanitiser = EdgeSanitiser(spark)
    # edge_sanitiser.load()
    # del edge_sanitiser

    # node_sanitiser = NodeSanitiser(spark)
    # node_sanitiser.load()
    # del node_sanitiser

    # Optimising ##############################################################

    graph_optimiser = GraphOptimiser(spark)
    graph_optimiser.contract()
    del graph_optimiser

    spark.stop()

    # # Load to Postgres ########################################################

    graph_uploader = GraphUploader()
    graph_uploader.init_db()
    graph_uploader.upload_nodes()
    graph_uploader.upload_edges()
