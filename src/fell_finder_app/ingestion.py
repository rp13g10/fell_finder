"""Primary execution script (for now). Triggers ingestion of LIDAR and OSM
data, joins the two datasets together to create a single augmented graph.
"""

# ruff: noqa: ERA001, E501, F401, RUF100

import logging
import os
import shutil
from pathlib import Path

from delta import configure_spark_with_delta_pip
from fell_loader.landing import (
    LidarLoader,
    OsmLoader,
)
from fell_loader.optimised import GraphOptimiser
from fell_loader.postgres import GraphUploader
from fell_loader.sanitised import EdgeSanitiser, NodeSanitiser
from fell_loader.staging import EdgeStager, NodeStager
from pyspark.sql import SparkSession

from fell_finder_app.utils import set_up_logging

# TODO: Resolve issue where removal of temp folder kills write_bounds_to_parquet

if __name__ == "__main__":
    # Initial Setup ###########################################################
    DATA_DIR = Path(os.environ["FF_DATA_DIR"])
    set_up_logging()

    logger = logging.getLogger(__name__)

    # Landing #################################################################

    logger.info("Processing raw LIDAR data")
    lidar_loader = LidarLoader()
    self = lidar_loader
    lidar_loader.run()
    del lidar_loader

    # Config set for execution on personal devices, not tuned for cloud
    logger.debug("Creating local spark context")
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
        .config("spark.local.dir", (DATA_DIR / "temp" / "spark").as_posix())
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    logger.info("Processing raw OSM data")
    osm_loader = OsmLoader(spark)
    osm_loader.run()
    del osm_loader

    # Staging #################################################################

    logger.info("Loading data to the staging layer")
    node_stager = NodeStager(spark)
    node_stager.run()
    del node_stager

    edge_stager = EdgeStager(spark)
    edge_stager.run()
    del edge_stager

    # Sanitising ##############################################################

    logger.info("Loading data to the sanitised layer")
    edge_sanitiser = EdgeSanitiser(spark)
    edge_sanitiser.run()
    del edge_sanitiser

    node_sanitiser = NodeSanitiser(spark)
    node_sanitiser.run()
    del node_sanitiser

    # Optimising ##############################################################

    logger.info("Loading data to the optimised layer")
    graph_optimiser = GraphOptimiser(spark)
    graph_optimiser.run()
    del graph_optimiser

    spark.stop()

    # Load to Postgres ########################################################

    logger.info("Uploading optimised data to postgres")
    graph_uploader = GraphUploader()
    graph_uploader.run()
    del graph_uploader

    # Clear temp data #########################################################

    logger.info("Clearing out temp files")
    shutil.rmtree(
        path=(DATA_DIR / "temp").as_posix(),
    )

    logger.info("Data load completed")
