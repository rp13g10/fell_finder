"""Defines the BaseStager class, which defines shared functionality for all
pipelines in the staging layer
"""

import contextlib
import logging
import shutil
from typing import Literal

from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from fell_loader.base import BaseSparkLoader

logger = logging.getLogger(__name__)


class BaseStager(BaseSparkLoader):
    """Defines shared functionality for all pipelines in the staging
    layer
    """

    def __init__(self, spark: SparkSession) -> None:
        super().__init__(spark)

        self.removals_applied = False
        self.updates_applied = False

        self.remaining_count = -1
        self.current_chunk_size = -1

    def check_for_lidar_update(self) -> bool:
        """Determine whether there has been any change in the LIDAR data
        available since the last run. This cross-checks the current contents
        of the 'landing/lidar' folder in the data directory against
        'last_run_lidar.txt' in the 'staging' folder in the data directory.
        This file is written by the `EdgeStager` component at the end of each
        successful run.

        Returns:
            True if there has been a change in the available LIDAR data, False
            if there hasn't
        """
        # TODO: Move this out into utils or a base class to prevent duplication
        #       of code

        lidar_files = (self.data_dir / "landing" / "lidar").glob("*.parquet")
        lidar_files = (path.as_posix() for path in lidar_files)

        last_run_loc = self.data_dir / "staging" / "last_run_lidar.txt"
        if not last_run_loc.exists():
            logger.debug("No record of lidar files from previous runs")
            last_run_files = ""
        else:
            logger.debug("Found record of previous lidar runs")
            last_run_files = last_run_loc.read_text()

        updated = str(sorted(lidar_files)) != last_run_files.strip()
        logger.debug(f"Lidar files updated: {updated}")
        return updated

    def clear_data_without_elevation(
        self, dataset: Literal["nodes", "edges"]
    ) -> None:
        """Drop any records which don't currently have a value for elevation.
        This will cause them to be re-processed, and tagged with elevation
        using the latest set of available LIDAR data. This should only be
        triggered when the LIDAR data changes.
        """
        logger.info("Dropping records where elevation is NULL")

        nodes_tbl = DeltaTable.forPath(
            self.spark, (self.data_dir / "staging" / dataset).as_posix()
        )

        nodes_tbl.delete(F.col("elevation").isNull())

        logger.debug("Records dropped successfully")

    def clear_temp_files(
        self, tbl_dir: Literal["node_updates", "edge_updates"]
    ) -> None:
        """One the data load operation has finished, clear out any files in
        the temp directory

        Args:
            tbl_dir: The temp directory to be cleared
        """
        logger.info("Clearing temp directory")
        with contextlib.suppress(FileNotFoundError):
            shutil.rmtree((self.data_dir / "temp" / tbl_dir).as_posix())
