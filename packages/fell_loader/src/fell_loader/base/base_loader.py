"""Defines the BaseLoader class, an abstract base class which sets out
common elements of the interface for the loader classes across all layers
of the ETL pipeline
"""

import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Literal

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

type Layer = Literal["landing", "staging", "sanitised", "optimised"]
type Dataset = Literal["nodes", "ways", "edges"]


class BaseLoader(ABC):
    """This BaseLoader class sets out common elements of the interface for
    all child loader classes.
    """

    def __init__(self) -> None:
        self.data_dir = Path(os.environ["FF_DATA_DIR"])

    @abstractmethod
    def run(self) -> None:
        """Every loader must provide a `run` method, which triggers all
        of the data processing required to populate one or more datasets in
        a layer of the data directory
        """


class BaseSparkLoader(BaseLoader, ABC):
    """This BaseSparkLoader class extends the BaseLoader with additional
    attributes required when loading data using pyspark
    """

    def __init__(self, spark: SparkSession) -> None:
        super().__init__()

        self.spark = spark

    def write_parquet(
        self,
        df: DataFrame,
        layer: Layer,
        dataset: Dataset,
    ) -> None:
        """Write the contents of the provided dataframe out to the specified
        layer in the data directory.

        Args:
            df: The dataframe to be written
            layer: The data layer to write the dataframe to
            dataset: The dataset being written
        """
        tgt_loc = self.data_dir / layer / dataset

        if not tgt_loc.exists():
            logger.info("Creating non-existent directory %s", tgt_loc)
            tgt_loc.mkdir(parents=True)

        logger.info("Writing data to %s", tgt_loc)

        df.write.parquet(tgt_loc.as_posix(), mode="overwrite")

        logger.info("Data written successfully")

    def read_parquet(
        self,
        layer: Layer,
        dataset: Dataset | str,
    ) -> DataFrame:
        """Read the contents of the specified dataset from a layer in the
        data directory, where data is stored in the parquet format

        Args:
            layer: The data layer to read the dataset from
            dataset: The dataset to read

        Raises:
            FileNotFoundError: If the target dataset does not exist in the
                specified layer, an exception will be raised

        Returns:
            A dataframe representing the target dataset
        """
        tgt_loc = self.data_dir / layer / dataset

        if not tgt_loc.exists():
            msg = f"Attempted to read from non-existent directory {tgt_loc}"
            # TODO: Confirm if this results in duplicate record in log
            logger.error(msg)
            raise FileNotFoundError(msg)

        logger.debug("Reading parquet from %s", tgt_loc)
        return self.spark.read.parquet(tgt_loc.as_posix())

    def read_delta(self, layer: Layer, dataset: Dataset) -> DataFrame:
        """Read the contents of the specified dataset from a layer in the
        data directory, where data is stored in the delta format. This is only
        appropriate for simple read operations, anything which needs to
        apply updates to the underlying delta table will need a custom
        implemetation.

        Args:
            layer: The data layer to read the dataset from
            dataset: The dataset to read

        Raises:
            FileNotFoundError: If the target dataset does not exist in the
                specified layer, an exception will be raised

        Returns:
            A dataframe representing the target dataset
        """
        tgt_loc = self.data_dir / layer / dataset

        if not tgt_loc.exists():
            msg = f"Attempted to read from non-existent directory {tgt_loc}"
            # TODO: Confirm if this results in duplicate record in log
            logger.error(msg)
            raise FileNotFoundError(msg)

        logger.debug("Reading delta from %s", tgt_loc)
        return DeltaTable.forPath(self.spark, tgt_loc.as_posix()).toDF()
