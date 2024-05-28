"""Functions relating to the extraction & initial transformation of the LIDAR
extracts into a tabular format."""

import os
import re
from functools import lru_cache
from glob import glob
from typing import List

import numpy as np
import rasterio as rio
from pyspark.sql import DataFrame, SQLContext, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from tqdm import tqdm

from fell_finder.ingestion.lidar.discovery import get_available_folders
from fell_finder.utils.partitioning import add_partitions_to_df


class LidarLoader:
    def __init__(self, sql: SQLContext, data_dir: str):
        self.sql = sql
        self.data_dir = data_dir

        self.to_load = get_available_folders(data_dir)

    @staticmethod
    def load_lidar_from_folder(lidar_dir: str) -> np.ndarray:
        """For a given data folder, read in the contents of the .tif file within as
        a numpy array.

        Args:
            lidar_dir (str): The location of the data folder to be loaded

        Returns:
            np.ndarray: The contents of the .tif file within the provided data
            folder. Each file represents an area of 5km^2, so the shape of this
            array will be 5000*5000
        """
        tif_loc = glob(os.path.join(lidar_dir, "*.tif"))[0]
        with rio.open(tif_loc) as tif:
            lidar = tif.read()

        lidar = lidar[0]
        return lidar

    @staticmethod
    def load_bbox_from_folder(lidar_dir: str) -> np.ndarray:
        """For a given data folder, read in the contents of the .shp file within as
        a numpy array of length 4.

        Args:
            lidar_dir (str): The location of the data folder to be loaded

        Returns:
            np.ndarray: The contents of the .shp file within the provided data
            folder. Will have 4 elements corresponding to the physical area
            represented by the corresponding .tif file in this folder.
        """
        tfw_loc = glob(os.path.join(lidar_dir, "*.tfw"))[0]

        with open(tfw_loc, "r", encoding="utf8") as fobj:
            tfw = fobj.readlines()

        easting_min = int(float(tfw[4].strip()))
        easting_max = easting_min + 5000

        northing_max = int(float(tfw[5].strip())) + 1
        northing_min = northing_max - 5000

        bbox = np.array(
            [easting_min, northing_min, easting_max, northing_max], dtype=int
        )

        return bbox

    @lru_cache(16)
    @staticmethod
    def generate_file_id(lidar_dir: str) -> str:
        """Extract the OS grid reference for a given LIDAR file based on its full
        location on the filesystem.

        Args:
            lidar_dir (str): The full path to a LIDAR file, expected format is
            /some/path/relevation/data/lidar_composite_dtm_YYYY-1-XXDDxx

        Returns:
            str: The OS grid reference for the file, expected format is
            XXDDxx (e.g. SU20ne)
        """

        id_match = re.search(r"[A-Z][A-Z]\d\d[a-z][a-z]$", lidar_dir)

        if id_match:
            file_id = id_match.group(0)
            return file_id

        raise ValueError(
            (
                "Unable to extract grid reference from provided lidar_dir: "
                + lidar_dir
            )
        )

    @staticmethod
    def generate_records_from_lidar_array(
        lidar: np.ndarray, bbox: np.ndarray
    ) -> List:
        """Parse a given array containing LIDAR data, and the corresponding
        bounding box. Returns a long-form dataframe containg eastings, northings
        and elevations.

        Args:
            lidar (np.ndarray): An array containing LIDAR data
            bbox (np.ndarray): The bounding box corresponding to the provided
            lidar array

        Returns:
            pd.DataFrame: A dataframe containing the 'easting', 'northing' and
            'elevation' columns
        """
        # Get array dimensions
        size_e, size_s = lidar.shape

        # Collapse elevations to 1 dimension, left to right then top to bottom
        elevations = lidar.flatten(order="C")

        # Repeat eastings by array (A, B, A, B)
        eastings = np.tile(range(bbox[0], bbox[2]), size_s).astype("int32")

        # Repeat northings by element (A, A, B, B)
        northings = np.repeat(
            range(bbox[3] - 1, bbox[1] - 1, -1), size_e
        ).astype("int32")

        records = [
            [easting, northing, elevation]
            for easting, northing, elevation in zip(
                eastings, northings, elevations
            )
        ]

        return records

    def generate_dataframe_from_records(self, records: List) -> DataFrame:
        schema = StructType(
            [
                StructField("easting", IntegerType()),
                StructField("northing", IntegerType()),
                StructField("elevation", DoubleType()),
            ]
        )

        df = self.sql.createDataFrame(data=records, schema=schema)

        return df

    # Continue conversion process from here, everything needs to run using
    # pyspark rather than pandas. Testing & benchmarking will be required.

    def add_file_ids(self, lidar_df: DataFrame, lidar_dir: str) -> DataFrame:
        """Generate a file ID for a given file name, and store it in the provided
        dataframe under the 'file_id' column name. The file ID will be the OS grid
        reference for the provided file name. For example,
        lidar_composite_dtm_2022-1-SU20ne would generate a file ID of SU20ne.

        Args:
            lidar_df (pd.DataFrame): A dataframe containing LIDAR data
            lidar_dir (str): The name of the file from which `lidar_df` was created

        Returns:
            pd.DataFrame: The input dataset, with an additional 'file_id' column
        """

        file_id = self.generate_file_id(lidar_dir)
        lidar_df = lidar_df.withColumn("file_id", F.lit(file_id))

        return lidar_df

    @staticmethod
    def set_output_schema(lidar_df: DataFrame) -> DataFrame:
        """Set expected column order

        Args:
            lidar_df (DataFrame): The parsed lidar dataframe

        Returns:
            DataFrame: A copy of the input dataframe with an updated schema
        """
        lidar_df = lidar_df.select(
            "easting",
            "northing",
            "elevation",
            "file_id",
            "easting_ptn",
            "northing_ptn",
        )

        return lidar_df

    def write_df_to_parquet(self, lidar_df: DataFrame):
        """Write the contents of a dataframe containing lidar data to the
        specified location, setting the file name to <lidar_id>.csv

        Args:
            lidar_df (pd.DataFrame): A dataframe containing lidar data
            lidar_id (str): The unique identifier for the lidar file which was
            used to create lidar_df
            data_dir (str): The location which the parsed dataframe should be
            exported to
        """
        tgt_loc = os.path.join(self.data_dir, "parsed/lidar")

        lidar_df.write.partitionBy("easting_ptn", "northing_ptn").parquet(
            tgt_loc
        )

    def parse_lidar_folder(self, lidar_dir: str) -> DataFrame:
        """This function will read in the contents of a single LIDAR folder,
        transform it into the correct format for ingestion into Cassandra/Scylla
        and return the output as a Pandas dataframe.

        Args:
            lidar_dir (str): The location of the LIDAR folder to be loaded

        Returns:
            pd.DataFrame: The contents of the provided LIDAR folder
        """

        lidar = self.load_lidar_from_folder(lidar_dir)
        bbox = self.load_bbox_from_folder(lidar_dir)

        records = self.generate_records_from_lidar_array(lidar, bbox)
        lidar_df = self.generate_dataframe_from_records(records)

        lidar_df = add_partitions_to_df(lidar_df)
        lidar_df = self.add_file_ids(lidar_df, lidar_dir)

        return lidar_df

    def load(self):
        for lidar_dir in tqdm(self.to_load, desc="Parsing LIDAR data"):
            self.parse_lidar_folder(lidar_dir)
