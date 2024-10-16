"""Functions relating to the extraction & initial transformation of the LIDAR
extracts into a tabular format."""

import os
import re
from glob import glob
from typing import Set

import numpy as np
import polars as pl

import rasterio as rio

from tqdm import tqdm

from fell_finder.utils.partitioning import add_partitions_to_polars_df


def get_available_folders(data_dir: str) -> Set[str]:
    """Get a list of all of the data folders which are available within the
    data directory of this package. Data must have been downloaded from the
    DEFRA website:
    https://environment.data.gov.uk/DefraDataDownload/?Mode=survey
    Data should be from the composite DTM layer at 1m resolution, and folder
    names should match the 'lidar_composite_dtm-*' pattern. Any zip archives
    should be extracted before running this script.

    Raises:
        FileNotFoundError: If no folders matching the above pattern are found,
          an error will be raised.

    Returns:
        A set containing the absolute path to each data folder
    """
    all_lidar_dirs = glob(
        os.path.join(data_dir, "extracts/lidar/lidar_composite_dtm-*")
    )
    if not all_lidar_dirs:
        raise FileNotFoundError("No files found in data directory!")
    return set(all_lidar_dirs)


class LidarLoader:
    """This contains all of the functions required to convert the data held in
    a LIDAR folder into tabular format. In most use cases, only the `load`
    method will need to be called."""

    def __init__(self, data_dir: str) -> None:
        """Initialize the loader class. An active pyspark SQL context is
        required, as is the path to the folder which contains the LIDAR
        extracts to be parsed.

        Args:
            data_dir: The data directory for this project. It is expected
              that this will contain an 'extracts/lidar' subdirectory, into
              which all LIDAR data will have been extracted.
        """
        # self.sql = sql
        self.data_dir = data_dir

        self.to_load = get_available_folders(data_dir)

    @staticmethod
    def load_lidar_from_folder(lidar_dir: str) -> np.ndarray:
        """For a given data folder, read in the contents of the .tif file
        within as a numpy array.

        Args:
            lidar_dir: The location of the data folder to be loaded

        Returns:
            The contents of the .tif file within the provided data folder.
            Each file represents an area of 5km^2, so the shape of this
            array will be 5000*5000
        """
        tif_loc = glob(os.path.join(lidar_dir, "*.tif"))[0]
        with rio.open(tif_loc) as tif:
            lidar = tif.read()

        lidar = lidar[0]
        return lidar

    @staticmethod
    def load_bbox_from_folder(lidar_dir: str) -> np.ndarray:
        """For a given data folder, read in the contents of the .shp file
        within as a numpy array of length 4.

        Args:
            lidar_dir: The location of the data folder to be loaded

        Returns:
            The contents of the .shp file within the provided data folder.
            Will have 4 elements corresponding to the physical area
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

    @staticmethod
    def generate_file_id(lidar_dir: str) -> str:
        """Extract the OS grid reference for a given LIDAR file based on its
        full location on the filesystem.

        Args:
            lidar_dir: The full path to a LIDAR file, expected format is
              /DATA_DIR/extracts/lidar/lidar_composite_dtm_YYYY-1-XXDDxx

        Returns:
            The OS grid reference for the file, expected format is
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
    def generate_df_from_lidar_array(
        lidar: np.ndarray, bbox: np.ndarray
    ) -> pl.DataFrame:
        """Parse a given array containing LIDAR data, and the corresponding
        bounding box. Returns a long-form dataframe containg eastings,
        northings and elevations.

        Args:
            lidar: An array containing LIDAR data
            bbox: The bounding box corresponding to the provided lidar array

        Returns:
            A dataframe containing the 'easting', 'northing' and 'elevation'
            columns
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

        df = pl.DataFrame(
            {
                "easting": eastings,
                "northing": northings,
                "elevation": elevations,
            }
        )

        return df

    def add_file_ids(
        self, lidar_df: pl.DataFrame, lidar_dir: str
    ) -> pl.DataFrame:
        """Generate a file ID for a given file name, and store it in the
        provided dataframe under the 'file_id' column name. The file ID will be
        the OS grid reference for the provided file name. For example,
        lidar_composite_dtm_2022-1-SU20ne would generate a file ID of SU20ne.

        Args:
            lidar_df: A dataframe containing LIDAR data
            lidar_dir: The name of the file from which `lidar_df` was created

        Returns:
            The input dataset, with an additional 'file_id' column
        """

        file_id = self.generate_file_id(lidar_dir)

        lidar_df = lidar_df.with_columns(pl.lit(file_id).alias("file_id"))

        return lidar_df

    @staticmethod
    def set_output_schema(lidar_df: pl.DataFrame) -> pl.DataFrame:
        """Set expected column order

        Args:
            lidar_df: The parsed lidar dataframe

        Returns:
            A copy of the input dataframe with an updated schema
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

    def parse_lidar_folder(self, lidar_dir: str) -> pl.DataFrame:
        """This function will read in the contents of a single LIDAR folder,
        transform it into a tabular format and return its contents as a pySpark
        Dataframe

        Args:
            lidar_dir: The location of the LIDAR folder to be loaded

        Returns:
            The contents of the provided LIDAR folder
        """

        lidar = self.load_lidar_from_folder(lidar_dir)
        bbox = self.load_bbox_from_folder(lidar_dir)

        lidar_df = self.generate_df_from_lidar_array(lidar, bbox)

        lidar_df = add_partitions_to_polars_df(lidar_df)
        lidar_df = self.add_file_ids(lidar_df, lidar_dir)
        lidar_df = self.set_output_schema(lidar_df)
        return lidar_df

    def write_df_to_parquet(self, lidar_df: pl.DataFrame) -> None:
        """Write the contents of a dataframe containing lidar data to the
        specified location. Data will be written in the parquet format, with
        partitioning set on the `easting_ptn` and `northing_ptn` columns

        Args:
            lidar_df: A dataframe containing lidar data
        """
        tgt_loc = os.path.join(self.data_dir, "parsed/lidar")

        lidar_df.write_parquet(
            tgt_loc,
            use_pyarrow=True,
            pyarrow_options={
                "partition_cols": ["easting_ptn", "northing_ptn"],
            },
        )

    def load(self) -> None:
        """Primary user facing function for this class. Parses every available
        LIDAR extract and stores the output as a partitioned parquet dataset.

        Data will be written to `data/parsed/lidar` within the configured
        data_dir
        """
        for lidar_dir in tqdm(self.to_load, desc="Parsing LIDAR data"):
            lidar_df = self.parse_lidar_folder(lidar_dir)
            self.write_df_to_parquet(lidar_df)
