"""Functions relating to the extraction & initial transformation of the LIDAR
extracts into a tabular format."""

import os
import sys
import zipfile
from glob import glob
from typing import Set
from xml.etree import ElementTree as ET

import numpy as np
import polars as pl
import rasterio as rio
from tqdm.contrib.concurrent import process_map

from fell_loader.utils.partitioning import add_bng_partition_to_polars_df

# TODO: Add support for resume of partial loads, will need a thread-safe
#       cache


class LidarLoader:
    """This contains all of the functions required to convert the data held in
    a LIDAR folder into tabular format. In most use cases, only the `load`
    method will need to be called."""

    def __init__(self) -> None:
        """Initialize the loader class. An active pyspark SQL context is
        required, as is the path to the folder which contains the LIDAR
        extracts to be parsed.

        Args:
            data_dir: The data directory for this project. It is expected
              that this will contain an 'extracts/lidar' subdirectory, into
              which all LIDAR data will have been extracted.

        """

        self.data_dir = os.environ["FF_DATA_DIR"]

        self.to_load = self.get_available_folders()

    def get_available_folders(self) -> Set[str]:
        """Get a list of all of the data folders which are available within the
        data directory of this package. Data must have been downloaded from the
        DEFRA website:
        https://environment.data.gov.uk/DefraDataDownload/?Mode=survey
        Data should be from the composite DTM layer at 1m resolution.

        Raises:
            FileNotFoundError: If no folders matching the above pattern are
            found, an error will be raised.

        Returns:
            A set containing the absolute path to each data folder

        """
        all_lidar_dirs = glob(
            os.path.join(self.data_dir, "extracts/lidar/*.zip")
        )
        if not all_lidar_dirs:
            raise FileNotFoundError("No files found in data directory!")
        return set(all_lidar_dirs)

    @staticmethod
    def _get_filenames_from_archive(
        archive: zipfile.ZipFile,
    ) -> tuple[str, str]:
        """Fetch the names of required .tif and .xml files from the provided
        archive. Improved error handling to be added in a later build."""
        tif_loc = next(
            x.filename for x in archive.filelist if ".tif" in x.filename
        )
        xml_loc = next(
            x.filename for x in archive.filelist if ".xml" in x.filename
        )
        return tif_loc, xml_loc

    @staticmethod
    def _get_bbox_from_xml(xml: str) -> np.ndarray:
        tree = ET.fromstring(xml)

        corners = tree.findall("./spatRepInfo/Georect/cornerPts/pos")

        min_easting = sys.maxsize
        max_easting = -sys.maxsize
        min_northing = sys.maxsize
        max_northing = -sys.maxsize
        for corner in corners:
            corner_text = corner.text
            if corner_text is None:
                raise AssertionError("oops!")
            easting, northing = corner_text.split(" ")
            easting = int(float(easting))
            northing = int(float(northing))

            if easting < min_easting:
                min_easting = easting
            if easting > max_easting:
                max_easting = easting
            if northing < min_northing:
                min_northing = northing
            if northing > max_northing:
                max_northing = northing

        assert all(
            [
                (abs(x) != sys.maxsize)
                for x in [min_easting, max_easting, min_northing, max_northing]
            ]
        ), "Error while extracting bbox"
        assert min_easting < max_easting, "Error while extracting bbox"
        assert min_northing < max_northing, "Error while extracting bbox"

        bbox = np.array(
            [min_easting, min_northing, max_easting, max_northing], dtype=int
        )

        return bbox

    def load_lidar_and_bbox_from_folder(
        self, lidar_dir: str
    ) -> tuple[np.ndarray, np.ndarray]:
        """For a given data folder, read in the contents of the .tif file
        within as a numpy array.

        Args:
            lidar_dir: The location of the data folder to be loaded

        Returns:
            The contents of the .tif file within the provided data folder, and
            the bounding box for the area it covers. Each lidar file represents
            an area of 5km^2, so the shape of the first array will be
            5000*5000. The bounding box will be an array of 4 integers
            corresponding to:
                - easting_min
                - northing_min
                - easting_max
                - northing_max

        """

        with zipfile.ZipFile(lidar_dir, mode="r") as archive:
            tif_loc, xml_loc = self._get_filenames_from_archive(archive)

            with rio.open(archive.open(tif_loc)) as tif:
                lidar = tif.read()

            xml = archive.read(xml_loc).decode("utf8")
            bbox = self._get_bbox_from_xml(xml)

        lidar = lidar[0]
        return lidar, bbox

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
        eastings = np.tile(range(bbox[0], bbox[2]), size_e).astype("int32")

        # Repeat northings by element (A, A, B, B), stepping backwards as row
        # 0 is the northernmost section of the grid
        northings = np.repeat(
            range(bbox[3] - 1, bbox[1] - 1, -1), size_s
        ).astype("int32")

        df = pl.DataFrame(
            {
                "easting": eastings,
                "northing": northings,
                "elevation": elevations,
            },
            orient="row",
        )

        return df

    @staticmethod
    def set_output_schema(lidar_df: pl.DataFrame) -> pl.DataFrame:
        """Set expected column order

        Args:
            lidar_df: The parsed lidar dataframe

        Returns:
            A copy of the input dataframe with an updated schema

        """
        lidar_df = lidar_df.select("easting", "northing", "elevation", "ptn")

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

        lidar, bbox = self.load_lidar_and_bbox_from_folder(lidar_dir)

        lidar_df = self.generate_df_from_lidar_array(lidar, bbox)

        lidar_df = add_bng_partition_to_polars_df(lidar_df)
        lidar_df = self.set_output_schema(lidar_df)
        return lidar_df

    def write_df_to_parquet(self, lidar_df: pl.DataFrame) -> None:
        """Write the contents of a dataframe containing lidar data to the
        specified location. Data will be written in the parquet format, with
        partitioning set on the `ptn` column

        Args:
            lidar_df: A dataframe containing lidar data

        """
        tgt_loc = os.path.join(self.data_dir, "parsed/lidar")

        lidar_df.write_parquet(
            tgt_loc,
            use_pyarrow=True,
            pyarrow_options={
                "partition_cols": ["ptn"],
                "compression": "snappy",
            },
        )

    def process_lidar_file(self, lidar_dir: str) -> None:
        """Process a single zip file containing LIDAR data. If the file has
        already been processed, no action will be taken. If an error is
        encountered during process, the name of the offending file will be
        written to bad_files.txt (in the current working directory).

        Args:
            lidar_dir: The location of the lidar file to be parsed

        """

        try:
            lidar_df = self.parse_lidar_folder(lidar_dir)
            self.write_df_to_parquet(lidar_df)
            del lidar_df
        except pl.exceptions.ShapeError:
            # NOTE: Risk of thread collision deemed too low to worry about
            with open("bad_files.txt", "a") as fobj:
                fobj.write(f"{lidar_dir}\n")

    def load(self) -> None:
        """Primary user facing function for this class. Parses every available
        LIDAR extract and stores the output as a partitioned parquet dataset.

        Data will be written to `data/parsed/lidar` within the configured
        data_dir
        """
        process_map(
            self.process_lidar_file,
            self.to_load,
            desc="Parsing LIDAR data",
            chunksize=1,
        )
