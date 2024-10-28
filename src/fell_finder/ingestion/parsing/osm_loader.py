"""Functions relating to the loading of a .osm.pbf network graph into a tabular
format"""

import os
from typing import Tuple

import pandas as pd
import polars as pl
from bng_latlon import WGS84toOSGB36
from pyrosm import OSM

from fell_finder.utils.partitioning import add_partitions_to_polars_df

# TODO: Set this up to work with other filenames once moving to a fully
#       automated pipeline


class OsmLoader:
    """Reads in the contents of the provided OSM extract. Generates two parquet
    datasets containing details of the graph nodes and edges. These datasets
    are partitioned according to the british national grid system to improve
    access speed in downstream applications."""

    def __init__(self, data_dir: str) -> None:
        """Create an instance of the OSM loader, store down the directory which
        contains the data to be loaded."""

        self.data_dir = data_dir

    @staticmethod
    def _subset_nodes(nodes: pd.DataFrame) -> pd.DataFrame:
        """Select only the required columns from the nodes dataframe

        Args:
            nodes: The nodes dataframe

        Returns:
            A subset of the input dataframe
        """
        nodes = nodes.loc[:, ["id", "lat", "lon"]]

        return nodes

    @staticmethod
    def _subset_edges(edges: pd.DataFrame) -> pd.DataFrame:
        """Select only the required columns from the edges dataframe

        Args:
            edges: The edges dataframe

        Returns:
            A subset of the input dataframe
        """
        edges = edges.loc[
            :,
            [
                "id",
                "u",
                "v",
                "highway",
                "surface",
                "bridge",
                "oneway",
            ],
        ]
        edges.loc[:, "row_no"] = edges.index  # type: ignore
        return edges

    def read_osm_data(self) -> Tuple[pl.DataFrame, pl.DataFrame]:
        """Read in the contents of the provided OSM extract using pyrosm,
        drop any unnecessary columns and convert the remaining records into
        polars for faster processing.

        Returns:
            A dataframe containing node data, and a dataframe containing edge
            data
        """
        file_path = os.path.join(
            self.data_dir, "extracts/osm/hampshire-latest.osm.pbf"
        )

        osm_handle = OSM(file_path)

        nodes, edges = osm_handle.get_network("walking", nodes=True)  # type: ignore

        nodes = self._subset_nodes(nodes)  # type: ignore
        edges = self._subset_edges(edges)  # type: ignore

        nodes = pl.DataFrame(nodes, orient="row")
        edges = pl.DataFrame(edges, orient="row")

        return nodes, edges

    @staticmethod
    def assign_bng_coords(nodes: pl.DataFrame) -> pl.DataFrame:
        """Assign each node an easting and northing, this will be used to
        split the nodes dataset into partitions according to their geographical
        location

        Args:
            nodes: The nodes dataframe

        Returns:
            A copy of the input dataframe with additional easting and northing
            columns
        """
        nodes = nodes.with_columns(
            pl.struct("lat", "lon")
            .map_elements(
                lambda x: WGS84toOSGB36(x["lat"], x["lon"]),
                return_dtype=pl.List(pl.Float64),
            )
            .alias("bng_coords")
        )

        nodes = nodes.with_columns(
            pl.col("bng_coords").list.get(0).cast(pl.Int32()).alias("easting"),
            pl.col("bng_coords")
            .list.get(1)
            .cast(pl.Int32())
            .alias("northing"),
        )

        nodes = nodes.drop("bng_coords")

        return nodes

    @staticmethod
    def set_node_output_schema(nodes: pl.DataFrame) -> pl.DataFrame:
        """Ensure the node output dataset contains only the required columns

        Args:
            nodes: A polars dataframe containing details of all nodes in the
              graph

        Returns:
            A subset of the provided dataframe
        """
        nodes = nodes.select(
            "id",
            "lat",
            "lon",
            "easting",
            "northing",
            "easting_ptn",
            "northing_ptn",
        )
        return nodes

    @staticmethod
    def tidy_edge_schema(edges: pl.DataFrame) -> pl.DataFrame:
        """Set more descriptive field aliases for the edge dataframe

        Args:
            edges: A polars dataframe containing details of all edges in the
              graph

        Returns:
            A subset of the provided dataframe
        """
        edges = edges.select(
            pl.col("u").alias("src"),
            pl.col("v").alias("dst"),
            pl.col("id").alias("way_id"),
            "highway",
            "surface",
            "bridge",
            "row_no",
            "oneway",
        )

        return edges

    @staticmethod
    def add_reverse_edges(edges: pl.DataFrame) -> pl.DataFrame:
        """For any edges A-B where the oneway field is either NULL or
        explicitlyset to no, create a second edge B-A. Assign this new edge
        to a new way, and record its position in the dataframe. The geometry
        of a way is set by the position of each edge in the dataframe, so it
        is important that this information be retained.

        Args:
            edges: A polars dataframe containing details of all edges in the
              graph

        Returns:
            A copy of the input dataframe, with any bi-directional edges
            explicitly represented in both directions
        """
        reverse = edges.filter(
            (pl.col("oneway").is_null()) | (pl.col("oneway") == "no")
        )

        reverse = reverse.with_columns(
            pl.col("src").alias("old_src"), pl.col("dst").alias("src")
        )

        reverse = reverse.with_columns(
            pl.col("old_src").alias("dst"),
            (pl.col("way_id") * -1).alias("way_id"),
            (pl.col("row_no") * -1).alias("row_no"),
        ).drop("old_src")

        edges = pl.concat([edges, reverse])

        return edges

    @staticmethod
    def derive_position_in_way(edges: pl.DataFrame) -> pl.DataFrame:
        """For each edge in a way, record its relative position as way_inx.
        The first edge will have a way_inx of 1, and the last edge will have a
        way_inx of N, where N is the number of edges in the way.

        Args:
            edges: A polars dataframe containing details of all edges in the
              graph

        Returns:
            A copy of the input dataframe with an additional way_inx column
        """
        edges = edges.with_columns(
            pl.col("row_no").rank("ordinal").over("way_id").alias("way_inx")
        )
        return edges

    @staticmethod
    def get_edge_start_coords(
        nodes: pl.DataFrame, edges: pl.DataFrame
    ) -> pl.DataFrame:
        """For each edge in the graph, work out its starting position by
        joining on details of the source node.

        Args:
            nodes: A polars dataframe containing details of all nodes in the
              graph
            edges: A polars dataframe containing details of all edges in the
              graph

        Returns:
            A copy of the edges dataframe with additional src_lat, src_lon,
            src_easting, src_northing, easting_ptn and northing_ptn columns
        """
        nodes = nodes.select(
            pl.col("id").alias("src"),
            pl.col("lat").alias("src_lat"),
            pl.col("lon").alias("src_lon"),
            pl.col("easting").alias("src_easting"),
            pl.col("northing").alias("src_northing"),
            "easting_ptn",
            "northing_ptn",
        )

        edges = edges.join(nodes, on="src", how="inner")

        return edges

    @staticmethod
    def get_edge_end_coords(
        nodes: pl.DataFrame, edges: pl.DataFrame
    ) -> pl.DataFrame:
        """For each edge in the graph, work out its final position by joining
        on details of the destination node.

        Args:
            nodes: A polars dataframe containing details of all nodes in the
              graph
            edges: A polars dataframe containing details of all edges in the
              graph

        Returns:
            A copy of the edges dataframe with additional dst_lat, dst_lon,
            dst_easting and dst_northing columns
        """
        nodes = nodes.select(
            pl.col("id").alias("dst"),
            pl.col("lat").alias("dst_lat"),
            pl.col("lon").alias("dst_lon"),
            pl.col("easting").alias("dst_easting"),
            pl.col("northing").alias("dst_northing"),
        )

        edges = edges.join(nodes, on="dst", how="inner")

        return edges

    @staticmethod
    def set_edge_output_schema(edges: pl.DataFrame) -> pl.DataFrame:
        """Ensure the node output dataset contains only the required columns

        Args:
            edges: A polars dataframe containing details of all edges in the
              graph

        Returns:
            A subset of the provided dataframe
        """

        edges = edges.select(
            "src",
            "dst",
            "way_id",
            "way_inx",
            "highway",
            "surface",
            "bridge",
            "src_lat",
            "src_lon",
            "dst_lat",
            "dst_lon",
            "src_easting",
            "src_northing",
            "dst_easting",
            "dst_northing",
            "easting_ptn",
            "northing_ptn",
        )
        return edges

    def write_nodes_to_parquet(self, nodes: pl.DataFrame) -> None:
        """Write the provided dataframe out to parquet format, partitioning
        by easting_ptn and northing_ptn

        Args:
            nodes: The dataframe to be written
        """
        tgt_loc = os.path.join(self.data_dir, "parsed/nodes")

        nodes.write_parquet(
            tgt_loc,
            use_pyarrow=True,
            pyarrow_options={
                "partition_cols": ["easting_ptn", "northing_ptn"],
                "max_partitions": 4096,
            },
        )

    def write_edges_to_parquet(self, edges: pl.DataFrame) -> None:
        """Write the provided dataframe out to parquet format, partitioning
        by easting_ptn and northing_ptn

        Args:
            edges: The dataframe to be written
        """
        tgt_loc = os.path.join(self.data_dir, "parsed/edges")

        edges.write_parquet(
            tgt_loc,
            use_pyarrow=True,
            pyarrow_options={
                "partition_cols": ["easting_ptn", "northing_ptn"],
                "max_partitions": 4096,
            },
        )

    def load(self) -> None:
        """End-to-end data load script for an OSM extract. This will read in
        the contents of the file using pyrosm, perform some basic processing to
        ensure the resultant graph will be bidirectional and way geometry
        will be preserved when processed by pyspark. Nodes and edges will be
        assigned to partitions according to eastings & northings before being
        written out to disk.
        """
        nodes, edges = self.read_osm_data()

        nodes = self.assign_bng_coords(nodes)
        nodes = add_partitions_to_polars_df(nodes)
        nodes = self.set_node_output_schema(nodes)

        edges = self.tidy_edge_schema(edges)
        edges = self.add_reverse_edges(edges)
        edges = self.derive_position_in_way(edges)

        edges = self.get_edge_start_coords(nodes, edges)
        edges = self.get_edge_end_coords(nodes, edges)
        edges = self.set_edge_output_schema(edges)

        self.write_nodes_to_parquet(nodes)
        self.write_edges_to_parquet(edges)
