"""Functions relating to the loading of a .osm.pbf network graph into a tabular
format"""

import os
from typing import Tuple

import pandas as pd
import polars as pl
from bng_latlon import WGS84toOSGB36
from pyrosm import OSM

from fell_finder.utils.partitioning import add_partitions_to_polars_df

# TODO: Confirm whether the length field corresponds to ways or edges


class OsmLoader:
    """Reads in the JSON graph, converted from OSM data. Generates two parquet
    datasets containing details of the graph nodes and edges. These datasets
    are partitioned according to the british national grid system to improve
    access speed in downstream applications."""

    def __init__(self, data_dir: str):
        """Create an instance of the OSM loader, store down the directory which
        contains the data to be loaded."""

        self.data_dir = data_dir

    def _subset_nodes(self, nodes: pd.DataFrame) -> pd.DataFrame:
        nodes = nodes.loc[:, ["id", "lat", "lon"]]

        return nodes

    def _subset_edges(self, edges: pd.DataFrame) -> pd.DataFrame:
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
        file_path = os.path.join(
            self.data_dir, "extracts/osm/hampshire-latest.osm.pbf"
        )

        osm_handle = OSM(file_path)

        nodes, edges = osm_handle.get_network("walking", nodes=True)  # type: ignore

        nodes = self._subset_nodes(nodes)  # type: ignore
        edges = self._subset_edges(edges)  # type: ignore

        nodes = pl.DataFrame(nodes)
        edges = pl.DataFrame(edges)

        return nodes, edges

    def assign_bng_coords(self, nodes: pl.DataFrame) -> pl.DataFrame:
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

    def set_node_output_schema(self, nodes: pl.DataFrame) -> pl.DataFrame:
        """Ensure the node output dataset contains only the required columns

        Args:
            nodes (pl.DataFrame): A polars dataframe containing details of
              all nodes in the graph

        Returns:
            pl.DataFrame: A subset of the provided dataframe
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

    def tidy_edge_schema(self, edges: pl.DataFrame) -> pl.DataFrame:
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

    def add_reverse_edges(self, edges: pl.DataFrame) -> pl.DataFrame:
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

    def derive_position_in_way(self, edges: pl.DataFrame):
        edges = edges.with_columns(
            pl.col("row_no").rank("ordinal").over("way_id").alias("way_inx")
        )
        return edges

    def get_edge_start_coords(
        self, nodes: pl.DataFrame, edges: pl.DataFrame
    ) -> pl.DataFrame:
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

    def get_edge_end_coords(
        self, nodes: pl.DataFrame, edges: pl.DataFrame
    ) -> pl.DataFrame:
        nodes = nodes.select(
            pl.col("id").alias("dst"),
            pl.col("lat").alias("dst_lat"),
            pl.col("lon").alias("dst_lon"),
            pl.col("easting").alias("dst_easting"),
            pl.col("northing").alias("dst_northing"),
        )

        edges = edges.join(nodes, on="dst", how="inner")

        return edges

    def set_edge_output_schema(self, edges: pl.DataFrame) -> pl.DataFrame:
        """Ensure the node output dataset contains only the required columns

        Args:
            nodes (pl.DataFrame): A polars dataframe containing details of
              all nodes in the graph

        Returns:
            pl.DataFrame: A subset of the provided dataframe
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

    def write_nodes_to_parquet(self, nodes: pl.DataFrame):
        """Write the provided dataframe out to parquet format, partitioning
        by easting_ptn and northing_ptn

        Args:
            nodes (pl.DataFrame): The dataframe to be written
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

    def write_edges_to_parquet(self, edges: pl.DataFrame):
        """Write the provided dataframe out to parquet format, partitioning
        by easting_ptn and northing_ptn

        Args:
            nodes (pl.DataFrame): The dataframe to be written
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

    def load(self):
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
