"""Functions relating to the loading of a .json network graph into a tabular
format"""

import json
import os
from typing import List, Tuple

import polars as pl
from bng_latlon import WGS84toOSGB36
from networkx import DiGraph
from networkx.readwrite import json_graph

from fell_finder.utils.partitioning import add_partitions_to_polars_df

# NOTE: This is currently set up to only bring in the contents of the
#       hampshire-latest OSM dataset. Expansion to other regions is planned as
#       part of a future build.


class OsmLoader:
    """Reads in the JSON graph, converted from OSM data. Generates two parquet
    datasets containing details of the graph nodes and edges. These datasets
    are partitioned according to the british national grid system to improve
    access speed in downstream applications."""

    def __init__(self, data_dir: str):
        """Create an instance of the OSM loader, store down the directory which
        contains the data to be loaded."""

        self.data_dir = data_dir

    @staticmethod
    def fetch_node_coords(graph: DiGraph, node_id: int) -> Tuple[int, int]:
        """Convenience function, retrieves the latitude and longitude for a
        single node in a graph."""
        node = graph.nodes[node_id]
        lat = node["lat"]
        lon = node["lon"]
        return lat, lon

    def load_graph(self) -> DiGraph:
        """Read in the contents of the JSON file specified by `source_path`
        to a networkx graph.

        Args:
            source_path (str): The location of the networkx graph to be
              enriched. The graph must have been saved to json format.

        Returns:
            Graph: A networkx graph with the contents of the provided json
              file.
        """

        graph_path = os.path.join(
            self.data_dir, "extracts/osm/hampshire-latest.json"
        )

        # Read in the contents of the JSON file
        with open(graph_path, "r", encoding="utf8") as fobj:
            graph_data = json.load(fobj)

        # Convert it back to a networkx graph
        graph = json_graph.adjacency_graph(graph_data)

        return graph

    @staticmethod
    def generate_node_data(graph: DiGraph) -> List[Tuple]:
        """Extracts the node_id, latitude and longitude for each node. Returns
        a list of tuples containing this information.

        Returns:
            List[Tuple]: A list of tuples containing for each node in the
            graph: id, latitude, longitude, easting, northing
        """

        all_coords = []
        for node_id, node_attrs in graph.nodes.items():
            node_lat = node_attrs["lat"]
            node_lon = node_attrs["lon"]
            node_easting, node_northing = WGS84toOSGB36(node_lat, node_lon)

            all_coords.append(
                (
                    node_id,
                    node_lat,
                    node_lon,
                    node_easting,
                    node_northing,
                )
            )

        return all_coords

    def generate_node_df_from_data(
        self, node_data: List[Tuple]
    ) -> pl.DataFrame:
        """Converts a list of tuples into a polars dataframe

        Args:
            node_data (List[Tuple]): A list of tuples containing one tuple for
              each node in the graph. Each tuple should contain the elements:
              id, latitude, longitude, easting, northing

        Returns:
            pl.DataFrame: A polars dataframe containing the provided data
        """
        node_schema = {
            "id": pl.Int64(),
            "lat": pl.Float64(),
            "lon": pl.Float64(),
            "easting": pl.Int32(),
            "northing": pl.Int32(),
        }

        nodes_df = pl.DataFrame(data=node_data, schema=node_schema)

        return nodes_df

    def set_node_output_schema(self, node_df: pl.DataFrame) -> pl.DataFrame:
        """Ensure the node output dataset contains only the required columns

        Args:
            node_df (pl.DataFrame): A polars dataframe containing details of
              all nodes in the graph

        Returns:
            pl.DataFrame: A subset of the provided dataframe
        """
        node_df = node_df.select(
            "id",
            "lat",
            "lon",
            "easting",
            "northing",
            "easting_ptn",
            "northing_ptn",
        )
        return node_df

    def write_node_df_to_parquet(self, node_df: pl.DataFrame):
        """Write the provided dataframe out to parquet format, partitioning
        by easting_ptn and northing_ptn

        Args:
            node_df (pl.DataFrame): The dataframe to be written
        """
        tgt_loc = os.path.join(self.data_dir, "parsed/nodes")

        node_df.write_parquet(
            tgt_loc,
            use_pyarrow=True,
            pyarrow_options={
                "partition_cols": ["easting_ptn", "northing_ptn"],
                "max_partitions": 4096,
            },
        )

    def generate_edge_data(self, graph: DiGraph) -> List[Tuple]:
        """Extracts the start and end points for each edge in the internal
        graph, returns both their IDs and lat/lon coordinates as a tuple
        for each edge in the graph.

        Returns:
            List[Tuple]: A list in which each tuple contains:
              start_id, end_id, start_lat, start_lon, end_lat, end_lon, ...
              \\... edge_type, src_easting, src_northing, dst_easting, ...
              \\... dst_northing
        """

        # NOTE: lat/lon datapoints can be dropped after UAT, not required
        #       for algorithm

        all_edges = []
        for start_id, end_id in graph.edges():
            edge_type = graph[start_id][end_id].get("highway")
            start_lat, start_lon = self.fetch_node_coords(graph, start_id)
            end_lat, end_lon = self.fetch_node_coords(graph, end_id)
            src_easting, src_northing = WGS84toOSGB36(start_lat, start_lon)
            dst_easting, dst_northing = WGS84toOSGB36(end_lat, end_lon)
            all_edges.append(
                (
                    start_id,
                    end_id,
                    start_lat,
                    start_lon,
                    end_lat,
                    end_lon,
                    edge_type,
                    src_easting,
                    src_northing,
                    dst_easting,
                    dst_northing,
                )
            )

        return all_edges

    def generate_edge_df_from_data(
        self, edge_data: List[Tuple]
    ) -> pl.DataFrame:
        """Converts a list of tuples into a polars dataframe

        Args:
            edge_data (List[Tuple]): A list of tuples containing one tuple for
              each node in the graph. Each tuple should contain the elements:
              start_id, end_id, start_lat, start_lon, end_lat, end_lon, ...
              \\... edge_type, easting, northing

        Returns:
            pl.DataFrame: A polars dataframe containing the provided data
        """
        edge_schema = {
            "src": pl.Int64(),
            "dst": pl.Int64(),
            "src_lat": pl.Float64(),
            "src_lon": pl.Float64(),
            "dst_lat": pl.Float64(),
            "dst_lon": pl.Float64(),
            "type": pl.String(),
            "easting": pl.Int32(),
            "northing": pl.Int32(),
            "dst_easting": pl.Int32(),
            "dst_northing": pl.Int32(),
        }

        edge_df = pl.DataFrame(data=edge_data, schema=edge_schema)

        return edge_df

    def set_edge_output_schema(self, edge_df: pl.DataFrame) -> pl.DataFrame:
        """Ensure the node output dataset contains only the required columns

        Args:
            node_df (pl.DataFrame): A polars dataframe containing details of
              all nodes in the graph

        Returns:
            pl.DataFrame: A subset of the provided dataframe
        """
        edge_df = edge_df.select(
            "src",
            "dst",
            "src_lat",
            "src_lon",
            "dst_lat",
            "dst_lon",
            "type",
            pl.col("easting").alias("src_easting"),
            pl.col("northing").alias("src_northing"),
            "dst_easting",
            "dst_northing",
            "easting_ptn",
            "northing_ptn",
        )
        return edge_df

    def write_edge_df_to_parquet(self, edge_df: pl.DataFrame):
        """Write the provided dataframe out to parquet format, partitioning
        by easting_ptn and northing_ptn

        Args:
            node_df (pl.DataFrame): The dataframe to be written
        """
        tgt_loc = os.path.join(self.data_dir, "parsed/edges")

        edge_df.write_parquet(
            tgt_loc,
            use_pyarrow=True,
            pyarrow_options={
                "partition_cols": ["easting_ptn", "northing_ptn"],
                "max_partitions": 4096,
            },
        )

    def load(self):
        """Primary user facing function for this class. Reads in the provided
        OSM extract and stores the nodes & edges as two separate partitioned
        parquet datasets"""
        graph = self.load_graph()

        node_data = self.generate_node_data(graph)
        node_df = self.generate_node_df_from_data(node_data)
        del node_data
        node_df = add_partitions_to_polars_df(node_df)
        node_df = self.set_node_output_schema(node_df)
        self.write_node_df_to_parquet(node_df)
        del node_df

        edge_data = self.generate_edge_data(graph)
        del graph
        edge_df = self.generate_edge_df_from_data(edge_data)
        del edge_data
        edge_df = add_partitions_to_polars_df(edge_df)
        edge_df = self.set_edge_output_schema(edge_df)
        self.write_edge_df_to_parquet(edge_df)
        del edge_df
