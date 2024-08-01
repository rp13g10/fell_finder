"""Defines the GraphContractor class, which is responsible for minimising the
complexity of the graph when loaded into networkx for route creation"""

from glob import glob
from typing import Set, Tuple
import os
import re

from pyspark.sql import DataFrame, functions as F, SparkSession
from pyspark.sql.window import Window


class GraphContractor:
    """Class which is responsible for minimising the complexity of the graph
    when loaded into networkx for route creation"""

    def __init__(self, data_dir: str, spark: SparkSession) -> None:
        """Create a graph contractor object, which exposes an optimise method
        that removes nodes from the graph which do not form junctions

        Args:
            data_dir: A folder containing parsed lidar, node and edge datasets
            spark: The active spark session"""
        self.data_dir = data_dir
        self.spark = spark
        self.num_ptns = 0

    def get_available_partitions(self) -> Set[Tuple[int, int]]:
        """Use the filesystem to determine which partitions are available in
        the specified subfolder. This should be much faster than using a
        pyspark groupby operation.

        Args:
            subfolder: The subfolder containing the dataset to be analysed

        Raises:
            FileNotFoundError: If no partitions can be identified, an exception
              will be raised

        Returns:
            A set in which each tuple represents a single
            easting_ptn/northing_ptn pair which is present in the data
        """

        # Fetch a list of all parquet files in the dataset
        all_files = glob(
            os.path.join(self.data_dir, "enriched/nodes", "**", "*.parquet"),
            recursive=True,
        )

        def _get_easting_northing(file_path: str) -> Tuple[int, int]:
            """Helper function which uses regular expressions to fetch the
            easting and northing partition for the provided file path and
            return them as a tuple.

            Args:
                file_path: The file path to be analysed

            Raises:
                FileNotFoundError: If no partitions can be identified, an
                  exception will be raised

            Returns:
                A tuple containing the easting_ptn and northing_ptn for the
                provided file_path
            """
            match_ = re.search(
                r".*/easting_ptn=(\d+)/northing_ptn=(\d+)/.*", file_path
            )
            if match_ is None:
                raise FileNotFoundError(
                    f"Unable to identify a partition for {file_path}"
                )
            easting = int(match_.group(1))
            northing = int(match_.group(2))
            return easting, northing

        all_partitions = {_get_easting_northing(file_) for file_ in all_files}

        return all_partitions

    def load_df(self, dataset: str) -> DataFrame:
        """Load in the contents of a single dataset, filtering it to include
        only the partitions which are present in all datasets.

        Args:
            dataset: The dataset to be loaded

        Returns:
            The filtered contents of the specified dataset
        """
        data_dir = os.path.join(self.data_dir, "enriched", dataset)

        df = self.spark.read.parquet(data_dir)
        df = df.repartition(self.num_ptns, "easting_ptn", "northing_ptn")

        return df

    def _get_node_degrees(self, edges: DataFrame) -> DataFrame:
        """Fetch a dataframe containing the in_degree and out_degree for each
        node in the graph.

        Args:
            edges: A dataframe containing all of the edges in the graph

        Returns:
            A new dataframe containing id, in_degree, out_degree and degree
            columns"""

        out_links = edges.select(
            F.col("src").alias("id"),
            F.col("dst").alias("neighbour"),
            F.lit(None).alias("neighbour_in"),
            F.col("dst").alias("neighbour_out"),
        )
        in_links = edges.select(
            F.col("dst").alias("id"),
            F.col("src").alias("neighbour"),
            F.col("src").alias("neighbour_in"),
            F.lit(None).alias("neighbour_out"),
        )
        links = out_links.union(in_links)

        degrees = links.groupBy("id").agg(
            F.count(F.col("neighbour_in")).alias("in_degree"),
            F.count(F.col("neighbour_out")).alias("out_degree"),
            F.countDistinct(F.col("neighbour")).alias("degree"),
        )

        return degrees

    def add_degrees_to_nodes(
        self, nodes: DataFrame, edges: DataFrame
    ) -> DataFrame:
        """Tag each node in the graph with in and out degrees. As a side-effect
        this will also drop any orphaned nodes.

        Args:
            nodes: A dataframe containing all of the nodes in the graph
            edges: A dataframe containing all of the edges in the graph

        Returns:
            A dataframe containing all of the nodes in the graph, with
            additional degree, in_degree and out_degree columns"""
        degrees = self._get_node_degrees(edges)

        nodes = nodes.join(degrees, on="id", how="inner")

        return nodes

    def derive_node_flags(self, nodes: DataFrame) -> DataFrame:
        """Use the degree information for each node to determine whether it
        can be contracted (or removed)

        Args:
            nodes: A dataframe containing all of the nodes in the graph

        Returns:
            A copy of the input dataset with additional flags::
              * contract_flag
              * dead_end_flag
              * orphan_flag"""

        contract_mask = F.col("degree") == 2
        dead_end_mask = F.col("degree") == 1

        nodes = nodes.withColumn(
            "contract_flag", F.when(contract_mask, 1).otherwise(0)
        )

        nodes = nodes.withColumn(
            "dead_end_flag", F.when(dead_end_mask, 1).otherwise(0)
        )

        return nodes

    def derive_way_start_end_flags(self, edges: DataFrame) -> DataFrame:
        """Determine which nodes are found at the start or end of ways, as
        they appear in the OSM data.

        Args:
            edges: A dataframe containing all of the edges in the graph

        Returns:
            A copy of the input dataset with two additional fields::
              * way_start_flag
              * way_end_flag
        """
        way_window = Window.partitionBy("way_id")

        edges = edges.withColumn(
            "way_max_inx", F.max("way_inx").over(way_window)
        )

        edges = edges.withColumn(
            "way_min_inx", F.min("way_inx").over(way_window)
        )

        edges = edges.withColumn(
            "way_start_flag",
            F.when(F.col("way_inx") == F.col("way_min_inx"), 1).otherwise(0),
        )

        edges = edges.withColumn(
            "way_end_flag",
            F.when(F.col("way_inx") == F.col("way_max_inx"), 1).otherwise(0),
        )

        edges = edges.drop("way_min_inx", "way_max_inx")

        return edges

    def derive_chain_src_dst(
        self, nodes: DataFrame, edges: DataFrame
    ) -> DataFrame:
        """For each edge in the graph, if it forms the start or end of a chain
        populate the chain_src and chain_dst columns with its src and dst.
        These fields will be NULL for any edges which do not form the start or
        end of a chain.

        Args:
            nodes: A dataframe containing all of the nodes in the graph
            edges: A dataframe containing all of the edges in the graph

        Returns:
            A copy of the edges dataframe with additional chain_src and
            chain_dst columns
        """

        # Bring through flags for the source node
        src_flags = nodes.select(
            F.col("id").alias("src"),
            F.col("contract_flag").alias("src_contract_flag"),
            F.col("elevation").alias("src_elevation"),
            F.col("dead_end_flag").alias("src_dead_end_flag"),
        )
        edges = edges.join(src_flags, on="src", how="inner")

        # Bring through flags for the destination node
        dst_flags = nodes.select(
            F.col("id").alias("dst"),
            F.col("contract_flag").alias("dst_contract_flag"),
            F.col("elevation").alias("dst_elevation"),
            F.col("dead_end_flag").alias("dst_dead_end_flag"),
        )
        edges = edges.join(dst_flags, on="dst", how="inner")

        # Populate IDs for nodes at the start of a chain
        chain_src_mask = (F.col("way_start_flag") == 1) | (
            F.col("src_contract_flag") == 0
        )
        edges = edges.withColumn(
            "chain_src", F.when(chain_src_mask, F.col("src"))
        )

        # Populate IDs for nodes at the end of a chain
        chain_end_mask = (F.col("way_end_flag") == 1) | (
            F.col("dst_contract_flag") == 0
        )
        edges = edges.withColumn(
            "chain_dst", F.when(chain_end_mask, F.col("dst"))
        )

        # Drop unused columns
        edges = edges.drop(
            "src_contract_flag",
            "dst_contract_flag",
            "way_start_flag",
            "way_end_flag",
        )

        return edges

    def propagate_chain_src_dst(self, edges: DataFrame) -> DataFrame:
        """Ensure that chain_src and chain_dst are populated for every edge in
        the graph by propagating the values forwards/backwards across each way

        Args:
            edges: A dataframe containing all of the edges in the graph

        Returns:
            A copy of the input dataframe with fully populated chain_src and
            chain_dst columns"""
        way_window = Window.partitionBy("way_id")
        way_window_asc = way_window.orderBy(F.col("way_inx").asc())
        way_window_desc = way_window.orderBy(F.col("way_inx").desc())

        edges = edges.withColumn(
            "chain_src_propa",
            F.last_value(F.col("chain_src"), ignoreNulls=True).over(
                way_window_asc
            ),
        )

        edges = edges.withColumn(
            "chain_dst_propa",
            F.last_value(F.col("chain_dst"), ignoreNulls=True).over(
                way_window_desc
            ),
        )

        edges = edges.drop("chain_src", "chain_dst").withColumnsRenamed(
            {"chain_src_propa": "chain_src", "chain_dst_propa": "chain_dst"}
        )

        return edges

    def contract_chains(self, edges: DataFrame) -> DataFrame:
        """Aggregate all of the edges in the graph according to their new
        chain_src and chain_dst values, eliminating edges which are in the
        middle of each chain. Retain key information about the geometry of
        each edge such as distances between each point, and the lat/lon
        coordinates visited travelling across each chain.

        Args:
            edges: A dataframe containing all of the edges in the graph

        Returns:
            An aggregated copy of the input dataframe
        """

        edges = edges.groupBy("way_id", "chain_src", "chain_dst").agg(
            F.collect_set("highway").alias("highway"),
            F.collect_set("surface").alias("surface"),
            F.sum("elevation_gain").alias("elevation_gain"),
            F.sum("elevation_loss").alias("elevation_loss"),
            F.sum("distance").alias("distance"),
            F.array_sort(
                F.collect_list(
                    F.struct(
                        "way_inx", "distance", "easting_ptn", "northing_ptn"
                    )
                )
            ).alias("geom"),
            F.array_sort(
                F.collect_list(
                    F.struct(
                        "way_inx",
                        "src_lat",
                        "src_lon",
                        "src_elevation",
                        "src_dead_end_flag",
                    )
                )
            ).alias("src_geom"),
            F.array_sort(
                F.collect_list(
                    F.struct(
                        "way_inx",
                        "dst_lat",
                        "dst_lon",
                        "dst_elevation",
                        "dst_dead_end_flag",
                    )
                )
            ).alias("dst_geom"),
        )

        return edges

    def generate_new_edges_from_chains(self, edges: DataFrame) -> DataFrame:
        """Unpack the information stored when aggregating the edges into a more
        meaningful format.

        Args:
            edges: A dataframe containg all of the chains in the graph

        Returns:
            A copy of the input dataframe with a normalized schema
        """

        edges = edges.select(
            F.col("chain_src").alias("src"),
            F.col("chain_dst").alias("dst"),
            F.when(F.size("highway") == 1, F.col("highway").getItem(0))
            .otherwise("unclassified")
            .alias("highway"),
            F.when(F.size("surface") == 1, F.col("surface").getItem(0))
            .otherwise("unclassified")
            .alias("surface"),
            F.col("elevation_gain"),
            F.col("elevation_loss"),
            F.col("distance"),
            # Collect lats at each point in the edge
            F.array_insert(
                F.col("src_geom.src_lat"),
                -1,
                F.element_at("dst_geom.dst_lat", -1),
            ).alias("geom_lat"),
            # Collect lons at each point in the edge
            F.array_insert(
                F.col("src_geom.src_lon"),
                -1,
                F.element_at("dst_geom.dst_lon", -1),
            ).alias("geom_lon"),
            # Collect elevation at each point in the edge
            F.array_insert(
                F.col("src_geom.src_elevation"),
                -1,
                F.element_at("dst_geom.dst_elevation", -1),
            ).alias("geom_elevation"),
            # Collect distance at each point in the edge, starting at 0
            F.array_insert(F.col("geom.distance"), 1, 0.0).alias(
                "geom_distance"
            ),
            # Retrieve dead end flags for src/dest
            F.element_at("src_geom.src_dead_end_flag", 1).alias(
                "src_dead_end_flag"
            ),
            F.element_at("dst_geom.dst_dead_end_flag", -1).alias(
                "dst_dead_end_flag"
            ),
            # Get partitions based on first point in the edge
            F.element_at("geom.easting_ptn", 1).alias("easting_ptn"),
            F.element_at("geom.northing_ptn", 1).alias("northing_ptn"),
        )

        return edges

    def drop_dead_ends(self, edges: DataFrame) -> DataFrame:
        """Remove any chains where either the start or end node is a dead end,
        as the routing algorithm will not be able to send users down them

        Args:
            edges: A dataframe containing all of the edges in the graph

        Returns:
            A filtered copy of the input dataset
        """
        src_mask = F.col("src_dead_end_flag") == 0
        dst_mask = F.col("dst_dead_end_flag") == 0

        edges = edges.filter(src_mask & dst_mask)

        return edges

    def set_edge_output_schema(self, edges: DataFrame) -> DataFrame:
        """Finalise the schema of the edges dataset ready for writing to disk

        Args:
            edges: A dataframe containing all of the chains in the graph

        Returns:
            A copy of the input dataframe with a standardized schema"""

        edges = edges.select(
            "src",
            "dst",
            "highway",
            "surface",
            "elevation_gain",
            "elevation_loss",
            "distance",
            F.struct(
                # NOTE: Array order is preserved within DataFrames
                F.col("geom_lat").alias("lat"),
                F.col("geom_lon").alias("lon"),
                F.col("geom_elevation").alias("elevation"),
                F.col("geom_distance").alias("distance"),
            ).alias("geometry"),
            "easting_ptn",
            "northing_ptn",
        )

        edges = edges.dropna(subset=["src", "dst"])

        return edges

    def drop_unused_nodes(
        self, nodes: DataFrame, edges: DataFrame
    ) -> DataFrame:
        """Drop any nodes from the graph which are no longer connected to an
        edge.

        Args:
            nodes: A dataframe containing all of the nodes in the graph
            edges: A dataframe containing all of the chains in the graph

        Returns:
            A filtered copy of the nodes dataframe
        """

        src_nodes = edges.select(F.col("src").alias("id"))
        dst_nodes = edges.select(F.col("dst").alias("id"))
        to_keep = src_nodes.union(dst_nodes).dropDuplicates()

        nodes = nodes.join(to_keep, on="id", how="inner")

        return nodes

    def set_node_output_schema(self, nodes: DataFrame) -> DataFrame:
        """Ensure the nodes dataset has a consistent schema

        Args:
            nodes: A dataframe containing all of the nodes in the graph

        Returns:
            A subset of the input dataframe
        """
        nodes = nodes.select(
            "id", "lat", "lon", "elevation", "easting_ptn", "northing_ptn"
        )

        nodes = nodes.dropna(subset=["id"])

        return nodes

    def store_df(self, df: DataFrame, target: str) -> None:
        """Store an enriched dataframe to disk, partitioning it by easting_ptn
        and northing_ptn.

        Args:
            df: The dataframe to be stored
            target: The target location for the enriched dataset
        """

        # Attempt to minimise the number of files written
        df = df.repartition(self.num_ptns, "easting_ptn", "northing_ptn")

        # Write the dataframe out to disk
        df.write.partitionBy("easting_ptn", "northing_ptn").mode(
            "overwrite"
        ).parquet(os.path.join(self.data_dir, "optimised", target))

    def contract(self) -> None:
        """Run the graph contraction algorithm on the enriched nodes & edges
        datasets. This will combine all edges in the graph which are neither
        junctions nor the ends of ways, storing the geometry of the combined
        edges as a new attribute. Only nodes which form the start or end
        of a combined edge will be retained.

        This minimises the speed and complexity of the graph, improving the
        performance of the route finding algorithm.
        """

        self.num_ptns = len(self.get_available_partitions())
        start_shuffle_ptns = self.spark.conf.get(
            "spark.sql.shuffle.partitions", "200"
        )

        self.spark.conf.set("spark.sql.shuffle.partitions", str(self.num_ptns))

        nodes = self.load_df("nodes")
        edges = self.load_df("edges")

        nodes = self.add_degrees_to_nodes(nodes, edges)
        nodes = self.derive_node_flags(nodes)

        edges = self.derive_way_start_end_flags(edges)
        edges = self.derive_chain_src_dst(nodes, edges)
        edges = self.propagate_chain_src_dst(edges)

        edges = self.contract_chains(edges)

        edges = self.generate_new_edges_from_chains(edges)
        edges = self.drop_dead_ends(edges)
        edges = self.set_edge_output_schema(edges)

        nodes = self.drop_unused_nodes(nodes, edges)

        self.store_df(nodes, "nodes")
        self.store_df(edges, "edges")

        # Restore the original settings of the spark session
        self.spark.conf.set(
            "spark.sql.shuffle.partitions",
            start_shuffle_ptns,  # type: ignore
        )
