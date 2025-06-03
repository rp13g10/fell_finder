"""Handles the joining of graph data with the corresponding elevation data"""

import os
from glob import glob

from pyspark.sql import DataFrame, SparkSession

from fell_loader.enriching.edge_mixin import EdgeMixin
from fell_loader.enriching.node_mixin import NodeMixin
from fell_loader.utils.partitioning import add_bng_partition_to_spark_df


class GraphEnricher(NodeMixin, EdgeMixin):
    """Defines an enrich method, which combines the parsed lidar, node and edge
    datasets. Generates enriched node and edge datasets, which contain
    elevation data (nodes and edges), and distance data (edges only)

    Args:
        NodeMixin: Mixin class defining required logic for nodes
        EdgeMixin: Mixin class defining required logic for edges

    """

    def __init__(self, spark: SparkSession) -> None:
        """Create a GraphEnricher object, which is tied to the provided
        data directory and spark session.

        Args:
            data_dir: A folder containing parsed lidar, node and edge
              datasets
            spark: The active spark session

        """
        self.data_dir = os.environ["FF_DATA_DIR"]
        self.spark = spark

        self.edge_resolution_m = int(os.environ["FF_EDGE_RESOLUTION"])

    def get_common_ptns(self) -> set[str]:
        """Return a set containing all of the partitions which are present for
        the LIDAR extracts, and the prejoin nodes/edges datasets. This can then
        be used to process data one partition at a time, reducing the amount
        of data which is spilled to disk at any one time.

        Returns:
            A set containing the partitions which are present in all required
            datasets

        """
        all_ptns: list[set[str]] = []
        for layer, dataset in [
            ("parsed", "lidar"),
            ("enriched", "prejoin_nodes"),
            ("enriched", "prejoin_edges"),
        ]:
            data_dir = os.path.join(self.data_dir, layer, dataset, "*")

            ptns = {
                os.path.basename(path)
                for path in glob(data_dir)
                if "=" in path
            }

            all_ptns.append(ptns)

        common_ptns = all_ptns.pop()
        while all_ptns:
            common_ptns = common_ptns.intersection(all_ptns.pop())

        return common_ptns

    def load_df(
        self, dataset: str, layer: str = "parsed", ptn: str | None = None
    ) -> DataFrame:
        """Load in the contents of a single dataset, filtering it to include
        only the partitions which are present in all datasets.

        Args:
            dataset: The name of the dataset to be loaded
            layer: The data layer to pull from. Defaults to 'parsed'.
            ptn: The partition to be loaded. If set to None, all data in the
                provided dataset will be loaded. Defaults to None.

        Returns:
            The filtered contents of the specified dataset

        """

        basepath = os.path.join(self.data_dir, layer, dataset)

        if ptn:
            data_dir = os.path.join(self.data_dir, layer, dataset, ptn)
            df = self.spark.read.option("basePath", basepath).parquet(data_dir)
        else:
            df = self.spark.read.parquet(basepath)

        return df

    def store_df(self, df: DataFrame, target: str, ptns: bool) -> None:
        """Store an enriched dataframe to disk.

        Args:
            df: The dataframe to be stored
            target: The target location for the enriched dataset
            ptns: Determines whether or not the provided dataframe should be
                partitioned on write. If set to True, the provided dataframe
                must contain a 'ptn' column

        """

        if ptns:
            # Write the dataframe out to disk
            df.write.mode("append").parquet(
                os.path.join(self.data_dir, "enriched", target),
                partitionBy="ptn",
                compression="snappy",
            )
        else:
            df.write.mode("append").parquet(
                os.path.join(self.data_dir, "enriched", target),
                compression="snappy",
            )

    def enrich(self) -> None:
        """Perform all of the steps required to enrich both the nodes and edges
        datasets with elevation & distance data. Store the enriched datasets
        to the 'enriched' subfolder within the specified data_dir.
        """

        # Repartition nodes (move earlier if it works)
        nodes_df = self.load_df("nodes")
        nodes_df = add_bng_partition_to_spark_df(nodes_df)
        self.store_df(nodes_df, "prejoin_nodes", ptns=True)
        del nodes_df

        # Explode edges before the big join
        edges_df = self.load_df("edges")
        edges_df = self.calculate_step_metrics(edges_df)
        edges_df = self.explode_edges(edges_df)
        edges_df = self.unpack_exploded_edges(edges_df)
        edges_df = add_bng_partition_to_spark_df(edges_df)
        self.store_df(edges_df, target="prejoin_edges", ptns=True)
        del edges_df

        # Join elevation onto OSM data, one partition at a time to keep
        # disk spillage to a minimum
        common_ptns = self.get_common_ptns()
        for ptn in common_ptns:
            # Read in elevation data
            lidar_ptn_df = self.load_df("lidar", layer="parsed", ptn=ptn)

            # Join nodes & elevation data
            nodes_ptn_df = self.load_df(
                "prejoin_nodes", layer="enriched", ptn=ptn
            )
            nodes_ptn_df = self.tag_nodes(nodes_ptn_df, lidar_ptn_df)
            nodes_ptn_df = self.set_node_output_schema(nodes_ptn_df)
            self.store_df(nodes_ptn_df, target="nodes", ptns=False)
            del nodes_ptn_df

            # Join exploded edges & elevation data
            edges_ptn_df = self.load_df(
                "prejoin_edges", layer="enriched", ptn=ptn
            )
            edges_ptn_df = self.tag_exploded_edges(edges_ptn_df, lidar_ptn_df)

            self.store_df(edges_ptn_df, "postjoin_edges", ptns=True)
            del edges_ptn_df
            del lidar_ptn_df

        # Combine edges again, this needs to look across partitions but data
        # volumes are a bit more sensible now that the LIDAR data has been
        # joined. Future builds might look to move this intermediate table
        # onto a database in order to benefit from table indexes.
        edges_df = self.load_df("postjoin_edges", layer="enriched")
        edges_df = edges_df.repartition("src", "dst")
        edges_df = self.calculate_elevation_changes(edges_df)
        edges_df = self.implode_edges(edges_df)
        edges_df = self.calculate_edge_distances(edges_df)
        edges_df = self.set_edge_output_schema(edges_df)

        self.store_df(edges_df, target="edges", ptns=False)
        del edges_df
