"""Sanitise the edge data by removing any edges which are not safe to
traverse, and tagging it up with some derived metrics such as distance and
elevation gain/loss.
"""

import os

from delta import DeltaTable
from geopy.distance import distance
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


class EdgeSanitiser:
    """Defines sanitising logic for the edges dataset"""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.data_dir = os.environ["FF_DATA_DIR"]

    def get_edges(self) -> DataFrame:
        """Read in the contents of the edges dataset from the staging layer

        Returns:
            The contents of the edges dataset
        """
        edges = DeltaTable.forPath(
            self.spark, os.path.join(self.data_dir, "staging", "edges")
        ).toDF()
        return edges

    @staticmethod
    def _get_tag_as_column(df: DataFrame, tag_name: str) -> DataFrame:
        """Fetch the value of a specific tag from the mapping held in 'tags'
        and add it to a new column of the same name. If the requested tag is
        not present for a record, NULL will be returned instead.
        All tag values will be stored as utf8 encoded strings.

        Args:
            df: A dataframe containing a tags column
            tag_name: The name of the tag to be fetched

        Returns:
            A copy of the input dataframe with an additional column matching
            the provided tag_name

        """
        df = df.withColumn(
            tag_name,
            F.regexp_replace(F.col("tags").getItem(tag_name), '"', ""),
        )

        return df

    def flag_footways(self, ways: DataFrame) -> DataFrame:
        """Attempt to use tags to determine which ways have been explicitly
        marked as accessible by foot. This may indicate a public crossing over
        private land, or the presence of a footpath next to a busy road.

        Args:
            ways: A dataframe containing the ways from the OSM extract

        Returns:
            A copy of the input dataset with an additional boolean
            `explicit_footway` column (road and footway represented by a single
            edge), and an additional `separate_footway` column (road and
            footway each have a unique edge)

        """
        footway_tags = [
            "foot",
            "sidewalk",
            "sidewalk:left",
            "sidewalk:right",
            "sidewalk:both",
        ]

        ways = ways.withColumn("explicit_footway", F.lit(False))
        ways = ways.withColumn("separate_footway", F.lit(False))
        for tag in footway_tags:
            ways = self._get_tag_as_column(ways, tag)
            # NOTE: sidewalk = separate denotes a sidewalk which is mapped as
            #       a separate way. We don't want to send people down the road,
            #       as the sidewalk will also be pulled through as part of the
            #       graph.

            explicit_mask = F.col(tag).isNotNull() & ~(
                F.col(tag).isin("no", "separate")
            )
            separate_mask = F.col(tag).isNotNull() & (F.col(tag) == "separate")

            ways = ways.withColumn(
                "explicit_footway", F.col("explicit_footway") | explicit_mask
            ).withColumn(
                "separate_footway", F.col("separate_footway") | separate_mask
            )
            ways = ways.drop(tag)

        return ways

    def remove_restricted_routes(self, ways: DataFrame) -> DataFrame:
        """Remove any ways which correspond to routes with access controls,
        which are not usable by the public

        Args:
            ways: A dataframe containing the ways from the OSM extract

        Returns:
            A filtered copy of the input dataset

        """
        ways = self._get_tag_as_column(ways, "access")

        accessible_mask = F.col("access").isin(
            "yes", "permissive", "designated"
        )

        noflag_mask = F.col("access").isNull()

        explicit_mask = F.col("explicit_footway")

        ways = ways.filter(accessible_mask | noflag_mask | explicit_mask).drop(
            "access"
        )

        return ways

    def remove_unsafe_routes(self, ways: DataFrame) -> DataFrame:
        """Some parts of the map will not be suitable for use by the routing
        algorithm as they are not safe to run on. This function will exclude
        all motorways, and any high-speed roads (<=50 mph) which do not have
        a footpath.

        Args:
            ways: A dataframe containing the ways from the OSM extract

        Returns:
            A filtered copy of the input dataset

        """
        # Drop motorways & motorway links
        not_motorway_mask = ~F.col("highway").contains(F.lit("motorway"))
        ways = ways.filter(not_motorway_mask)

        # Drop any roads where the footway forms a separate edge
        separate_mask = F.col("separate_footway")
        ways = ways.filter(~separate_mask)

        # Get rid of roundabouts
        ways = self._get_tag_as_column(ways, "junction")
        not_roundabout_mask = F.col("junction") != "roundabout"
        not_junction_mask = F.col("junction").isNull()
        ways = ways.filter(not_roundabout_mask | not_junction_mask).drop(
            "junction"
        )

        # Extract max speed on other roads as an integer
        ways = self._get_tag_as_column(ways, "maxspeed")

        ways = ways.withColumn(
            "maxspeed",
            F.regexp_extract(
                F.col("maxspeed"), r"[A-Za-z\s]*(\d{1,3})[A-Za-z\s]*", 1
            ),
        )

        # Keep records with max speed below 50, no known max speed, or have
        # been flagged as having a footpath
        low_speed_mask = F.col("maxspeed") < 50
        no_speed_mask = F.col("maxspeed").isNull()
        footway_mask = F.col("explicit_footway")
        ways = ways.filter(low_speed_mask | no_speed_mask | footway_mask).drop(
            "maxspeed"
        )

        return ways

    def set_flat_flag(self, ways: DataFrame) -> DataFrame:
        """For any edges which correspond to a feature which means data from
        a terrain model for elevation will not apply (i.e. a bridge or a
        tunnel), set a flag to ensure the elevation data for these edges is
        masked.

        Args:
            ways: A dataframe containing the ways from the OSM extract

        Returns:
            A copy of the input dataset with an additional boolean
            'is_flat' column

        """
        ways = self._get_tag_as_column(ways, "bridge")
        ways = self._get_tag_as_column(ways, "tunnel")

        ways = ways.withColumn(
            "is_flat",
            (F.col("bridge").isNotNull()) | (F.col("tunnel").isNotNull()),
        ).drop("bridge", "tunnel")

        return ways

    def set_oneway_flag(self, ways: DataFrame) -> DataFrame:
        """For any routes which are not one-way, we will need to create a
        reversed view to enable graph traversal in both directions. This
        information is recorded in the tags, so we must retrieve it for
        ease of use.

        Args:
            ways: A dataframe containing the ways from the OSM extract

        Returns:
            A copy of the input dataset with an additional boolean 'oneway'
            column

        """
        ways = self._get_tag_as_column(ways, "oneway")

        bidirectional_mask = F.col("oneway") == "no"
        noflag_mask = F.col("oneway").isNull()

        ways = ways.withColumn(
            "oneway",
            ~(bidirectional_mask | noflag_mask),
        )

        return ways

    @staticmethod
    def calculate_elevation_changes(edges: DataFrame) -> DataFrame:
        """For each step along each edge in the enriched edges dataset,
        calculate the change in elevation from the previous step. Store
        elevation loss and gain separately.

        Args:
            edges: A table representing exploded edges in the OSM graph

        Returns:
            A copy of the input dataset with additional elevation_gain and
            elevation_loss fields

        """
        # TODO: This needs updating to work on an array, source dataset is
        #       no longer exploded

        rolling_window = Window.partitionBy("src", "dst").orderBy("inx")

        edges = edges.withColumn(
            "last_elevation", F.lag("elevation", offset=1).over(rolling_window)
        )
        edges = edges.withColumn(
            "delta", F.col("elevation") - F.col("last_elevation")
        )

        edges = edges.withColumn(
            "elevation_gain",
            F.when(F.col("is_flat"), F.lit(0.0)).otherwise(
                F.abs(F.greatest(F.col("delta"), F.lit(0.0)))
            ),
        )

        edges = edges.withColumn(
            "elevation_loss",
            F.when(F.col("is_flat"), F.lit(0.0)).otherwise(
                F.abs(F.least(F.col("delta"), F.lit(0.0)))
            ),
        )

        edges = edges.withColumn(
            "elevation", F.when(~F.col("is_flat"), F.col("elevation"))
        )

        return edges

    @staticmethod
    def calculate_edge_distances(edges: DataFrame) -> DataFrame:
        """For each edge in the graph, calculate the distance from the start
        point to the end point in metres. A UDF must be applied to achieve
        this, as pySpark does not provide this function natively.

        Args:
            edges: A table representing edges in the OSM graph

        Returns:
            A copy of the input dataset with an additional distance column

        """

        @F.udf(returnType=T.DoubleType())
        def distance_udf(
            src_lat: float, src_lon: float, dst_lat: float, dst_lon: float
        ) -> float:
            """Enables the use of the geopy distance function to calculate the
            length of each edge represented by the pyspark edges dataset.

            Args:
                src_lat: The source latitude
                src_lon: The source longitude
                dst_lat: The destination latitude
                dst_lon: The destination longitude

            Returns:
                float: The distance between source and destination in metres

            """
            dist = distance((src_lat, src_lon), (dst_lat, dst_lon))
            dist_m = dist.meters
            return dist_m

        edges = edges.withColumn(
            "distance",
            distance_udf(
                F.col("src_lat"),
                F.col("src_lon"),
                F.col("dst_lat"),
                F.col("dst_lon"),
            ),
        )

        return edges

    @staticmethod
    def add_reverse_edges(edges: DataFrame) -> DataFrame:
        """For any edges A-B where the oneway field is either NULL or
        explicitly set to no, create a second edge B-A. Assign this new edge
        to a new way, and record its position in the dataframe. The geometry
        of a way is set by the position of each edge in the dataframe, so it
        is important that this information be retained.

        Args:
            edges: A dataframe containing details of all edges in the graph

        Returns:
            A copy of the input dataframe, with any bi-directional edges
            explicitly represented in both directions

        """
        # Get bi-directional edges
        reverse = edges.filter(~F.col("oneway"))

        # Swap src and dst
        reverse = reverse.withColumn("old_src", F.col("src"))
        reverse = reverse.withColumn("src", F.col("dst"))
        reverse = reverse.withColumn("dst", F.col("old_src"))
        reverse = reverse.drop("old_src")

        # Create new way ID and invert position of edge in way
        reverse = reverse.withColumn("way_id", F.col("way_id") * -1)
        reverse = reverse.withColumn("way_inx", F.col("way_inx") * -1)

        # Invert gain & loss
        reverse = reverse.withColumn(
            "elevation_gain", F.col("elevation_gain") * -1
        )
        reverse = reverse.withColumn(
            "elevation_loss", F.col("elevation_loss") * -1
        )

        # Add inverted edges onto the original dataset
        out = edges.unionByName(reverse)

        return out
