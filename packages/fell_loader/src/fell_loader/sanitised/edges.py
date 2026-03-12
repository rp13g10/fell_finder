"""Sanitise the edge data by removing any edges which are not safe to
traverse, and tagging it up with some derived metrics such as distance and
elevation gain/loss.
"""

import logging

from geopy.distance import distance
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from fell_loader.base import BaseSparkLoader
from fell_loader.schemas.sanitised import EDGES_SCHEMA

logger = logging.getLogger(__name__)


class EdgeSanitiser(BaseSparkLoader):
    """Defines sanitising logic for the edges dataset"""

    @staticmethod
    def drop_edges_without_elevation(edges: DataFrame) -> DataFrame:
        """Drop any edges which haven't been tagged with elevation data. This
        can happen when the user uploads a .osm file which extends beyond the
        range which they have provided LIDAR data for.

        Args:
            edges: The contents of the staged edges dataset

        Returns:
            A copy of the edges dataset with records with NULL in 'elevation'
            removed
        """
        return edges.dropna(subset=["elevation"])

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

    def flag_footways(self, edges: DataFrame) -> DataFrame:
        """Attempt to use tags to determine which edges have been explicitly
        marked as accessible by foot. This may indicate a public crossing over
        private land, or the presence of a footpath next to a busy road.

        Args:
            edges: A dataframe containing the edges from the OSM extract

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

        edges = edges.withColumn("explicit_footway", F.lit(False))
        edges = edges.withColumn("separate_footway", F.lit(False))
        for tag in footway_tags:
            edges = self._get_tag_as_column(edges, tag)
            # NOTE: sidewalk = separate denotes a sidewalk which is mapped as
            #       a separate way. We don't want to send people down the road,
            #       as the sidewalk will also be pulled through as part of the
            #       graph.

            explicit_mask = F.col(tag).isNotNull() & ~(
                F.col(tag).isin("no", "separate")
            )
            separate_mask = F.col(tag).isNotNull() & (F.col(tag) == "separate")

            edges = edges.withColumn(
                "explicit_footway", F.col("explicit_footway") | explicit_mask
            ).withColumn(
                "separate_footway", F.col("separate_footway") | separate_mask
            )
            edges = edges.drop(tag)

        return edges

    def remove_restricted_routes(self, edges: DataFrame) -> DataFrame:
        """Remove any edges which correspond to routes with access controls,
        which are not usable by the public

        Args:
            edges: A dataframe containing the edges from the OSM extract

        Returns:
            A filtered copy of the input dataset

        """
        edges = self._get_tag_as_column(edges, "access")

        accessible_mask = F.col("access").isin(
            "yes", "permissive", "designated"
        )

        noflag_mask = F.col("access").isNull()

        explicit_mask = F.col("explicit_footway")

        edges = edges.filter(
            accessible_mask | noflag_mask | explicit_mask
        ).drop("access")

        return edges

    def remove_unsafe_routes(self, edges: DataFrame) -> DataFrame:
        """Some parts of the map will not be suitable for use by the routing
        algorithm as they are not safe to run on. This function will exclude
        all motorways, and any high-speed roads (<=50 mph) which do not have
        a footpath.

        Args:
            edges: A dataframe containing the edges from the OSM extract

        Returns:
            A filtered copy of the input dataset

        """
        # Drop motorways & motorway links
        edges = self._get_tag_as_column(edges, "highway")
        not_motorway_mask = (
            ~F.col("highway").contains(F.lit("motorway"))
        ) | F.col("highway").isNull()
        edges = edges.filter(not_motorway_mask)

        # Drop any roads where the footway forms a separate edge
        separate_mask = F.col("separate_footway")
        edges = edges.filter(~separate_mask)

        # Get rid of roundabouts
        edges = self._get_tag_as_column(edges, "junction")
        not_roundabout_mask = F.col("junction") != "roundabout"
        not_junction_mask = F.col("junction").isNull()
        edges = edges.filter(not_roundabout_mask | not_junction_mask).drop(
            "junction"
        )

        # Extract max speed on other roads as an integer
        edges = self._get_tag_as_column(edges, "maxspeed")

        edges = edges.withColumn(
            "maxspeed",
            F.regexp_extract(
                F.col("maxspeed"), r"[A-Za-z\s]*(\d{1,3})[A-Za-z\s]*", 1
            ).try_cast(T.IntegerType()),
        )

        # Keep records with max speed below 50, no known max speed, or have
        # been flagged as having a footpath
        low_speed_mask = F.col("maxspeed") < 50
        no_speed_mask = F.col("maxspeed").isNull()
        footway_mask = F.col("explicit_footway")
        edges = edges.filter(
            low_speed_mask | no_speed_mask | footway_mask
        ).drop("maxspeed")

        return edges

    def set_flat_flag(self, edges: DataFrame) -> DataFrame:
        """For any edges which correspond to a feature which means data from
        a terrain model for elevation will not apply (i.e. a bridge or a
        tunnel), set a flag to ensure the elevation data for these edges is
        masked.

        Args:
            edges: A dataframe containing the edges from the OSM extract

        Returns:
            A copy of the input dataset with an additional boolean
            'is_flat' column

        """
        edges = self._get_tag_as_column(edges, "bridge")
        edges = self._get_tag_as_column(edges, "tunnel")

        edges = edges.withColumn(
            "is_flat",
            (F.col("bridge").isNotNull()) | (F.col("tunnel").isNotNull()),
        ).drop("bridge", "tunnel")

        return edges

    def set_oneway_flag(self, edges: DataFrame) -> DataFrame:
        """For any routes which are not one-way, we will need to create a
        reversed view to enable graph traversal in both directions. This
        information is recorded in the tags, so we must retrieve it for
        ease of use.

        Args:
            edges: A dataframe containing the edges from the OSM extract

        Returns:
            A copy of the input dataset with an additional boolean 'oneway'
            column

        """
        edges = self._get_tag_as_column(edges, "oneway")

        bidirectional_mask = F.col("oneway") == "no"
        noflag_mask = F.col("oneway").isNull()

        edges = edges.withColumn(
            "oneway",
            ~(bidirectional_mask | noflag_mask),
        )

        return edges

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

        @F.udf(T.MapType(T.StringType(), T.DoubleType()))
        def changes_udf(elevation: list[float]) -> dict[str, float]:
            """Calculate total gain/loss for each edge. A UDF has been used as
            the alternative explode operation would also be very expensive, and
            this operation cannot be achieved with native spark functions.

            Args:
                elevation: The (ordered) list of elevation samples along the
                    edge. At time of writing, samples are taken every 10m.

            Returns:
                A dict containing 'gain' and 'loss' keys, showing total
                gain/loss accrued when traversing the edge
            """
            # If data suggests a grade of >60%, zero it out
            MAX_DELTA = 6.0
            output = {"elevation_gain": 0.0, "elevation_loss": 0.0}

            last_point = None
            for point in elevation:
                # Skip first point, need something to compare to
                if last_point is None:
                    last_point = point
                    continue

                # Check that absolute change is within expected bounds
                delta = abs(point - last_point)
                valid_step = delta < MAX_DELTA

                # Add step to gain/loss ass appropriate
                if (point > last_point) & valid_step:
                    output["elevation_gain"] += point - last_point
                elif (point < last_point) & valid_step:
                    output["elevation_loss"] += last_point - point
                last_point = point

            return output

        edges = edges.withColumn(
            "changes",
            F.when(
                # Zero-out changes when edge is known to be flat
                F.col("is_flat"),
                F.create_map(),
            ).otherwise(
                # Otherwise, sum changes across the array
                changes_udf(F.col("elevation"))
            ),
        )
        edges = (
            edges.withColumns(
                {
                    "elevation_gain": F.col("changes").getItem(
                        "elevation_gain"
                    ),
                    "elevation_loss": F.col("changes").getItem(
                        "elevation_loss"
                    ),
                }
            )
            .fillna(0.0, subset=["elevation_gain", "elevation_loss"])
            .drop("changes", "elevation")
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
    def _swap_columns(reverse: DataFrame, src: str, dst: str) -> DataFrame:
        # src --> old_src
        reverse = reverse.withColumn(f"old_{src}", F.col(src))
        # dst --> src
        reverse = reverse.withColumn(src, F.col(dst))
        # old_src --> dst
        reverse = reverse.withColumn(dst, F.col(f"old_{src}"))
        reverse = reverse.drop(f"old_{src}")
        return reverse

    def add_reverse_edges(self, edges: DataFrame) -> DataFrame:
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

        # Swap IDs & Coordinates of src/dst, invert loss/gain
        for src_col, dst_col in [
            ("src", "dst"),
            ("src_lat", "dst_lat"),
            ("src_lon", "dst_lon"),
            ("elevation_gain", "elevation_loss"),
        ]:
            reverse = self._swap_columns(reverse, src_col, dst_col)

        # Create new way ID and invert position of edge in way
        reverse = reverse.withColumn("way_id", F.col("way_id") * -1)
        reverse = reverse.withColumn("way_inx", F.col("way_inx") * -1)

        # Add inverted edges onto the original dataset
        out = edges.unionByName(reverse)

        return out

    def run(self) -> None:
        """End-to-end script for the sanitising of edge data. This reads in the
        contents of the edges dataset from the staging layer, removes any
        edges which are not suitable for runners and tags each edge with
        additional metrics. Elevation gain/loss is calculated based on the
        LIDAR samples for each edge (with some post-processing), and the
        distance for each edge is calculated based on its start/end
        coordinates.
        """
        # Read in data
        edges = self.read_delta(layer="staging", dataset="edges").repartition(
            512  # Prevents hang when running against whole-UK data
        )
        edges = self.drop_edges_without_elevation(edges)

        # Limit to edges which are suitable for runners
        edges = self.flag_footways(edges)
        edges = self.remove_restricted_routes(edges)
        edges = self.remove_unsafe_routes(edges)

        # Calculate elevation gain/loss & distance
        edges = self.set_flat_flag(edges)
        edges = self.calculate_elevation_changes(edges)
        edges = self.calculate_edge_distances(edges)

        # Add reverse edges
        edges = self.set_oneway_flag(edges)
        edges = self.add_reverse_edges(edges)

        # Write to disk
        edges = self._get_tag_as_column(edges, "surface")
        edges = self.map_to_schema(edges, EDGES_SCHEMA)
        self.write_parquet(edges, layer="sanitised", dataset="edges")
