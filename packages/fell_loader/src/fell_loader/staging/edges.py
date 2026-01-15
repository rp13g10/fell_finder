"""Staging logic for the edges datasets. Assigns elevations to all edges,
with updates applied as a delta on top of data already in the staging layer.
"""

import logging

from delta import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

import fell_loader.schemas.landing as lnd
import fell_loader.schemas.staging as stg
from fell_loader.staging.base import BaseStager

CHUNK_SIZE = 100_000
ELEVATION_RES_M = 10

logger = logging.getLogger(__name__)


class EdgeStager(BaseStager):
    """Defines staging logic for the edges dataset."""

    def initialize_output_table(self) -> None:
        """If no data is present in the staging layer, create an empty table"""
        DeltaTable.createIfNotExists(self.spark).location(
            (self.data_dir / "staging" / "edges").as_posix()
        ).addColumns(stg.EDGES_SCHEMA.fields).execute()

    def get_edges_to_remove(self) -> DataFrame | None:
        """Determine which edges need to be removed by finding any src/dst
        pairs which are present in the existing edges dataset, but not in the
        new one.

        Returns:
            A dataframe of src/dst pairs to be removed, or None (if no removals
            are required)

        """
        if self.removals_applied:
            return None

        new_edges = self.read_parquet(layer="landing", dataset="edges").select(
            "src", "dst"
        )

        # Staging layer must be read in as delta table
        current_edges = self.read_delta(
            layer="staging", dataset="edges"
        ).select("src", "dst")

        # Need to remove edges in staging layer which are not in the landing
        # layer
        to_delete = (
            current_edges.join(new_edges, on=["src", "dst"], how="anti")
            .select("src", "dst")
            .alias("removals")
        )

        return to_delete

    def get_edges_to_update(self) -> DataFrame:
        """Determine which edges need to be updated. Updates are required
        either when the last modified timestamp for a edge has changed (as
        someone might have improved its position on the map), or the edge
        is a new addition to the map.

        edges are updated in chunks due to the size of the table we need to
        join them to. This helps to reduce storage requirements, and more
        importantly allows the join to be aborted & resumed midway through.

        Returns:
            A dataframe containing a chunk of edges which need to be updated

        """
        logger.debug("Determining edges to update")
        # Read in new & existing datasets
        new_edges = self.read_parquet(layer="landing", dataset="edges")

        current_edges = self.read_delta(
            layer="staging", dataset="edges"
        ).select(
            "src",
            "dst",
            F.col("timestamp").alias("cur_timestamp"),
        )

        # Determine which edges need updating
        update_mask = F.col("timestamp") > F.col("cur_timestamp")
        new_mask = F.col("cur_timestamp").isNull()
        to_update = (
            new_edges.join(current_edges, on=["src", "dst"], how="left")
            .filter(update_mask | new_mask)
            .drop("cur_timestamp")
        )

        if self.remaining_count == -1:
            self.remaining_count = to_update.count()

        # Sort according to BNG grid to minimise number of LIDAR partitions
        # required, then select first N records
        to_update = to_update.orderBy(
            # NOTE: Each LIDAR file has dimensions 5000x5000, so this is
            #       analogous to sorting by file ID
            F.try_divide(F.col("src_easting"), F.lit(5000)).cast("integer"),
            F.try_divide(F.col("src_northing"), F.lit(5000)).cast("integer"),
        ).limit(CHUNK_SIZE)

        # If no data is returned, the load operation can be stopped
        self.current_chunk_size = to_update.count()
        if not self.current_chunk_size:
            self.updates_applied = True
            logger.info("No further updates required")
        else:
            logger.info(
                f"Updating {self.current_chunk_size:,.0f} of "
                f"{self.remaining_count:,.0f} edges"
            )

        return to_update

    def read_elevation(self, edges: DataFrame) -> DataFrame:
        """Read in the elevation data required to tag the edges in a
        particular chunk of records. Use the known bounds of the different
        partitions in the elevation dataset to limit the volume of data which
        is read in.

        Args:
            edges: A dataframe containing a chunk of edges to be updated

        Returns:
            A dataframe containing the elevation data for the area around the
            provided edges

        """
        bounds = self.read_parquet(
            layer="landing", dataset="lidar_bounds.parquet"
        )

        # Get file IDs for edges by joining onto bounds and getting distinct
        # values
        src_cond = (
            (edges.src_easting >= bounds.min_easting)
            & (edges.src_easting < bounds.max_easting)
            & (edges.src_northing >= bounds.min_northing)
            & (edges.src_northing < bounds.max_northing)
        )

        dst_cond = (
            (edges.dst_easting >= bounds.min_easting)
            & (edges.dst_easting < bounds.max_easting)
            & (edges.dst_northing >= bounds.min_northing)
            & (edges.dst_northing < bounds.max_northing)
        )

        join_cond = src_cond | dst_cond

        # Collect required IDs as list of strings
        files_df = (
            edges.join(F.broadcast(bounds), on=join_cond)
            .select("file_id")
            .dropDuplicates()
        )

        files = [row.file_id for row in files_df.collect()]
        logger.debug("Loading elevation for: %s", repr(files))

        # Read in elevation dataset, limiting to only the file paths required
        paths = [
            (
                self.data_dir / "landing" / "lidar" / f"{file}.parquet"
            ).as_posix()
            for file in files
        ]

        # Schema must be provided, as `paths` may be empty
        elevation = self.spark.read.schema(lnd.LIDAR_SCHEMA).parquet(*paths)

        return elevation

    @staticmethod
    def calculate_step_metrics(edges: DataFrame) -> DataFrame:
        """Work out the number steps which each edge will need to be broken
        down into in order to calculate the elevation gain/loss at the
        specified resolution. The number of steps will always be at least 2, to
        ensure that elevation at the start and end points is always taken into
        account.

        Args:
            edges: A table representing edges in the OSM graph

        Returns:
            A copy of the input table with an additional 'edge_steps' column

        """
        # Calculate edge distance based on start/end coords
        # each increment is ~1m, disregarding the curvature of the earth
        edges = edges.withColumn(
            "edge_size_h", F.col("dst_easting") - F.col("src_easting")
        )
        edges = edges.withColumn(
            "edge_size_v", F.col("dst_northing") - F.col("src_northing")
        )
        edges = edges.withColumn(
            "edge_size",
            ((F.col("edge_size_h") ** 2) + (F.col("edge_size_v") ** 2)) ** 0.5,
        )

        # Calculate required number of points
        edges = edges.withColumn(
            "edge_steps",
            F.round(
                (F.col("edge_size") / F.lit(ELEVATION_RES_M)) + F.lit(1)
            ).astype(T.IntegerType()),
        )

        # Set minimum of 2 points (start & end)
        edges = edges.withColumn(
            "edge_steps", F.greatest(F.lit(2), F.col("edge_steps"))
        )
        return edges

    @staticmethod
    def explode_edges(edges: DataFrame) -> DataFrame:
        """Starting from one record per edge in the OSM graph, explode the
        dataset out to (approximately) one record for every 10 metres along
        each edge. The 10m resolution can be changed via the
        ELEVATION_RES_M variable.

        Args:
            edges: A table representing edges in the OSM graph

        Returns:
            An exploded view of the input dataset

        """

        @F.udf(returnType=T.ArrayType(T.DoubleType()))
        def linspace_udf(start: int, end: int, n_steps: int) -> list[float]:
            """Return a list of n_steps evenly spaced points between the start
            and end coordinates.

            Args:
                start: The starting easting/northing
                end: The ending easting/northing
                n_steps: The number of steps to generate

            Returns:
                A list of evenly spaced points between start and end

            """
            start_f, end_f = float(start), float(end)
            range_ = end_f - start_f
            if n_steps == 2:
                return [start_f, end_f]

            delta = range_ / (n_steps - 1)
            return [start_f + (delta * step) for step in range(n_steps)]

        @F.udf(returnType=T.ArrayType(T.IntegerType()))
        def inx_udf(n_checkpoints: int) -> list[int]:
            """Returns a list of indexes between 0 and n_checkpoints

            Args:
                n_checkpoints: The number of indexes to generate

            Returns:
                A list of indexes between 0 and n_checkpoints

            """
            return list(range(n_checkpoints))

        # Generate the eastings, northings and indexes for each point
        edges = edges.withColumn(
            "easting_arr",
            linspace_udf(
                F.col("src_easting"),
                F.col("dst_easting"),
                F.col("edge_steps"),
            ),
        )

        edges = edges.withColumn(
            "northing_arr",
            linspace_udf(
                F.col("src_northing"),
                F.col("dst_northing"),
                F.col("edge_steps"),
            ),
        )

        edges = edges.withColumn("edge_inx_arr", inx_udf(F.col("edge_steps")))

        # Zip them into a single column [(inx, easting, northing), ...]
        edges = edges.withColumn(
            "coords_arr",
            F.arrays_zip(
                F.col("edge_inx_arr"),
                F.col("easting_arr"),
                F.col("northing_arr"),
            ),
        )

        # Explode the dataset to one record per step
        edges = edges.withColumn("coords", F.explode(F.col("coords_arr")))
        edges = edges.drop(
            "edge_inx_arr", "easting_arr", "northing_arr", "coords_arr"
        )

        return edges

    @staticmethod
    def unpack_exploded_edges(edges: DataFrame) -> DataFrame:
        """Standardize the schema of the exploded edges dataset, unpacking
        indexes, eastings and northing for each step into standard columns.

        Args:
            edges: A table representing exploded edges in the OSM graph

        Returns:
            A tidied up view of the input dataset

        """
        edges = edges.select(
            "src",
            "dst",
            "src_lat",
            "src_lon",
            "dst_lat",
            "dst_lon",
            "src_easting",
            "src_northing",
            "way_id",
            "way_inx",
            F.col("coords.edge_inx_arr").alias("edge_inx"),
            F.round(F.col("coords.easting_arr"))
            .astype(T.IntegerType())
            .alias("easting"),
            F.round(F.col("coords.northing_arr"))
            .astype(T.IntegerType())
            .alias("northing"),
            "tags",
            "timestamp",
        )
        return edges

    @staticmethod
    def tag_exploded_edges(
        edges: DataFrame, elevation: DataFrame
    ) -> DataFrame:
        """Join the exploded edges dataset onto the elevation table to retrieve
        the elevation at multiple points along each edge.

        Args:
            edges: A table representing exploded edges in the OSM graph
            elevation: A table containing elevation data

        Returns:
            A copy of the edges dataframe with an additional elevation column

        """
        tagged = edges.join(
            elevation,
            on=["easting", "northing"],
            how="left",
        )

        tagged = tagged.select(
            "src",
            "dst",
            "src_lat",
            "src_lon",
            "dst_lat",
            "dst_lon",
            "src_easting",
            "src_northing",
            "edge_inx",
            "elevation",
            "way_id",
            "way_inx",
            "tags",
            "timestamp",
        )

        return tagged

    @staticmethod
    def implode_edges(edges: DataFrame) -> DataFrame:
        """Collect the elevation data for the edge as an array, rolling the
        dataset back up to one record per edge.

        Args:
            edges: A table representing exploded edges in the OSM graph

        Returns:
            An aggregated view of the input dataset, with one record per edge

        """
        # Need to get back to one record per edge
        edges = edges.groupBy(
            "src",
            "dst",
        ).agg(
            # For metadata columns, all values will be the same per edge
            *[
                F.first_value(col).alias(col)
                for col in [
                    "src_lat",
                    "src_lon",
                    "dst_lat",
                    "dst_lon",
                    "src_easting",
                    "src_northing",
                    "way_id",
                    "way_inx",
                    "tags",
                    "timestamp",
                ]
            ],
            # Collect elevations into an array of structs [{inx=1, ele=1}, ...]
            F.collect_list(F.struct("edge_inx", "elevation")).alias(
                "elevation_pairs"
            ),
        )

        # Get elevation as an array, sorted by index
        edges = edges.withColumn(
            "elevation",
            F.transform(
                F.sort_array(F.col("elevation_pairs")),
                lambda x: x.getItem("elevation"),
            ),
        ).drop("elevation_pairs")

        # Null out elevation if only partial data is present
        edges = edges.withColumn(
            "elevation",
            F.when(
                F.exists(F.col("elevation"), lambda x: x.isNull()), F.lit(None)
            ).otherwise(F.col("elevation")),
        )
        return edges

    def apply_edge_updates(
        self, updates: DataFrame, removals: DataFrame | None
    ) -> None:
        """Apply a chunk of edge updates to the data in the staging layer.

        Args:
            updates: A dataframe containing updated data for a chunk of edges
            removals: A list of edge IDs which should be removed

        """
        logger.debug("Writing edge updates to temp folder")
        updates.write.format("delta").mode("overwrite").save(
            (self.data_dir / "temp" / "edge_updates").as_posix()
        )

        # Create references to target table & updates
        edges_tbl = DeltaTable.forPath(
            self.spark, (self.data_dir / "staging" / "edges").as_posix()
        ).alias("edges")
        updates_tbl = DeltaTable.forPath(
            self.spark, (self.data_dir / "temp" / "edge_updates").as_posix()
        )
        updates_df = updates_tbl.toDF().alias("updates")

        # Update or insert records
        logger.info("Applying edge updates")
        edges_tbl.merge(
            updates_df,
            condition=(
                "(edges.src = updates.src) AND (edges.dst = updates.dst)"
            ),
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        logger.info("edge updates applied")

        # Remove records
        if removals is not None:
            remove_count = removals.count()
            logger.info(
                f"Removing {remove_count} edges no longer present in map"
            )
            edges_tbl.merge(
                removals,
                condition=(
                    "(edges.src = removals.src) AND (edges.dst = removals.dst)"
                ),
            ).whenMatchedDelete().execute()
            logger.info("edge removals applied")
            self.removals_applied = True

    def optimise_edges_table(self) -> None:
        """Once the data load operation has finished, optimise the files in
        the staging layer to prevent file counts from getting out of control.
        """
        edges_tbl = DeltaTable.forPath(
            self.spark, (self.data_dir / "staging" / "edges").as_posix()
        )
        logger.info("Optimizing edges table")
        edges_tbl.optimize().executeCompaction()
        edges_tbl.vacuum()
        logger.info("Optimization completed")

    def record_lidar_files(self) -> None:
        """Store a record of the LIDAR files which are currently present in
        the landing layer. Future runs will reference this in order to detect
        the presence of new files. The file list is written to
        'staging/last_run_lidar.txt'
        """
        lidar_files = (self.data_dir / "landing" / "lidar").glob("*.parquet")

        (self.data_dir / "staging" / "last_run_lidar.txt").write_text(
            str(sorted(path.as_posix() for path in lidar_files))
        )

    def run(self) -> None:
        """End-to-end script for the staging of edge data. This reads in the
        contents of the edges dataset from the landing layer, calculates which
        records need to be updated and tags them with elevation data. Any edges
        which have not been changed since the last data load will not be
        modified.
        """
        self.initialize_output_table()

        if self.check_for_lidar_update():
            self.clear_data_without_elevation(dataset="edges")

        to_remove = self.get_edges_to_remove()
        to_update_df = self.get_edges_to_update()

        while not self.updates_applied:
            # Free up memory before each new iteration
            self.spark._jvm.System.gc()  # type: ignore

            elevation_df = self.read_elevation(to_update_df)

            to_update_df = self.calculate_step_metrics(to_update_df)
            to_update_df = self.explode_edges(to_update_df)
            to_update_df = self.unpack_exploded_edges(to_update_df)
            to_update_df = self.tag_exploded_edges(to_update_df, elevation_df)
            to_update_df = self.implode_edges(to_update_df)

            to_update_df = self.map_to_schema(to_update_df, stg.EDGES_SCHEMA)

            self.apply_edge_updates(to_update_df, to_remove)

            self.remaining_count -= self.current_chunk_size

            to_remove = self.get_edges_to_remove()
            to_update_df = self.get_edges_to_update()

        self.optimise_edges_table()
        self.record_lidar_files()
        self.clear_temp_files("edge_updates")
