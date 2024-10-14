"""Generate some very simple graph data which can be read in by the
GraphFetcher class"""

import itertools
import os
import shutil
import polars as pl

from fell_finder.ingestion.parsing.osm_loader import OsmLoader
from fell_finder.utils.partitioning import add_partitions_to_polars_df

# NOTE
# For the coordinates and target distance used in the unit tests, the route
# covers the following area
# Easting ptn between 105 and 107
# Northing ptn between 35 and 37
# Min lat, lon (51.45161837130822, -0.2216311269341653)
# Max lat, lon (51.550487726034426, -0.06321826026211329)

cur_dir = os.path.dirname(os.path.abspath(__file__))

lats = list(x / 100 for x in range(5140, 5161))
lons = list(x / 100 for x in range(-30, 1))

pairs = list(itertools.product(lats, lons))

nodes_df = pl.DataFrame(
    data=pairs, schema={"lat": pl.Float64(), "lon": pl.Float64()}
)

nodes_df = OsmLoader.assign_bng_coords(nodes_df)
nodes_df = add_partitions_to_polars_df(nodes_df)
nodes_df = nodes_df.with_row_index("id", 0)
nodes_df = nodes_df.with_columns((pl.col("id") / 10).alias("elevation"))


shutil.rmtree(os.path.join(cur_dir, "data/optimised/nodes"))
nodes_df.write_parquet(
    os.path.join(cur_dir, "data/optimised/nodes"),
    use_pyarrow=True,
    pyarrow_options={"partition_cols": ["easting_ptn", "northing_ptn"]},
)

src_df = nodes_df.select(
    pl.col("id").alias("src"),
    "easting",
    "northing",
    "easting_ptn",
    "northing_ptn",
)

dst_df = nodes_df.select(
    (pl.col("id") + 1).alias("dst"),
)

edges_df = src_df.join(
    dst_df, left_on="src", right_on="dst", how="inner", coalesce=False
)

edges_df = edges_df.with_columns(
    pl.lit("valid").alias("highway"),
    pl.lit("valid").alias("surface"),
    (pl.col("src") / 2).alias("distance"),
    (pl.col("src") / 5).alias("elevation_gain"),
    (pl.col("src") / 10).alias("elevation_loss"),
    pl.lit("geometry").alias("geometry"),
)

edges_df = OsmLoader.set_edge_output_schema(edges_df)

invalid_static_data = {
    "easting": 528833,
    "northing": 179536,
    "easting_ptn": 106,
    "northing_ptn": 36,
    "distance": 42.0,
    "elevation_gain": 5.0,
    "elevation_loss": 5.0,
}
invalid_edges_df = pl.DataFrame(
    data=[
        {
            "src": 9000,
            "dst": 9001,
            "highway": "invalid",
            "surface": "valid",
            **invalid_static_data,
        },
        {
            "src": 9001,
            "dst": 9002,
            "highway": "valid",
            "surface": "invalid",
            **invalid_static_data,
        },
        {
            "src": 9002,
            "dst": 9003,
            "highway": "invalid",
            "surface": "invalid",
            **invalid_static_data,
        },
    ],
    schema={
        "src": pl.Int32(),
        "dst": pl.Int32(),
        "highway": pl.String(),
        "surface": pl.String(),
        "distance": pl.Float64(),
        "elevation_gain": pl.Float64(),
        "elevation_loss": pl.Float64(),
        "geometry": pl.String(),
        "easting": pl.Int32(),
        "northing": pl.Int32(),
        "easting_ptn": pl.Int32(),
        "northing_ptn": pl.Int32(),
    },
)
invalid_edges_df = OsmLoader.set_edge_output_schema(invalid_edges_df)

edges_df = pl.concat([edges_df, invalid_edges_df], how="vertical")
