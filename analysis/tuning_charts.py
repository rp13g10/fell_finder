"""Charts used for analysis of data gathered by tuning.py"""

import plotly.express as px
import polars as pl

# Read data
df = pl.read_csv("tuning_main.csv")

# TODO: Handle anomalous readings, smooth out data per dist/location?

# Determine % of max for each distance
df = df.with_columns(ratio=pl.col("max_ele") / pl.col("max_dist"))
df = df.with_columns(
    prev_max=pl.col("max_ele")
    .shift(n=1)
    .over(partition_by=["n_edges"], order_by="max_candidates"),
    next_max=pl.col("max_ele")
    .shift(n=1)
    .over(partition_by=["n_edges"], order_by="max_candidates"),
)
df = df.with_columns(
    delta_to_prev=(pl.lit(1) - (pl.col("max_ele") / pl.col("prev_max"))).abs(),
    delta_to_next=(pl.lit(1) - (pl.col("max_ele") / pl.col("next_max"))).abs(),
)
df = df.filter(
    ((pl.col("delta_to_prev") < 0.2) & (pl.col("delta_to_next") < 0.2))
    | ((pl.col("delta_to_prev").is_null()) & (pl.col("delta_to_next") < 0.2))
    | ((pl.col("delta_to_prev") < 0.2) & (pl.col("delta_to_next").is_null()))
)
df = df.with_columns(
    max_in_group=pl.max("ratio").over(
        partition_by=["n_edges"],
    )
)
df = df.with_columns(perc_of_max=pl.col("ratio") / pl.col("max_in_group"))

# Determine min cands to get acceptably close to the max
df = df.with_columns(
    meets_threshold=pl.when(pl.col("perc_of_max") >= 0.975)
    .then(pl.lit(1))
    .otherwise(pl.lit(0))
)
df = df.with_columns(
    num_in_group=pl.cum_count("ratio").over(
        partition_by=["n_edges"],
        order_by=["meets_threshold", -1 * pl.col("max_candidates")],
        descending=True,
    )
)

# Generate plots
plot_df = df.filter(pl.col("num_in_group") == 1)
fig = px.scatter(plot_df, x="n_edges", y="max_candidates", color="label")
fig.show()

# NOTE
# More data needed. Looks like setting n_candidates to equal the number of
# edges could be a reasonable approach?
