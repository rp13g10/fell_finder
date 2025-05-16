import daft

ways_df = daft.read_parquet(
    "/home/ross/repos/fell_finder/data/extracts/osm/hampshire-latest.osm.pbf.way.parquet"
)

print(ways_df.schema())

print(ways_df.show())
