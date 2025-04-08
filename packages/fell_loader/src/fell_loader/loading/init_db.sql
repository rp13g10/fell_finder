/* Run using psql -U ross -d fell_finder -a -f init_db.sql */ CREATE SCHEMA IF NOT EXISTS routing;

DROP TABLE IF EXISTS routing.nodes;

CREATE TABLE
    IF NOT EXISTS routing.nodes (
        id bigint,
        lat float NOT NULL,
        lon float NOT NULL,
        elevation float NOT NULL,
        ptn text NOT NULL,
        PRIMARY KEY (id, ptn)
    )
PARTITION BY
    LIST (ptn);

CREATE INDEX nodes_latlon_inx ON routing.nodes (lat, lon);

DROP TABLE IF EXISTS routing.edges;

CREATE TABLE
    IF NOT EXISTS routing.edges (
        src bigint,
        dst bigint,
        src_lat float NOT NULL,
        src_lon float NOT NULL,
        highway text,
        surface text,
        elevation_gain float,
        elevation_loss float,
        distance float,
        lats float ARRAY,
        lons float ARRAY,
        eles float ARRAY,
        dists float ARRAY,
        ptn text NOT NULL,
        PRIMARY KEY (src, dst, ptn)
    )
PARTITION BY
    LIST (ptn);

CREATE INDEX edges_latlon_inx ON routing.edges (src_lat, src_lon);

CREATE INDEX edges_highway_inx ON routing.edges (highway);

CREATE INDEX edges_surface_inx ON routing.edges (surface);