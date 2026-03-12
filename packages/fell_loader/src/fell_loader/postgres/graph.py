"""Contains a helper class which iterates through all of the CSV
files present in the optimised data directory and copies them directly
into postgres
"""

import logging
from functools import lru_cache
from pathlib import Path
from typing import Literal

import psycopg2 as pg
from psycopg2.extensions import connection

from fell_loader.base import BaseLoader
from fell_loader.utils import get_env_var

logger = logging.getLogger(__name__)

CUR_DIR = Path(__file__).parent.absolute()


class GraphUploader(BaseLoader):
    """Class responsible for initializing the postgres database and loading
    data into it
    """

    def __init__(self) -> None:
        super().__init__()
        self.db = self._get_db_conn()
        self.db.autocommit = True

    @staticmethod
    @lru_cache
    def _get_sql_query(
        query_name: Literal[
            "init_db", "init_ptns", "copy_nodes", "copy_edges"
        ],
    ) -> str:
        """Fetch one of the pre-defined SQL queries from the `postgres` folder

        Args:
            query_name: The name of the query to be retrieved, this must
                correspond to one of the .sql files stored in the
                same folder as this file

        Returns:
            The contents of the requested .sql file
        """
        with (CUR_DIR / f"{query_name}.sql").open(
            "r", encoding="utf8"
        ) as file:
            query = file.read()
        return query

    def _get_db_conn(self) -> connection:
        """Fetch a connection to the fell_finder database, user will need to
        provide credentials for login

        Returns:
            A psycopg2 connection to the database

        """
        logger.debug("Connecting to database")
        pg_host = get_env_var("FF_DB_HOST")
        pg_port = get_env_var("FF_DB_PORT")
        pg_user = get_env_var("FF_DB_USER")
        pg_pass = get_env_var("FF_DB_PASS")

        pg_uri = (
            f"postgres://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/fell_finder"
        )
        conn = pg.connect(pg_uri)
        logger.debug("Connected to database")
        return conn

    def _gen_ptns_query(self) -> str:
        """Programmatically generate a query which will generate all required
        partitions for the nodes & edges tables. The range of lats/lons used
        should cover the entirety of the British Isles.

        Returns:
            A SQL query which will create partitions on the nodes and
            edges tables

        """
        query = []
        for table in ["nodes", "edges"]:
            for lat in range(49, 62):
                min_lat = lat - 1
                for lon in range(-14, 3):
                    min_lon = lon - 1
                    ptn = f"{min_lat}_{min_lon}".replace("-", "n")
                    query.append(
                        self._get_sql_query("init_ptns")
                        .format(
                            table=table,
                            ptn=ptn,
                        )
                        .replace("_-", "_n")
                    )

        query_str = "\n".join(query)
        return query_str

    def init_db(self) -> None:
        """Initializes the database with all required tables, indexes and
        partitions ready for data to be loaded.
        """
        logger.info("Initializing database")
        cur = self.db.cursor()
        cur.execute(self._get_sql_query("init_db"))
        cur.execute(self._gen_ptns_query())

        cur.close()
        logger.info("Database initialization complete")

    def upload_nodes(self) -> None:
        """Loads each CSV file in the optimised nodes folder into the db"""
        to_load = list((self.data_dir / "optimised" / "nodes").glob("*.csv"))

        cur = self.db.cursor()

        # TODO: Switch back to using COPY command once running in containers,
        #       this seems to go via stdout

        logger.info("Uploading nodes to database")
        for path in to_load:
            logger.debug("Loading file %s", path)
            with path.open(encoding="utf8") as file:
                cur.copy_expert(
                    self._get_sql_query("copy_nodes"),
                    file,
                )
        cur.close()
        logger.info("Upload completed")

    def upload_edges(self) -> None:
        """Loads each CSV file in the optimised edges folder into the db"""
        to_load = list((self.data_dir / "optimised" / "edges").glob("*.csv"))

        cur = self.db.cursor()

        logger.info("Uploading edges to database")
        for path in to_load:
            logger.debug("Loading file %s", path)
            with path.open(encoding="utf8") as file:
                cur.copy_expert(
                    self._get_sql_query("copy_edges"),
                    file,
                )
        cur.close()
        logger.info("Upload completed")

    def run(self) -> None:
        """Loads all CSV files into the db"""
        self.init_db()
        self.upload_nodes()
        self.upload_edges()
