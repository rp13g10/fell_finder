"""Contains a helper class which iterates through all of the CSV
files present in the optimised data directory and copies them directly
into postgres
"""

import logging
import os
from pathlib import Path

import psycopg2 as pg
from psycopg2.extensions import connection
from tqdm import tqdm

from fell_loader.base import BaseLoader

logger = logging.Logger(__name__)

CUR_DIR = Path(__file__).parent.absolute()

with (CUR_DIR / "init_db.sql").open("r", encoding="utf8") as file:
    INIT_DB_QUERY = file.read()

with (CUR_DIR / "init_ptns.sql").open("r", encoding="utf8") as file:
    INIT_PTNS_TEMPLATE = file.read()

with (CUR_DIR / "copy_nodes.sql").open("r", encoding="utf8") as file:
    COPY_NODES_QUERY = file.read()

with (CUR_DIR / "copy_edges.sql").open("r", encoding="utf8") as file:
    COPY_EDGES_QUERY = file.read()


class GraphUploader(BaseLoader):
    """Class responsible for initializing the postgres database and loading
    data into it
    """

    def __init__(self) -> None:
        super().__init__()

        self.db = self._get_db_conn()
        self.db.autocommit = True

    def _get_credentials(self) -> tuple[str, str]:
        """Quick & dirty function to fetch credentials for database access,
        this will be phased out once project migrates over to containers

        Returns:
            tuple[str, str]: User provided username, password

        """
        # TODO: Set up error handling
        user = os.environ["FF_DB_USER"]
        pass_ = os.environ["FF_DB_PASS"]

        return user, pass_

    def _get_db_conn(self) -> connection:
        """Fetch a connection to the fell_finder database, user will need to
        provide credentials for login

        Returns:
            A psycopg2 connection to the database

        """
        user, pass_ = self._get_credentials()
        conn = pg.connect(dbname="fell_finder", user=user, password=pass_)
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
            for lat in range(50, 60):
                min_lat = lat - 1
                for lon in range(-2, 11):
                    min_lon = lon - 1
                    ptn = f"{min_lat}_{min_lon}".replace("-", "n")
                    query.append(
                        INIT_PTNS_TEMPLATE.format(
                            table=table,
                            ptn=ptn,
                        ).replace("_-", "_n")
                    )

        query_str = "\n".join(query)
        return query_str

    def init_db(self) -> None:
        """Initializes the database with all required tables, indexes and
        partitions ready for data to be loaded.
        """
        init_ptns_query = self._gen_ptns_query()

        logger.info("Initializing database")
        cur = self.db.cursor()
        cur.execute(INIT_DB_QUERY)
        cur.execute(init_ptns_query)

        cur.close()
        logger.info("Database initialization complete")

    def upload_nodes(self) -> None:
        """Loads each CSV file in the optimised nodes folder into the db"""
        to_load = list((self.data_dir / "optimised" / "nodes").glob("*.csv"))

        cur = self.db.cursor()

        # TODO: Switch back to using COPY command once running in containers,
        #       this seems to go via stdout

        logger.info("Uploading nodes to database")
        for path in tqdm(to_load):
            logger.debug("Loading file %s", path)
            with path.open(encoding="utf8") as file:
                cur.copy_expert(
                    COPY_NODES_QUERY,
                    file,
                )
        cur.close()
        logger.info("Upload completed")

    def upload_edges(self) -> None:
        """Loads each CSV file in the optimised edges folder into the db"""
        to_load = list((self.data_dir / "optimised" / "edges").glob("*.csv"))

        cur = self.db.cursor()

        logger.info("Uploading edges to database")
        for path in tqdm(to_load):
            logger.debug("Loading file %s", path)
            with path.open(encoding="utf8") as file:
                cur.copy_expert(
                    COPY_EDGES_QUERY,
                    file,
                )
        cur.close()
        logger.info("Upload completed")

    def run(self) -> None:
        """Loads all CSV files into the db"""
        self.init_db()
        self.upload_nodes()
        self.upload_edges()
