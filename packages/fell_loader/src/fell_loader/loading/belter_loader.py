"""Contains a helper class which iterates through all of the CSV
files present in the optimised data directory and copies them directly
into postgres"""

import os
from glob import glob

import psycopg2 as pg
from psycopg2.extensions import connection
from tqdm import tqdm

curdir = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(curdir, "init_db.sql"), "r", encoding="utf8") as fobj:
    INIT_DB_QUERY = fobj.read()

with open(os.path.join(curdir, "init_ptns.sql"), "r", encoding="utf8") as fobj:
    INIT_PTNS_TEMPLATE = fobj.read()

with open(
    os.path.join(curdir, "copy_nodes.sql"), "r", encoding="utf8"
) as fobj:
    COPY_NODES_QUERY = fobj.read()

with open(
    os.path.join(curdir, "copy_edges.sql"), "r", encoding="utf8"
) as fobj:
    COPY_EDGES_QUERY = fobj.read()


class BelterLoader:
    """Class responsible for initializing the postgres database and loading
    data into it"""

    def __init__(self, data_dir: str) -> None:
        self.db = self._get_db_conn()
        self.db.autocommit = True
        self.data_dir = data_dir

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
        """Programatically generate a query which will generate all required
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
        partitions ready for data to be loaded."""
        init_ptns_query = self._gen_ptns_query()

        cur = self.db.cursor()
        cur.execute(INIT_DB_QUERY)
        cur.execute(init_ptns_query)

        cur.close()

    def load_nodes(self) -> None:
        """Loads each CSV file in the optimised nodes folder into the db"""
        to_load = glob(
            os.path.join(self.data_dir, "optimised", "nodes", "*.csv")
        )

        cur = self.db.cursor()

        # TODO: Switch back to using COPY command once running in containers,
        #       this seems to go via stdout

        for file_name in tqdm(to_load):
            with open(file_name, "r", encoding="utf8") as fobj:
                cur.copy_expert(
                    COPY_NODES_QUERY,
                    fobj,
                )
        cur.close()

    def load_edges(self) -> None:
        """Loads each CSV file in the optimised edges folder into the db"""
        to_load = glob(
            os.path.join(self.data_dir, "optimised", "edges", "*.csv")
        )

        cur = self.db.cursor()

        for file_name in tqdm(to_load):
            with open(file_name, "r", encoding="utf8") as fobj:
                cur.copy_expert(
                    COPY_EDGES_QUERY,
                    fobj,
                )
        cur.close()
