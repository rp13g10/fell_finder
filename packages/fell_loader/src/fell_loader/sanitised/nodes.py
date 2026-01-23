"""Sanitise the node data by restricting the scope of the dataset to only
the nodes which have a corresponding (usable) edge.
"""

import logging

from fell_loader.base import BaseSparkLoader
from fell_loader.schemas.sanitised import NODES_SCHEMA

logger = logging.getLogger(__name__)


class NodeSanitiser(BaseSparkLoader):
    """Defines sanitising logic for the nodes dataset"""

    def run(self) -> None:
        """End-to-end loading script for nodes going into the sanitised
        layer. This simply filters the nodes dataset to include only those
        which have a corresponding edge.
        """
        nodes = self.read_delta(layer="staging", dataset="nodes")
        edges = self.read_parquet(layer="sanitised", dataset="edges")

        nodes = self.drop_unused_nodes(nodes, edges)
        nodes = self.map_to_schema(nodes, NODES_SCHEMA)

        self.write_parquet(nodes, layer="sanitised", dataset="nodes")
