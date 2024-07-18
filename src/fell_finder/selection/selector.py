"""Contains the Selector class, which retrieves a graph containing all nodes
which can be reached from the requested route start point without going over
the max configured distance."""

import os
from typing import Dict, List, Tuple, Any

from geopy import distance, point
from pyarrow.parquet import ParquetDataset
import rustworkx as rx


from fell_finder.containers.geo import BBox
from fell_finder.containers.routes import RouteConfig

VALID_TYPES = {
    "footway",
    "living_street",
    "path",
    "pedestrian",
    "primary",
    "primary_link",
    "residential",
    "secondary",
    "secondary_link",
    "service",
    "steps",
    "tertiary",
    "tertiary_link",
    "track",
    "unclassified",
}

# TODO: Tidy this up, then update route finding algorithm to work with the
#       RustworkX graph type


class GraphNode:
    def __init__(self, node_dict: Dict[str, Any]):
        self.index = None

        self.lat = node_dict["lat"]
        self.lon = node_dict["lon"]
        self.elevation = node_dict["elevation"]

        self.dist_to_start = None

    def set_index(self, index) -> None:
        self.index = index

    def set_dist_to_start(self, dist: float) -> None:
        self.dist_to_start = dist


class GraphEdge:
    def __init__(self, edge_dict: Dict[str, Any]):
        self.index = None

        self.highway = edge_dict["highway"]
        self.surface = edge_dict["surface"]
        self.distance = edge_dict["distance"]
        self.elevation_gain = edge_dict["elevation_gain"]
        self.elevation_loss = edge_dict["elevation_loss"]
        self.geometry = edge_dict["geometry"]

    def set_index(self, index) -> None:
        self.index = index


class Selector:
    """Retrieves a graph containing all nodes which can be reached from the
    requested route start point without going over the max configured distance.
    """

    def __init__(self, config: RouteConfig, data_dir: str):
        """Create an instance of the graph enricher class based on the
        contents of the networkx graph specified by `source_path`

        Args:
            config (RouteConfig): A configuration file detailing the route
              requested by the user.
        """

        # Store down core attributes
        self.config = config
        self.bbox = self.get_bounding_box_for_route()
        self.data_dir = data_dir

        if self.config.terrain_types:
            assert all(x in VALID_TYPES for x in self.config.terrain_types)

        self.id_maps = {}
        self.inverse_maps = {}
        self.inv_to_id = {}

    def get_bounding_box_for_route(self) -> BBox:
        """Generate a square bounding box which contains a circle with diameter
        equal to the max requested distance.

        Returns:
            BBox: A bounding box for the entire route
        """
        start_point = point.Point(self.config.start_lat, self.config.start_lon)

        dist_to_corner = (self.config.max_distance / 2) * (2**0.5)

        nw_corner = distance.distance(meters=dist_to_corner).destination(
            point=start_point, bearing=315
        )

        se_corner = distance.distance(meters=dist_to_corner).destination(
            point=start_point, bearing=135
        )

        bbox = BBox(
            min_lat=se_corner.latitude,
            min_lon=nw_corner.longitude,
            max_lat=nw_corner.latitude,
            max_lon=se_corner.longitude,
        )

        return bbox

    def retrieve_nodes_for_bounding_box(
        self,
    ) -> Tuple[List[Tuple[int, GraphNode]], List[int]]:
        """For the provided bounding box, fetch a list of dictionaries from
        the enriched parquet dataset. Each entry in the list represents one
        node in the graph.

        Returns:
            List[Dict]: A list of node metadata
        """
        nodes_dataset = ParquetDataset(
            os.path.join(self.data_dir, "optimised/nodes"),
            filters=[
                ("easting_ptn", ">=", self.bbox.min_easting_ptn),
                ("easting_ptn", "<=", self.bbox.max_easting_ptn),
                ("northing_ptn", ">=", self.bbox.min_northing_ptn),
                ("northing_ptn", "<=", self.bbox.max_northing_ptn),
            ],
        )

        node_cols = ["id", "lat", "lon", "elevation"]

        nodes_list = nodes_dataset.read(columns=node_cols).to_pylist()

        all_nodes = [node_dict["id"] for node_dict in nodes_list]

        nodes_list = [
            (node_dict["id"], GraphNode(node_dict)) for node_dict in nodes_list
        ]

        return nodes_list, all_nodes

    def get_id_index_mappings(
        self, all_nodes: List[int], indices: List[int]
    ) -> Dict[int, int]:
        maps = {id_: ind for id_, ind in zip(all_nodes, indices)}

        return maps

    def retrieve_edges_for_bounding_box(
        self, id_maps: Dict[int, int]
    ) -> List[Tuple[int, int, GraphEdge]]:
        """For the provided bounding box, fetch a list of tuples from the
        enriched parquet dataset. Each entry in the list represents one edge
        in the graph.

        Returns:
            List[Tuple]: A list of edges & the corresponding metadata
        """

        filters = [
            ("easting_ptn", ">=", self.bbox.min_easting_ptn),
            ("easting_ptn", "<=", self.bbox.max_easting_ptn),
            ("northing_ptn", ">=", self.bbox.min_northing_ptn),
            ("northing_ptn", "<=", self.bbox.max_northing_ptn),
        ]

        edges_dataset = ParquetDataset(
            os.path.join(self.data_dir, "optimised/edges"),
            filters=filters,
        )

        edge_cols = [
            "src",
            "dst",
            "highway",
            "surface",
            "distance",
            "elevation_gain",
            "elevation_loss",
            "geometry",
        ]

        edges_list = edges_dataset.read(columns=edge_cols).to_pylist()

        edges_list = [
            (
                id_maps[edge_dict["src"]],
                id_maps[edge_dict["dst"]],
                GraphEdge(edge_dict),
            )
            for edge_dict in edges_list
            if (edge_dict["src"] in id_maps) and (edge_dict["dst"] in id_maps)
        ]

        return edges_list

    def _initialize_graph(self, graph: rx.PyDiGraph) -> rx.PyDiGraph:
        # Store down indexes, as recommended by rustworkx documentation
        for index in graph.node_indices():
            graph[index][1].index = index

        for index, data in graph.edge_index_map().items():
            data[2].index = index

        return graph

    def fetch_coarse_subgraph(self) -> Tuple[rx.PyDiGraph, rx.PyDiGraph]:
        """Fetch a graph which covers roughly the right area, by filtering with
        a square bounding box with edges the same length as the max requested
        route distance. Also returns an inverted version of the graph, which
        will be needed when applying Dijkstra's algorithm to calculate
        distances from each point back to the start.

        Returns:
            A rustworkx graph containing a coarsely filtered map of the
            requested area, and its inverse.
        """
        nodes_list, all_nodes = self.retrieve_nodes_for_bounding_box()

        graph = rx.PyDiGraph()
        node_indices = graph.add_nodes_from(nodes_list)
        id_maps = self.get_id_index_mappings(all_nodes, node_indices)  # type: ignore
        self.id_maps = id_maps

        edges_list = self.retrieve_edges_for_bounding_box(id_maps)
        graph.add_edges_from(edges_list)

        inverse_graph = rx.PyDiGraph()
        inverse_indices = inverse_graph.add_nodes_from(nodes_list)
        inverse_maps = self.get_id_index_mappings(all_nodes, inverse_indices)  # type: ignore
        self.inverse_maps = inverse_maps
        self.inv_to_id = {v: k for k, v in self.inverse_maps.items()}

        inverse_edges_list = self.retrieve_edges_for_bounding_box(inverse_maps)

        inverse_graph.add_edges_from(
            [(dst, src, attrs) for src, dst, attrs in inverse_edges_list]
        )

        graph = self._initialize_graph(graph)
        inverse_graph = self._initialize_graph(inverse_graph)

        return graph, inverse_graph

    def find_nearest_node(
        self, graph: rx.PyDiGraph, lat: float, lon: float
    ) -> int:
        closest_node = None
        closest_dist = None

        for node_inx in graph.node_indices():
            _, node_data = graph.get_node_data(node_inx)
            node_lat, node_lon = node_data.lat, node_data.lon

            dist = (((lat - node_lat) ** 2) + ((lon - node_lon) ** 2)) ** 0.5

            if closest_dist is None:
                closest_dist = dist
                closest_node = node_inx
            elif dist < closest_dist:
                closest_dist = dist
                closest_node = node_inx

        if closest_node is None:
            raise ValueError(
                "This doesn't look right, is there any data in your graph?"
            )

        return closest_node

    def inverse_inx_to_node_inx(self, inverse_inx: int):
        id_ = self.inv_to_id[inverse_inx]

        inx = self.id_maps[id_]

        return inx

    def tag_distances_to_start(
        self, graph: rx.PyDiGraph, inverse_graph: rx.PyDiGraph, start_node: int
    ) -> rx.PyDiGraph:
        """Tags each node in the graph with the distance & elevation which
        must be travelled in order to get back to the start point."""

        dists = rx.dijkstra_shortest_path_lengths(
            inverse_graph,
            start_node,
            edge_cost_fn=lambda attrs: attrs.distance,
        )

        for inv_inx, dist in dists.items():
            inx = self.inverse_inx_to_node_inx(inv_inx)
            graph[inx][1].set_dist_to_start(dist)

        return graph

    def generate_fine_subgraph(self, graph: rx.PyDiGraph, start_node: int):
        search_radius = self.config.max_distance / 2

        def check_removal_cond(
            data: Tuple[int, GraphNode], search_radius: float, start_node: int
        ):
            node_inx, attrs = data
            if attrs.dist_to_start is None:
                # Remove if no path back to start
                return False
            elif node_inx == start_node:
                # Keep start node
                return True
            # Remove nodes outside of search radius
            return attrs.dist_to_start > search_radius

        to_remove = graph.filter_nodes(
            lambda data: check_removal_cond(data, search_radius, start_node)
        )

        graph.remove_nodes_from(to_remove)

        return graph

    def create_graph(self) -> Tuple[int, rx.PyDiGraph]:
        graph, inverse_graph = self.fetch_coarse_subgraph()

        start_node = self.find_nearest_node(
            graph, self.config.start_lat, self.config.start_lon
        )

        graph = self.tag_distances_to_start(graph, inverse_graph, start_node)

        graph = self.generate_fine_subgraph(graph, start_node)

        return start_node, graph
