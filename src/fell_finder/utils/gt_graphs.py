"""General-purpose functions for graph-tool graphs"""

import numpy as np
import graph_tool.all as gt


def find_nearest_node(graph: gt.Graph, lat: float, lon: float) -> np.int64:
    """For a given latitude/longitude, find the node which is geographically
    closest to it, that is connected to at least one other node.
    As the network has already been compressed, this node will always be at
    a junction.

    Args:
        graph (gt.Graph): A graph-tool graph object
        lat (float): The target latitude
        lon (float): The target longitude

    Returns:
        np.int64: The ID for the node in `graph` which is closes to the start
            point"""

    lat_dists = graph.vertex_properties["lat"].get_array() - lat
    lon_dists = graph.vertex_properties["lon"].get_array() - lon
    sl_dists = ((lat_dists**2) + (lon_dists**2)) ** 0.5

    closest_node = np.argmin(sl_dists)

    return closest_node


def remove_isolates(graph: gt.Graph) -> gt.Graph:
    """Remove any nodes from the graph which are not connected to another
    node.

    Args:
        graph (gt.Graph): A graph-tool graph object

    Returns:
        gt.Graph: A filtered copy of the input graph
    """
    filter_prop = graph.new_vertex_property("bool")
    filter_vals = (
        graph.degree_property_map("total").get_array() > 0  # type: ignore
    )
    filter_prop.a = filter_vals

    graph.purge_edges()
    graph.purge_vertices()
    return graph
