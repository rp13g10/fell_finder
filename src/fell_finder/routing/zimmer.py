"""This class calculates the impact of taking a step across the graph."""

from typing import Iterable, Tuple, Literal, Union

from rustworkx import PyDiGraph, dijkstra_shortest_path_lengths
from fell_finder.containers.routes import Route, StepData
from fell_finder.containers.config import RouteConfig
from fell_finder.containers.graph_data import GraphEdge


class Zimmer:
    """Class which handles stepping from one route to the next. Functions are
    provided which will generate a list of possible nodes to step to, and
    applying those steps to a given route."""

    def __init__(self, graph: PyDiGraph, config: RouteConfig) -> None:
        """Store down the required information to handle processing of routes

        Args:
            graph: The network graph representing the geographical
              area in which routes are being generated
            config: The user-provided configuration for the
              routes to be created
        """

        self.graph = graph
        self.config = config

    def _get_distance_to_start(
        self, graph: PyDiGraph, route: Route, node_id: int
    ) -> Union[float, None]:
        try:
            dist_to_start = dijkstra_shortest_path_lengths(
                graph,
                node_id,
                goal=route.start_node,
                edge_cost_fn=lambda attrs: attrs.distance,
            )[route.start_node]
        except IndexError:
            # TODO: Need to verify that this is actually what happens when no
            #       path exists. Can't find any other reasonable explanation.
            return None

        return dist_to_start

    def _validate_step(
        self, graph: PyDiGraph, route: Route, node_id: int
    ) -> bool:
        """For a given route and potential next step, verify that the step
        does not result in us visiting a node we've already been to. This
        requirement is waived when revisiting one of the first 3 nodes in
        the route, to make it easier to get back to the start point.

        Args:
            graph: The graph being used for route creation
            route: An (incomplete) candidate route
            node_id: The ID for the node to be stepped to

        Returns:
            Whether or not the provided node_id would be a valid step to take
        """

        dist_to_start = self._get_distance_to_start(graph, route, node_id)

        if dist_to_start is None:
            return False

        dist_remaining = self.config.max_distance - route.metrics.distance
        if dist_to_start > dist_remaining:
            return False

        visited = route.visited

        # We can always step to new nodes
        if node_id not in visited:
            return True

        # Allow steps back to the first few nodes, but prevent backtracking
        if (
            node_id in route.first_n_nodes
            and node_id not in route.last_3_nodes
        ):
            return True

        return False

    def generate_possible_steps(self, route: Route) -> Iterable[int]:
        """For a given route, determine which Node IDs are reachable without
        breaching the conditions of the route finding algorithm.

        Args:
            route: An incomplete route

        Returns:
            An iterator containing all of the IDs which can be stepped to from
            the current position of the provided route
        """

        valid_graph = self.graph.copy()
        if len(route.visited) > route.overlap_n_nodes:
            # Remove all visited nodes from the graph which are not allowed to
            # overlap
            to_remove = route.visited.difference(route.first_n_nodes)
            try:
                # Ensure the current node stays in the graph
                to_remove.remove(route.cur_node)
            except KeyError:
                pass
            to_remove = list(to_remove)
            valid_graph.remove_nodes_from(to_remove)

        # Get all nodes which we can potentially step to
        neighbours = filter(
            lambda node: self._validate_step(valid_graph, route, node),
            valid_graph.neighbors(route.cur_node),
        )
        neighbours = list(neighbours)

        return neighbours

    def _fetch_step_metrics(self, route: Route, next_node: int) -> StepData:
        """For a candidate route, calculate the change in distance & elevation
        when moving from the end point to the specified neighbour. Record any
        intermediate nodes which are traversed when making this journey.

        Args:
            route: A candidate route
            next_node: The ID of a neighbouring node

        Returns:
            The calculated metrics for this step
        """

        step: GraphEdge = self.graph.get_edge_data(route.cur_node, next_node)

        step_metrics = StepData(
            next_node=next_node,
            distance=step.distance,
            elevation_gain=step.elevation_gain,
            elevation_loss=step.elevation_loss,
            surface=step.surface,
            lats=step.geometry["lat"],
            lons=step.geometry["lon"],
            distances=step.geometry["distance"],
            elevations=step.geometry["elevation"],
        )

        return step_metrics

    def _check_if_route_is_complete(self, route: Route) -> bool:
        # Route is a closed loop
        if route.is_closed_loop:
            # Route is of correct distance
            if (
                (self.config.min_distance)
                <= route.metrics.distance
                <= (self.config.max_distance)
            ):
                return True

        return False

    def _check_if_route_meets_surface_requirements(self, route: Route) -> bool:
        surfaces = self.config.restricted_surfaces
        perc = self.config.restricted_surfaces_perc

        if not (surfaces and perc):
            return True

        # Set max allowable distance on this surface type
        surface_type_max_dist = perc * self.config.max_distance

        # Calculate total distance on this surface type
        surface_type_dist = 0.0
        for surface in surfaces:
            surface_type_dist += route.metrics.surface_distances[surface]

        # Check if criteria is satisfied
        if surface_type_dist > surface_type_max_dist:
            return False

        # If all criteria satisfied, return True
        return True

    def _validate_route(
        self, route: Route
    ) -> Literal["complete", "valid", "invalid"]:
        """For a newly generated candidate route, validate that it is still
        within the required parameters. If not, then it should be discarded.

        Args:
            route: A candidate route

        Returns:
            str: The status of the route
        """

        # All routes
        if route.metrics.distance >= self.config.max_distance:
            return "invalid"
        # --> Only routes below max distance

        if route.is_closed_loop:
            is_complete = self._check_if_route_is_complete(route)
            if not is_complete:
                return "invalid"
        # --> Only open loops and completed routes

        meets_surface_requirements = (
            self._check_if_route_meets_surface_requirements(route)
        )
        if not meets_surface_requirements:
            return "invalid"
        # --> Only routes which meet the surface requirements

        if not route.is_closed_loop:
            return "valid"

        if (
            self.config.min_distance
            <= route.metrics.distance
            <= self.config.max_distance
        ):
            return "complete"

        return "valid"

    def step_to_next_node(
        self, route: Route, next_node: int
    ) -> Tuple[Literal["complete", "valid", "invalid"], Route]:
        """For a given route and node to step to, perform the step and update
        the route metrics, then validate that the route is still within the
        user-provided parameters.

        Args:
            route: An incomplete route
            next_node: The node to be stepped to
            new_id: The ID for the new route

        Returns:
            The status of the new route, and the new route itself
        """

        # Calculate the impact of stepping to the neighbour
        step_metrics = self._fetch_step_metrics(route, next_node)

        # Update the new candidate to reflect this step
        candidate = route.take_step(step_metrics)

        candidate_status = self._validate_route(candidate)

        return candidate_status, candidate
