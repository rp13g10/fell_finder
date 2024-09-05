"""Primary class which handles the creation of circular routes according
to the user provided configuration."""

from typing import List, Tuple, Dict, Generator, Union, Any

from fell_finder.containers.routes import Route
from fell_finder.containers.config import RouteConfig
from fell_finder.retrieval.graph_fetcher import GraphFetcher
from fell_finder.routing.zimmer import Zimmer
from fell_finder.routing.route_selector import PyRouteSelector
from fell_finder import app_config


class RouteMaker:
    """Main route finder class for the application, given a graph containing
    enriched OSM/Defra data create a circular route based on a starting
    point & a max distance.
    """

    def __init__(self, config: RouteConfig, data_dir: str) -> None:
        """Create a route finder based on user preferences.

        Args:
            config: The user-generated route configuration
            data_dir: The path to the folder containing the optimised graph
              data

        """

        self.config = config

        # Fetch networkx graph from enriched parquet dataset
        fetcher = GraphFetcher(self.config, data_dir)
        start_node, graph = fetcher.create_graph()
        self.start_node = start_node
        self.graph = graph

        # Set up for route generation
        self.zimmer = Zimmer(self.graph, self.config)
        self.candidates = self._create_seed_route(self.start_node)
        self.completed_routes: List[Route] = []

        # Debugging
        self.last_candidates: List[Route] = []

    def fetch_node_coords(self, node_id: int) -> Tuple[int, int]:
        """Convenience function, retrieves the latitude and longitude for a
        single node in a graph.

        Args:
            node_id: The ID of the node to fetch lat/lon for
        """
        node = self.graph[node_id][1]
        lat = node.lat
        lon = node.lon
        return lat, lon

    # Route Seeding ###########################################################

    def _create_seed_route(self, start_node: int) -> List[Route]:
        """Based on the user provided start point, generate a seed route
        starting at the closest available node.

        Returns:
            A candidate route of zero distance, starting at the node closest to
            the specified start point.
        """

        seed = [Route(start_node=start_node)]

        return seed

    # Route Finding ###########################################################

    def _remove_start_node_from_visited(self, route: Route) -> Route:
        """In order to be able to complete a circular route, the starting node
        must be removed from the list of visited nodes. This must be done once
        the algorithm is on its second iteration, at which point there will
        be an intermediate node which prevents an immediate return to the
        starting point.

        Args:
            route: A candidate route

        Returns:
            A copy of the input route, with the first visited node removed from
            the 'visited' key
        """

        route.visited.remove(route.start_node)
        return route

    def _update_progress(self) -> Dict[str, Any]:
        """Update the progress bar with relevant metrics which enable the
        user to track the calculation as it progresses
        """

        n_candidates = len(self.candidates)
        n_valid = len(self.completed_routes)

        if n_candidates:
            iter_dist = sum(
                route.metrics.distance for route in self.candidates
            )
            iter_dist += sum(
                route.metrics.distance for route in self.completed_routes
            )
            avg_distance = iter_dist / (n_candidates + n_valid)
        else:
            avg_distance = self.config.max_distance

        progress_dict = {}
        progress_dict["n_candidates"] = n_candidates
        progress_dict["n_valid"] = n_valid
        progress_dict["avg_distance"] = avg_distance
        progress_dict["max_distance"] = self.config.max_distance

        return progress_dict

    def find_routes(
        self,
    ) -> Generator[Tuple[Dict, Union[List[Route], None]], None, None]:
        """Main user-facing function for this class. Generates a list of
        circular routes according to the user's preferences.

        Returns:
            A list of completed routes
        """

        # Recursively check for circular routes

        iters = 0
        while self.candidates:
            # For each potential candidate route
            new_candidates = []
            for cand_inx, candidate in enumerate(self.candidates):
                # Check which nodes can be reached
                possible_steps = self.zimmer.generate_possible_steps(candidate)

                # For each node which can be reached
                for step_inx, possible_step in enumerate(possible_steps):
                    # Step to the next node and validate the resulting route
                    candidate_status, new_candidate = (
                        self.zimmer.step_to_next_node(candidate, possible_step)
                    )

                    # Make sure the route can get back to the starting node
                    if iters == 2:
                        new_candidate = self._remove_start_node_from_visited(
                            new_candidate
                        )

                    # Check whether the route is still valid
                    if candidate_status == "complete":
                        self.completed_routes.append(new_candidate)
                    elif candidate_status == "valid":
                        new_candidates.append(new_candidate)

            # Make sure the total number of routes stays below the configured
            # limit
            if len(new_candidates) > self.config.max_candidates:
                selector = PyRouteSelector(
                    routes=new_candidates,
                    n_routes=self.config.max_candidates
                    // app_config["routing"]["pruning_level"],
                    sort_attr="ratio",
                    sort_order="desc"
                    if self.config.route_mode == "hilly"
                    else "asc",
                )

                new_candidates = selector.select_routes_with_binning()

            self.last_candidates = self.candidates
            self.candidates = new_candidates

            # Update the progress bar
            iters += 1
            progress = self._update_progress()

            yield progress, None

        if self.completed_routes:
            selector = PyRouteSelector(
                routes=self.completed_routes,
                n_routes=app_config["webapp"]["routes_to_display"],
                sort_attr="ratio",
                sort_order="desc"
                if self.config.route_mode == "hilly"
                else "asc",
            )

            final_routes = selector.select_routes_with_binning()
            [route.finalize() for route in final_routes]

            yield progress, final_routes
            return

        yield progress, []
        return
