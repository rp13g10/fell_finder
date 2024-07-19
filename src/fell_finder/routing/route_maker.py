"""Primary class which handles the creation of circular routes according
to the user provided configuration."""

from typing import List, Tuple, Dict, Generator, Union

import tqdm

from fell_finder.containers.routes import Route, RouteConfig
from fell_finder.selection.selector import Selector
from fell_finder.routing.zimmer import Zimmer
from fell_finder.routing.route_selector import FZRouteSelector, PyRouteSelector

# TODO: Make this more configurable


class RouteMaker:
    """Main route finder class for the application, given a graph containing
    enriched OSM/Defra data create a circular route based on a starting
    point & a max distance.
    """

    def __init__(self, config: RouteConfig, data_dir: str):
        """Create a route finder based on user preferences.

        Args:
            config (RouteConfig): The user-generated route configuration
        """

        self.config = config

        # Fetch networkx graph from enriched parquet dataset
        selector = Selector(self.config, data_dir)
        start_node, graph = selector.create_graph()
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
            node_id (int): The ID of the node to fetch lat/lon for
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
            Route: A candidate route of zero distance, starting at the node
              closest to the specified start point.
        """

        seed = [
            Route(
                route_id="seed",
                current_position=start_node,
                route=[start_node],
                visited={start_node},
            )
        ]

        return seed

    # Route Finding ###########################################################

    def _remove_start_node_from_visited(self, route: Route) -> Route:
        """In order to be able to complete a circular route, the starting node
        must be removed from the list of visited nodes. This must be done once
        the algorithm is on its second iteration, at which point there will
        be an intermediate node which prevents an immediate return to the
        starting point.

        Args:
            route (Route): A candidate route

        Returns:
            Route: A copy of the input route, with the first visited
              node removed from the 'visited' key
        """

        start_pos = route.route[0]
        route.visited.remove(start_pos)
        return route

    def _update_progress(self):
        """Update the progress bar with relevant metrics which enable the
        user to track the calculation as it progresses
        """

        n_candidates = len(self.candidates)
        n_valid = len(self.completed_routes)

        if n_candidates:
            iter_dist = sum(route.distance for route in self.candidates)
            iter_dist += sum(route.distance for route in self.completed_routes)
            avg_distance = iter_dist / (n_candidates + n_valid)
        else:
            avg_distance = self.config.max_distance

        progress_dict = {}
        progress_dict["n_candidates"] = n_candidates
        progress_dict["n_valid"] = n_valid
        progress_dict["avg_distance"] = avg_distance
        progress_dict["max_distance"] = self.config.max_distance

        print("Progress Dict:")
        print(progress_dict)

        return progress_dict

    def _generate_route_id(self, cand_inx: int, step_inx: int) -> str:
        """Generate a unique identifier for a route"""
        return f"{cand_inx}_{step_inx}"

    def find_routes(
        self,
    ) -> Generator[Tuple[Dict, Union[List[Route], None]], None, None]:
        """Main user-facing function for this class. Generates a list of
        circular routes according to the user's preferences.

        Returns:
            List[Route]: A list of completed routes
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
                    new_id = self._generate_route_id(cand_inx, step_inx)
                    candidate_status, new_candidate = (
                        self.zimmer.step_to_next_node(
                            candidate, possible_step, new_id
                        )
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
                    routes=sorted(
                        new_candidates,
                        key=lambda x: x.ratio,
                        reverse=self.config.route_mode == "hilly",
                    ),
                    n_routes=self.config.max_candidates // 2,
                    threshold=0.95,
                )

                new_candidates = selector.select_routes()

            self.last_candidates = self.candidates
            self.candidates = new_candidates

            # Update the progress bar
            iters += 1
            progress = self._update_progress()

            yield progress, None

        if self.completed_routes:
            self.completed_routes = sorted(
                self.completed_routes,
                key=lambda x: x.ratio,
                reverse=self.config.route_mode == "hilly",
            )

            yield progress, self.completed_routes
            return

        yield progress, []
        return
