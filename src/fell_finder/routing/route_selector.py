"""Once a list of valid routes has been generated, the RouteSelector can be
used to remove near-duplicate routes from it."""

# from thefuzz import fuzz
from typing import List, Union, Optional
from fell_finder.containers.routes import Route


class RouteSelector:
    """Takes the top N routes from a pre-sorted list of candidates, ensuring
    that each route is sufficiently different to all of the routes which
    preceeded it."""

    def __init__(
        self,
        routes: List[Route],
        num_routes_to_select: int,
        threshold: float,
        depth: int = 0,
    ):
        """Create a route selector with the provided parameters

        Args:
            routes (List[Route]): A list of valid route, sorted according to
              their desired elevation profile
            num_routes_to_select (int): How many distinct routes should be
              pulled from the provided list
            threshold (float): How similar can each route be to the next.
              Set to 0 to allow absolutely no overlap, set to 1 to allow
              even completely identical routes.
        """
        self.routes = routes
        self.n_routes = num_routes_to_select
        self.threshold = threshold
        self.depth = depth
        # print(
        #     f"Created a new selector with depth {depth} and threshold {threshold}"
        # )

        self.selected_routes: List[Route] = []

    @staticmethod
    def get_similarity(route_1: Route, route_2: Route):
        union = len(route_1.visited.union(route_2.visited))
        intersection = len(route_1.visited.intersection(route_2.visited))
        ratio = intersection / union

        return ratio

    def select_routes(self):
        for route in self.routes:
            is_selectable = True

            for selected_route in self.selected_routes:
                route_diff = self.get_similarity(route, selected_route)
                if route_diff > self.threshold:
                    is_selectable = False

            if is_selectable:
                if len(self.selected_routes) < self.n_routes:
                    self.selected_routes.append(route)
                if len(self.selected_routes) == self.n_routes:
                    return self.selected_routes

        if (
            len(self.selected_routes) < (self.n_routes * 0.25)
            and self.depth < 10
            and self.threshold < 0.99
        ):
            # print(f"Number of selected routes: {len(self.selected_routes)}")
            new_selector = RouteSelector(
                self.routes,
                self.n_routes,
                min(self.threshold + 0.01, 0.99),
                self.depth + 1,
            )
            new_selector.select_routes()
            self.selected_routes = new_selector.selected_routes

        # print(f"Final number of selected routes: {len(self.selected_routes)}")
        return self.selected_routes
