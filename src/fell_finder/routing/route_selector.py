"""Once a list of valid routes has been generated, the RouteSelector can be
used to remove near-duplicate routes from it."""

# from thefuzz import fuzz
from abc import ABC, abstractmethod
from typing import List, Literal
from fell_finder.routing.containers import Route


class BaseRouteSelector(ABC):
    """Base class containing methods useful for all child route selectors"""

    def __init__(
        self,
        routes: List[Route],
        threshold: float,
        n_routes: int,
        sort_attr: str,
        sort_order: Literal["asc", "desc"],
    ) -> None:
        """Base class containing methods useful for all child route
        selectors"""

        self.threshold = threshold
        self.n_routes = n_routes
        self.sort_attr = sort_attr
        self.sort_order = sort_order

        routes = sorted(
            routes,
            key=lambda x: getattr(x, sort_attr),
            reverse=self.sort_order == "desc",
        )

        self.routes = routes

    @abstractmethod
    def _get_dissimilar_routes(
        self, routes: List[Route], threshold: float, n_routes: int
    ) -> List[Route]:
        """Each implementation must contain a method which removes similar
        routes from a list"""

    def select_routes(self) -> List[Route]:
        """Based on the configured similarity threshold, bring through a
        list of routes which meet the provided criteria but with routes which
        are very nearly identical removed.

        Returns:
            A list of dissimilar routes
        """
        num_selected = 0
        current_threshold = self.threshold

        while num_selected < self.n_routes and current_threshold < 0.99:
            selected_routes = self._get_dissimilar_routes(
                self.routes, current_threshold, self.n_routes
            )
            num_selected = len(selected_routes)
            current_threshold = min(current_threshold + 0.01, 0.99)

        selected_routes = sorted(
            selected_routes,
            key=lambda x: getattr(x, self.sort_attr),
            reverse=self.sort_order == "desc",
        )

        return selected_routes


class PyRouteSelector(BaseRouteSelector):
    """Takes the top N routes from a pre-sorted list of candidates, ensuring
    that each route is sufficiently different to all of the routes which
    preceeded it. Pure python implementation, uses sets to calculate degree
    of overlap between routes."""

    def __init__(
        self,
        routes: List[Route],
        n_routes: int,
        threshold: float,
        sort_attr: str,
        sort_order: Literal["asc", "desc"],
    ) -> None:
        """Create a route selector with the provided parameters

        Args:
            routes: A list of valid route, sorted according to their desired
              elevation profile
            n_routes: How many distinct routes should be pulled from the
              provided list
            threshold: How similar can each route be to the next. Set to 0 to
              allow absolutely no overlap, set to 1 to allow even completely
              identical routes.
            depth: How many prior selectors have been created with a reduced
              similarity threshold. Used as a safety net to prevent
              recursion erros.
            sort_attr: The route attribute to sort by
            sort_order: The order to sort routes in
        """
        super().__init__(routes, threshold, n_routes, sort_attr, sort_order)

    @staticmethod
    def get_similarity(route_1: Route, route_2: Route) -> float:
        """Determine the level of similarity between two routes based on the
        degree of crossover between the nodes in each

        Args:
            route_1: The first route to compare
            route_2: The second route to compare

        Returns:
            The similarity between the two routes, 0 being completely different
            and 1 being identical
        """

        union = len(route_1.visited.union(route_2.visited))
        intersection = len(route_1.visited.intersection(route_2.visited))
        ratio = intersection / union

        return ratio

    def _get_dissimilar_routes(
        self, routes: List[Route], threshold: float, n_routes: int
    ) -> List[Route]:
        """Retain only sufficiently different routes, with the maximum level
        of similarity defined by the provided threshold

        Args:
            routes: A list of routes which may be similar
            threshold: The maximum similarity between two routes, must be a
              number between 0 and 1
            n_routes: The maximum number of routes to retain

        Returns:
            A list of dissimilar routes
        """
        to_process = routes[:]
        selected_routes = []

        while to_process and len(selected_routes) < n_routes:
            route = to_process.pop(0)

            current_distance = route.distance
            check_threshold = current_distance * threshold

            selected_routes.append(route)

            for candidate in to_process[:]:
                if candidate.distance > check_threshold:
                    similarity = self.get_similarity(route, candidate)
                    if similarity > threshold:
                        to_process.remove(candidate)

        return selected_routes
