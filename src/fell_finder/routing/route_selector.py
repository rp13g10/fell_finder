"""Once a list of valid routes has been generated, the RouteSelector can be
used to remove near-duplicate routes from it."""

# from thefuzz import fuzz
from abc import ABC, abstractmethod
from rapidfuzz import process
from typing import List
from fell_finder.containers.routes import Route


class BaseRouteSelector(ABC):
    """Base class containing methods useful for all child route selectors"""

    def __init__(self, routes: List[Route], threshold: float, n_routes: int):
        """Base class containing methods useful for all child route selectors"""
        self.routes = routes
        self.threshold = threshold
        self.n_routes = n_routes

    def get_routes_to_check(
        self, route: Route, to_check: List[Route], threshold: float
    ):
        """Fetch a subset of the provided list of routes which contains only
        those with a total distance within the configured similarity
        threshold of the primary route. This is assumed to be less
        computationally expensive than comparing them all individually.

        Args:
            route: The primary route, any routes with a similar length to this
              will be returned. Others will be discarded.
            to_check: A list of routes which may or may not be similar to the
              primary route.
            threshold: The maximum similarity between two routes, must be
              a value between 0 and 1.

        Returns:
            A filtered copy of the provided list of routes
        """
        current_distance = route.distance
        min_distance = current_distance * threshold

        to_check = [
            route_ for route_ in to_check if route.distance > min_distance
        ]

        return to_check

    @abstractmethod
    def _get_dissimilar_routes(
        self, routes: List[Route], threshold: float, n_routes: int
    ):
        """Each implementation must contain a method which removes similar
        routes from a list"""

    def select_routes(self):
        """Based on the configured similarity threshold, bring through a
        list of routes which meet the provided criteria but with routes which
        are very nearly identical removed."""
        num_selected = 0
        current_threshold = self.threshold

        while num_selected < self.n_routes and current_threshold < 0.99:
            selected_routes = self._get_dissimilar_routes(
                self.routes, current_threshold, self.n_routes
            )
            current_threshold = min(current_threshold + 0.01, 0.99)

        return selected_routes


class PyRouteSelector(BaseRouteSelector):
    """Takes the top N routes from a pre-sorted list of candidates, ensuring
    that each route is sufficiently different to all of the routes which
    preceeded it. Pure python implementation, uses sets to calculate degree
    of overlap between routes."""

    # NOTE: Speed seemed similar, very quickly ran into a dead end

    def __init__(
        self,
        routes: List[Route],
        n_routes: int,
        threshold: float,
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
            depth (int): How many prior selectors have been created with
              a reduced similarity threshold. Used as a safety net to prevent
              recursion erros.
        """
        super().__init__(routes, threshold, n_routes)

    @staticmethod
    def get_similarity(route_1: Route, route_2: Route):
        union = len(route_1.visited.union(route_2.visited))
        intersection = len(route_1.visited.intersection(route_2.visited))
        ratio = intersection / union

        return ratio

    def _get_dissimilar_routes(
        self, routes: List[Route], threshold: float, n_routes: int
    ) -> List[Route]:
        to_process = routes[:]
        selected_routes = []

        while to_process and len(selected_routes) < n_routes:
            route = to_process.pop(0)
            selected_routes.append(route)
            maybe_similar_routes = self.get_routes_to_check(
                route, to_process, threshold
            )
            for candidate in maybe_similar_routes:
                similarity = self.get_similarity(route, candidate)
                if similarity > threshold:
                    to_process.remove(candidate)

        return selected_routes


class FZRouteSelector(BaseRouteSelector):
    """Takes the top N routes from a pre-sorted list of candidates, ensuring
    that each route is sufficiently different to all of the routes which
    preceeded it. RapidFuzz implementation, processes the route as a string
    in order to select sufficiently different routes."""

    # NOTE: 5:25, route couldn't complete as it needed to step over 2
    #       consecutive nodes

    def __init__(self, routes: List[Route], n_routes: int, threshold: float):
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
        super().__init__(routes, threshold, n_routes)

    def _get_dissimilar_routes(
        self, routes: List[Route], threshold: float, n_routes: int
    ) -> List[Route]:
        score_cutoff = threshold * 100

        def _get_route_str(route: Route):
            route_list = [str(x) for x in route.route]
            route_str = " ".join(route_list)
            return route_str

        to_process = routes[:]
        selected_routes = []

        while to_process and len(selected_routes) < n_routes:
            route = to_process.pop(0)
            selected_routes.append(route)
            maybe_similar_routes = self.get_routes_to_check(
                route, to_process, threshold
            )
            similar_routes = process.extract(
                query=route,
                choices=maybe_similar_routes,
                processor=_get_route_str,
                score_cutoff=score_cutoff,
            )
            for similar_route, _, _ in similar_routes:
                to_process.remove(similar_route)

        return selected_routes
