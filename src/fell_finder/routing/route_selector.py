"""Once a list of valid routes has been generated, the RouteSelector can be
used to remove near-duplicate routes from it."""

import itertools
import math
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import List, Literal, Dict, Tuple
import numpy as np
from fell_finder import app_config
from fell_finder.containers.routes import Route


class BaseRouteSelector(ABC):
    """Base class containing methods useful for all child route selectors"""

    def __init__(
        self,
        routes: List[Route],
        n_routes: int,
        sort_attr: str,
        sort_order: Literal["asc", "desc"],
    ) -> None:
        """Base class containing methods useful for all child route
        selectors"""

        self.threshold = app_config["routing"]["initial_similarity_threshold"]
        self.max_threshold = app_config["routing"]["max_similarity_threshold"]
        self.threshold_step = app_config["routing"][
            "similarity_threshold_increment"
        ]
        self.n_routes = n_routes
        self.sort_attr = sort_attr
        self.sort_order = sort_order

        routes = sorted(
            routes,
            key=lambda x: getattr(x.metrics, sort_attr),
            reverse=self.sort_order == "desc",
        )

        self.routes = routes

    def increase_threshold(self) -> None:
        """Increase the internal similarity threshold by the configured amount,
        up to a configured maximum"""

        new_threshold = self.threshold + self.threshold_step
        new_threshold = min(new_threshold, self.max_threshold)

        self.threshold = new_threshold

    @abstractmethod
    def _get_dissimilar_routes(
        self, routes: List[Route], n_routes: int
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

        while (
            num_selected < self.n_routes
            and self.threshold < self.max_threshold
        ):
            selected_routes = self._get_dissimilar_routes(
                self.routes, self.n_routes
            )
            num_selected = len(selected_routes)
            self.increase_threshold()

        selected_routes = sorted(
            selected_routes,
            key=lambda x: getattr(x.metrics, self.sort_attr),
            reverse=self.sort_order == "desc",
        )

        return selected_routes

    def bin_routes(self) -> Tuple[Dict, Dict]:
        """Assign all of the provided routes to bins based on their central
        coordinates. Routes are binned across both lats and lons, analagous
        to placing them into a 2d grid.

        Returns:
            A dict mapping bins indices (lat_inx, lon_inx) to a list of the
            routes within it.
        """
        lats = []
        lons = []
        for route in self.routes:
            lat, lon = route.geometry.centre
            lats.append(lat)
            lons.append(lon)

        n_bins = app_config["routing"]["selection_bins"]
        lat_bins = np.linspace(min(lats), max(lats), n_bins)
        lon_bins = np.linspace(min(lons), max(lons), n_bins)

        lat_indices = np.digitize(lats, lat_bins)
        lon_indices = np.digitize(lons, lon_bins)

        bins = defaultdict(list)
        counts = defaultdict(lambda: 0)
        for lat_inx, lon_inx, route in zip(
            lat_indices, lon_indices, self.routes
        ):
            bins[(lat_inx, lon_inx)].append(route)
            counts[(lat_inx, lon_inx)] += 1

        return bins, counts

    def select_routes_with_binning(self) -> List[Route]:
        """Retrieve a limited subset of routes, with an initial binning step
        to ensure a good distribution of route shapes. This aims to avoid
        the trap of routes which find hills earlier on from being selected
        over those which find them later in the process.

        Returns:
            A list of selected routes
        """
        num_selected = 0

        binned_routes, bin_counts = self.bin_routes()
        routes_per_bin = math.ceil(self.n_routes / len(binned_routes))

        while (
            num_selected < self.n_routes
            and self.threshold < self.max_threshold
        ):
            shortfall = 0
            bin_selections = []
            for bin_routes in binned_routes.values():
                # If prior bins have been smaller than the target, attempt to
                # make up the difference from larger bins
                bin_target = routes_per_bin + shortfall
                bin_selection = self._get_dissimilar_routes(
                    bin_routes, routes_per_bin
                )

                # Adjust shortfall
                bin_selected = len(bin_selection)
                if bin_selected < bin_target:
                    shortfall += bin_target - bin_selected
                if bin_selected > routes_per_bin:
                    shortfall -= bin_selected - routes_per_bin

                # Store selected routes
                bin_selections.append(bin_selection)

            selected_routes = list(
                itertools.chain.from_iterable(bin_selections)
            )

            num_selected = len(selected_routes)
            self.increase_threshold()

        selected_routes = sorted(
            selected_routes,
            key=lambda x: getattr(x.metrics, self.sort_attr),
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
        super().__init__(routes, n_routes, sort_attr, sort_order)

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
        self, routes: List[Route], n_routes: int
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

        # For smaller bins we may not need to do any filtering
        if len(routes) <= n_routes:
            return routes

        while to_process and len(selected_routes) < n_routes:
            route = to_process.pop(0)

            current_distance = route.metrics.distance
            check_threshold = current_distance * self.threshold

            selected_routes.append(route)

            for candidate in to_process[:]:
                if candidate.metrics.distance > check_threshold:
                    similarity = self.get_similarity(route, candidate)
                    if similarity > self.threshold:
                        to_process.remove(candidate)

        return selected_routes
