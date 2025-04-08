"""Make sure all dataclasses have been set up correctly"""

from typing import Tuple
from unittest.mock import MagicMock, patch

from fell_viewer.containers.geometry import BBox


@patch("fell_viewer.containers.geometry.get_partitions")
@patch("fell_viewer.containers.geometry.WGS84toOSGB36")
def test_bbox(mock_wgs84toosgb36: MagicMock, mock_get_partitions: MagicMock):
    """Make sure post initialization is working properly"""
    # Arrange
    test_min_lat = "min_lat"
    test_min_lon = "min_lon"
    test_max_lat = "max_lat"
    test_max_lon = "max_lon"

    mock_wgs84toosgb36.side_effect = [
        ("easting_1", "northing_1"),
        ("easting_2", "northing_2"),
    ]

    def side_effect(easting: str, northing: str) -> Tuple[str, str]:
        epn = easting.replace("easting", "easting_ptn")
        npn = northing.replace("northing", "northing_ptn")

        return epn, npn

    mock_get_partitions.side_effect = side_effect

    target_min_easting_ptn = "easting_ptn_1"
    target_min_northing_ptn = "northing_ptn_1"
    target_max_easting_ptn = "easting_ptn_2"
    target_max_northing_ptn = "northing_ptn_2"

    # Act
    result = BBox(test_min_lat, test_min_lon, test_max_lat, test_max_lon)  # type: ignore

    # Assert
    assert result.min_easting_ptn == target_min_easting_ptn
    assert result.min_northing_ptn == target_min_northing_ptn
    assert result.max_easting_ptn == target_max_easting_ptn
    assert result.max_northing_ptn == target_max_northing_ptn
