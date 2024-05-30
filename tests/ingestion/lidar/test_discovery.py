"""Unit tests for LIDAR file discovery tools"""

from unittest.mock import patch, MagicMock

import pytest

from fell_finder.ingestion.lidar.discovery import get_available_folders


class TestGetAvailableFolders:
    """Check behaviour is as-expected when searching for files"""

    @patch("fell_finder.ingestion.lidar.discovery.glob")
    def test_files_found(self, mock_glob: MagicMock):
        """Check expected output format when files are found"""
        # Arrange
        test_dir = "test/dir"
        mock_glob.return_value = ["item_one"]
        target_out = {"item_one"}
        target_call = "test/dir/extracts/lidar/lidar_composite_dtm-*"

        # Act
        result = get_available_folders(test_dir)

        # Assert
        assert result == target_out
        mock_glob.assert_called_once_with(target_call)

    @patch("fell_finder.ingestion.lidar.discovery.glob")
    def test_files_not_found(self, mock_glob):
        """Check exception is thrown when they aren't"""
        # Arrange
        test_dir = "dummy"
        mock_glob.return_value = {}

        # Act, Assert
        with pytest.raises(FileNotFoundError):
            _ = get_available_folders(test_dir)
