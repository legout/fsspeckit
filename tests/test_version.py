"""Test version handling in development environments."""

import sys
import importlib.metadata
import pytest
from unittest.mock import patch


def test_version_fallback():
    """Test that version falls back gracefully when package is not installed."""
    with patch("importlib.metadata.version") as mock_version:
        mock_version.side_effect = importlib.metadata.PackageNotFoundError()

        # Remove fsspeckit from modules if it exists
        modules_to_remove = [
            mod for mod in sys.modules.keys() if mod.startswith("fsspeckit")
        ]
        for mod in modules_to_remove:
            del sys.modules[mod]

        # Add src to path and import
        if "src" not in sys.path:
            sys.path.insert(0, "src")

        import fsspeckit

        assert fsspeckit.__version__ == "0.5.0-dev"


def test_version_with_exception():
    """Test that version handles other exceptions gracefully."""
    with patch("importlib.metadata.version") as mock_version:
        mock_version.side_effect = Exception("Some other error")

        # Remove fsspeckit from modules if it exists
        modules_to_remove = [
            mod for mod in sys.modules.keys() if mod.startswith("fsspeckit")
        ]
        for mod in modules_to_remove:
            del sys.modules[mod]

        # Add src to path and import
        if "src" not in sys.path:
            sys.path.insert(0, "src")

        import fsspeckit

        assert fsspeckit.__version__ == "0.5.0-dev"
