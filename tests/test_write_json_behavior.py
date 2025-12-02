"""Test write_json behavior with and without orjson installed."""

import tempfile
import json
from unittest.mock import patch
import pytest

from test_core_io_helpers import MockFileSystem


class TestWriteJsonBehavior:
    """Test write_json with optional orjson dependency."""

    def test_write_json_with_orjson_available(self, tmp_path):
        """Test write_json works when orjson is available."""
        from fsspeckit.core.ext import write_json

        # Create test data
        test_data = {"id": 1, "value": "test"}
        file_path = tmp_path / "test.json"

        # Mock filesystem
        fs = MockFileSystem()
        fs.files = {str(file_path): None}

        # This should work without error when orjson is available
        write_json(fs, test_data, file_path)

        # Verify file was written (mock filesystem would capture this)
        assert str(file_path) in fs.files_written

    def test_write_json_without_orjson_available(self, tmp_path):
        """Test write_json raises clear ImportError when orjson is not available."""
        from fsspeckit.core.ext import write_json

        # Create test data
        test_data = {"id": 1, "value": "test"}
        file_path = tmp_path / "test.json"

        # Mock filesystem
        fs = MockFileSystem()

        # Mock orjson not available
        with patch("fsspeckit.common.optional._import_orjson") as mock_import:
            mock_import.side_effect = ImportError(
                "orjson is required for this function. "
                "Install with: pip install fsspeckit[sql]"
            )

            # This should raise ImportError with clear message
            with pytest.raises(ImportError, match="orjson is required"):
                write_json(fs, test_data, file_path)
