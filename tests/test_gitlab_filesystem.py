"""Tests for GitLab filesystem hardening and functionality."""

import urllib.parse
from unittest.mock import Mock, patch

import pytest
import requests


class TestGitLabFileSystem:
    """Test GitLab filesystem implementation."""

    def test_gitlab_filesystem_initialization(self):
        """Test GitLab filesystem initialization with new timeout parameter."""
        from fsspeckit.core.filesystem import GitLabFileSystem

        # Test with project_id
        fs = GitLabFileSystem(
            project_id="12345",
            timeout=60.0
        )
        assert fs.project_id == "12345"
        assert fs.timeout == 60.0
        assert hasattr(fs, "_session")

        # Test with project_name
        fs_name = GitLabFileSystem(
            project_name="group/project",
            timeout=45.0
        )
        assert fs_name.project_name == "group/project"
        assert fs_name.timeout == 45.0

        # Test default timeout
        fs_default = GitLabFileSystem(project_id="12345")
        assert fs_default.timeout == 30.0

    def test_gitlab_project_identifier_url_encoding(self):
        """Test that project identifiers are URL-encoded correctly."""
        from fsspeckit.core.filesystem import GitLabFileSystem

        # Test with special characters in project name
        fs = GitLabFileSystem(project_name="group with spaces/project-name")
        identifier = fs._get_project_identifier()
        
        # Should be URL-encoded
        expected = urllib.parse.quote("group with spaces/project-name", safe="")
        assert identifier == expected

        # Test with project ID (should not be encoded)
        fs_id = GitLabFileSystem(project_id="12345")
        identifier_id = fs_id._get_project_identifier()
        assert identifier_id == "12345"

    def test_gitlab_file_path_url_encoding(self):
        """Test that file paths are URL-encoded correctly."""
        from fsspeckit.core.filesystem import GitLabFileSystem

        fs = GitLabFileSystem(project_id="12345")

        # Test simple path
        simple_path = fs._get_file_path("file.txt")
        assert simple_path == "/file.txt"

        # Test path with spaces and special characters
        complex_path = fs._get_file_path("path with spaces/file-name.txt")
        # Forward slash should be encoded as %2F
        expected = "/path%20with%20spaces%2Ffile-name.txt"
        assert complex_path == expected

    def test_gitlab_make_request_with_timeout(self):
        """Test that requests use the configured timeout."""
        from fsspeckit.core.filesystem import GitLabFileSystem

        fs = GitLabFileSystem(project_id="12345", timeout=15.0)

        # Mock the session.get method
        with patch.object(fs._session, 'get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = []
            mock_get.return_value = mock_response

            # Make a request
            fs._make_request("test")

            # Verify timeout was passed
            mock_get.assert_called_once()
            args, kwargs = mock_get.call_args
            assert kwargs['timeout'] == 15.0

    def test_gitlab_make_request_error_logging(self):
        """Test that HTTP errors are logged with context."""
        from fsspeckit.core.filesystem import GitLabFileSystem

        fs = GitLabFileSystem(project_id="12345")

        # Mock a failed response
        with patch.object(fs._session, 'get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 404
            mock_response.reason = "Not Found"
            mock_response.text = "Project not found"
            
            error = requests.HTTPError("404 Not Found")
            error.response = mock_response
            
            mock_get.side_effect = error

            # Should raise the error with logging
            with pytest.raises(requests.HTTPError):
                fs._make_request("test")

    def test_gitlab_ls_pagination_single_page(self):
        """Test ls method with single page response."""
        from fsspeckit.core.filesystem import GitLabFileSystem

        fs = GitLabFileSystem(project_id="12345")

        # Mock response with single page
        with patch.object(fs._session, 'get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = [
                {"name": "file1.txt", "type": "blob"},
                {"name": "file2.txt", "type": "blob"}
            ]
            mock_response.headers = {}  # No X-Next-Page header
            mock_get.return_value = mock_response

            result = fs.ls("/")

            assert result == ["file1.txt", "file2.txt"]
            assert mock_get.call_count == 1

    def test_gitlab_ls_pagination_multiple_pages(self):
        """Test ls method with multiple page responses."""
        from fsspeckit.core.filesystem import GitLabFileSystem

        fs = GitLabFileSystem(project_id="12345")

        # Mock responses for multiple pages
        # First page
        mock_response1 = Mock()
        mock_response1.status_code = 200
        mock_response1.json.return_value = [
            {"name": "file1.txt", "type": "blob"},
            {"name": "file2.txt", "type": "blob"}
        ]
        mock_response1.headers = {"X-Next-Page": "2"}

        # Second page
        mock_response2 = Mock()
        mock_response2.status_code = 200
        mock_response2.json.return_value = [
            {"name": "file3.txt", "type": "blob"}
        ]
        mock_response2.headers = {}  # No next page

        responses = [mock_response1, mock_response2]

        with patch.object(fs._session, 'get', side_effect=responses) as mock_get:
            result = fs.ls("/")

            # Should collect all files from both pages
            assert result == ["file1.txt", "file2.txt", "file3.txt"]
            assert mock_get.call_count == 2

    def test_gitlab_ls_pagination_failure_recovery(self):
        """Test ls method handles pagination failures gracefully."""
        from fsspeckit.core.filesystem import GitLabFileSystem
        from fsspeckit.core.filesystem import gitlab

        fs = GitLabFileSystem(project_id="12345")

        # Mock responses: first page succeeds, second page fails
        # First page succeeds
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"name": "file1.txt", "type": "blob"}
        ]
        mock_response.headers = {"X-Next-Page": "2"}

        responses = [mock_response, requests.RequestException("Network error")]

        with patch.object(fs._session, 'get', side_effect=responses):
            with patch.object(gitlab, 'logger') as mock_logger:
                result = fs.ls("/")

                # Should return files from successful page
                assert result == ["file1.txt"]
                # Should log a warning
                mock_logger.warning.assert_called_once()

    def test_gitlab_exists_with_404(self):
        """Test exists method handles 404 errors correctly."""
        from fsspeckit.core.filesystem import GitLabFileSystem

        fs = GitLabFileSystem(project_id="12345")

        # Mock 404 response for info method
        with patch.object(fs._session, 'get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 404
            mock_response.reason = "Not Found"
            
            error = requests.HTTPError("404 Not Found")
            error.response = mock_response
            
            mock_get.side_effect = error

            # exists should return False for 404
            assert fs.exists("nonexistent.txt") is False

    def test_gitlab_exists_other_errors(self):
        """Test exists method re-raises non-404 HTTP errors."""
        from fsspeckit.core.filesystem import GitLabFileSystem

        fs = GitLabFileSystem(project_id="12345")

        # Mock 500 response
        with patch.object(fs._session, 'get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 500
            mock_response.reason = "Internal Server Error"
            
            error = requests.HTTPError("500 Internal Server Error")
            error.response = mock_response
            
            mock_get.side_effect = error

            # exists should re-raise non-404 errors
            with pytest.raises(requests.HTTPError):
                fs.exists("test.txt")

    def test_gitlab_cat_file_url_encoding(self):
        """Test that cat_file uses URL-encoded paths."""
        from fsspeckit.core.filesystem import GitLabFileSystem

        fs = GitLabFileSystem(project_id="12345")

        with patch.object(fs._session, 'get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "content": "dGVzdCBjb250ZW50"  # base64 for "test content"
            }
            mock_get.return_value = mock_response

            # Call with path containing special characters
            result = fs.cat_file("path with spaces/file.txt")

            # Verify the session.get was called with URL-encoded path
            mock_get.assert_called_once()
            call_args = mock_get.call_args
            assert "repository/files/path%20with%20spaces/file.txt" in call_args[0][0]

            # Verify content decoding
            assert result == b"test content"

    def test_gitlab_filesystem_session_cleanup_close(self):
        """Test that close() method properly cleans up the requests session."""
        from fsspeckit.core.filesystem import GitLabFileSystem

        fs = GitLabFileSystem(project_id="12345")

        # Mock the session.close method
        with patch.object(fs._session, 'close') as mock_close:
            # Call close
            fs.close()

            # Verify close was called
            mock_close.assert_called_once()
            # Verify closed state is set
            assert fs._closed is True

        # Call close again (should be idempotent)
        fs.close()
        # Verify close was still only called once
        assert mock_close.call_count == 1

    def test_gitlab_filesystem_multiple_instances(self):
        """Test that multiple filesystem instances don't share sessions."""
        from fsspeckit.core.filesystem import GitLabFileSystem

        fs1 = GitLabFileSystem(project_id="12345")
        fs2 = GitLabFileSystem(project_id="67890")

        # Each instance should have its own session
        assert fs1._session is not fs2._session

    def test_gitlab_ls_pagination_limit_default(self):
        """Test ls method enforces default pagination limit."""
        from fsspeckit.core.filesystem import GitLabFileSystem
        from fsspeckit.core.filesystem import gitlab

        fs = GitLabFileSystem(project_id="12345")
        # Default max_pages is 1000

        # Mock responses that would continue indefinitely without limit
        def create_mock_response(page_num):
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = [
                {"name": f"file{page_num}.txt", "type": "blob"}
            ]
            # Always return next page
            mock_response.headers = {"X-Next-Page": str(page_num + 1)}
            return mock_response

        with patch.object(fs._session, 'get', side_effect=[
            create_mock_response(i) for i in range(1, 1005)  # Exceeds default limit
        ]):
            with patch.object(gitlab, 'logger') as mock_logger:
                result = fs.ls("/")

                # Should stop at max_pages
                assert len(result) == 1000
                # Should log a warning about reaching the limit
                mock_logger.warning.assert_called_once_with(
                    "Reached maximum pages limit (%d), returning %d files",
                    1000,
                    1000
                )

    def test_gitlab_ls_pagination_limit_custom(self):
        """Test ls method enforces custom pagination limit."""
        from fsspeckit.core.filesystem import GitLabFileSystem
        from fsspeckit.core.filesystem import gitlab

        fs = GitLabFileSystem(project_id="12345", max_pages=5)

        # Mock responses
        responses = []
        for i in range(1, 10):
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = [{"name": f"file{i}.txt", "type": "blob"}]
            mock_response.headers = {"X-Next-Page": str(i + 1)}
            responses.append(mock_response)

        with patch.object(fs._session, 'get', side_effect=responses):
            with patch.object(gitlab, 'logger') as mock_logger:
                result = fs.ls("/")

                # Should stop at custom limit
                assert len(result) == 5
                mock_logger.warning.assert_called_once()

    def test_gitlab_ls_malformed_pagination_header(self):
        """Test ls method handles malformed X-Next-Page headers gracefully."""
        from fsspeckit.core.filesystem import GitLabFileSystem
        from fsspeckit.core.filesystem import gitlab

        fs = GitLabFileSystem(project_id="12345")

        # Mock response with malformed header
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"name": "file1.txt", "type": "blob"}
        ]
        mock_response.headers = {"X-Next-Page": "invalid"}  # Non-numeric header

        with patch.object(fs._session, 'get', return_value=mock_response):
            with patch.object(gitlab, 'logger') as mock_logger:
                result = fs.ls("/")

                # Should return the file and stop
                assert result == ["file1.txt"]
                # Should log a warning about malformed header
                mock_logger.warning.assert_called_once_with(
                    "Malformed X-Next-Page header: '%s', stopping pagination at page %d",
                    "invalid",
                    1
                )

    def test_gitlab_timeout_validation_valid(self):
        """Test that valid timeout values are accepted."""
        from fsspeckit.core.filesystem import GitLabFileSystem

        # Test various valid timeouts
        valid_timeouts = [0.01, 1.0, 30.0, 3600.0]
        for timeout in valid_timeouts:
            fs = GitLabFileSystem(project_id="12345", timeout=timeout)
            assert fs.timeout == timeout

    def test_gitlab_timeout_validation_negative(self):
        """Test that negative timeout values are rejected."""
        from fsspeckit.core.filesystem import GitLabFileSystem

        with pytest.raises(ValueError, match="timeout must be a positive number"):
            GitLabFileSystem(project_id="12345", timeout=-1.0)

    def test_gitlab_timeout_validation_zero(self):
        """Test that zero timeout value is rejected."""
        from fsspeckit.core.filesystem import GitLabFileSystem

        with pytest.raises(ValueError, match="timeout must be a positive number"):
            GitLabFileSystem(project_id="12345", timeout=0.0)

    def test_gitlab_timeout_validation_too_large(self):
        """Test that timeout values exceeding 3600 seconds are rejected."""
        from fsspeckit.core.filesystem import GitLabFileSystem

        with pytest.raises(ValueError, match="timeout must not exceed 3600 seconds"):
            GitLabFileSystem(project_id="12345", timeout=3601.0)

    def test_gitlab_max_pages_validation_valid(self):
        """Test that valid max_pages values are accepted."""
        from fsspeckit.core.filesystem import GitLabFileSystem

        # Test various valid max_pages
        valid_values = [1, 100, 1000, 10000]
        for max_pages in valid_values:
            fs = GitLabFileSystem(project_id="12345", max_pages=max_pages)
            assert fs.max_pages == max_pages

    def test_gitlab_max_pages_validation_negative(self):
        """Test that negative max_pages values are rejected."""
        from fsspeckit.core.filesystem import GitLabFileSystem

        with pytest.raises(ValueError, match="max_pages must be a positive integer"):
            GitLabFileSystem(project_id="12345", max_pages=-1)

    def test_gitlab_max_pages_validation_zero(self):
        """Test that zero max_pages value is rejected."""
        from fsspeckit.core.filesystem import GitLabFileSystem

        with pytest.raises(ValueError, match="max_pages must be a positive integer"):
            GitLabFileSystem(project_id="12345", max_pages=0)

    def test_gitlab_max_pages_validation_too_large(self):
        """Test that max_pages values exceeding 10000 are rejected."""
        from fsspeckit.core.filesystem import GitLabFileSystem

        with pytest.raises(ValueError, match="max_pages must not exceed 10000"):
            GitLabFileSystem(project_id="12345", max_pages=10001)

    def test_gitlab_ls_with_detail_true(self):
        """Test ls method with detail=True returns full file info."""
        from fsspeckit.core.filesystem import GitLabFileSystem

        fs = GitLabFileSystem(project_id="12345")

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"name": "file1.txt", "type": "blob", "size": 100},
            {"name": "file2.txt", "type": "blob", "size": 200}
        ]
        mock_response.headers = {}

        with patch.object(fs._session, 'get', return_value=mock_response):
            result = fs.ls("/", detail=True)

            # Should return full file info
            assert len(result) == 2
            assert result[0]["name"] == "file1.txt"
            assert result[0]["size"] == 100
            assert result[1]["name"] == "file2.txt"
            assert result[1]["size"] == 200

    def test_gitlab_ls_with_path_filter(self):
        """Test ls method respects path parameter."""
        from fsspeckit.core.filesystem import GitLabFileSystem

        fs = GitLabFileSystem(project_id="12345")

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"name": "file.txt", "type": "blob"}
        ]
        mock_response.headers = {}

        with patch.object(fs._session, 'get') as mock_get:
            mock_get.return_value = mock_response

            fs.ls("subdir")

            # Verify path parameter was passed
            call_args = mock_get.call_args
            assert call_args[1]['params']['path'] == "subdir"


class TestGitLabStorageOptions:
    """Test GitLabStorageOptions validation and functionality."""

    def test_gitlab_storage_options_initialization(self):
        """Test GitLabStorageOptions initialization with new parameters."""
        from fsspeckit.storage_options.git import GitLabStorageOptions

        options = GitLabStorageOptions(
            project_id="12345",
            timeout=60.0,
            max_pages=500
        )
        assert options.project_id == "12345"
        assert options.timeout == 60.0
        assert options.max_pages == 500

    def test_gitlab_storage_options_from_env(self):
        """Test GitLabStorageOptions.from_env with new parameters."""
        from fsspeckit.storage_options.git import GitLabStorageOptions
        import os

        # Set environment variables
        os.environ["GITLAB_PROJECT_ID"] = "12345"
        os.environ["GITLAB_TIMEOUT"] = "45.0"
        os.environ["GITLAB_MAX_PAGES"] = "500"

        try:
            options = GitLabStorageOptions.from_env()
            assert options.project_id == "12345"
            assert options.timeout == 45.0
            assert options.max_pages == 500
        finally:
            # Clean up
            del os.environ["GITLAB_PROJECT_ID"]
            del os.environ["GITLAB_TIMEOUT"]
            del os.environ["GITLAB_MAX_PAGES"]

    def test_gitlab_storage_options_to_env(self):
        """Test GitLabStorageOptions.to_env exports new parameters."""
        from fsspeckit.storage_options.git import GitLabStorageOptions
        import os

        options = GitLabStorageOptions(
            project_id="12345",
            timeout=30.0,
            max_pages=1000
        )

        options.to_env()

        assert os.getenv("GITLAB_PROJECT_ID") == "12345"
        assert os.getenv("GITLAB_TIMEOUT") == "30.0"
        assert os.getenv("GITLAB_MAX_PAGES") == "1000"

    def test_gitlab_storage_options_to_fsspec_kwargs(self):
        """Test GitLabStorageOptions.to_fsspec_kwargs includes new parameters."""
        from fsspeckit.storage_options.git import GitLabStorageOptions

        options = GitLabStorageOptions(
            project_id="12345",
            token="glpat_xxxx",
            timeout=60.0,
            max_pages=500
        )

        kwargs = options.to_fsspec_kwargs()

        assert kwargs["project_id"] == "12345"
        assert kwargs["token"] == "glpat_xxxx"
        assert kwargs["timeout"] == 60.0
        assert kwargs["max_pages"] == 500

    def test_gitlab_storage_options_validation_timeout_negative(self):
        """Test GitLabStorageOptions rejects negative timeout."""
        from fsspeckit.storage_options.git import GitLabStorageOptions

        with pytest.raises(ValueError, match="timeout must be a positive number"):
            GitLabStorageOptions(project_id="12345", timeout=-1.0)

    def test_gitlab_storage_options_validation_timeout_too_large(self):
        """Test GitLabStorageOptions rejects timeout > 3600."""
        from fsspeckit.storage_options.git import GitLabStorageOptions

        with pytest.raises(ValueError, match="timeout must not exceed 3600 seconds"):
            GitLabStorageOptions(project_id="12345", timeout=3601.0)

    def test_gitlab_storage_options_validation_max_pages_negative(self):
        """Test GitLabStorageOptions rejects negative max_pages."""
        from fsspeckit.storage_options.git import GitLabStorageOptions

        with pytest.raises(ValueError, match="max_pages must be a positive integer"):
            GitLabStorageOptions(project_id="12345", max_pages=-1)

    def test_gitlab_storage_options_validation_max_pages_too_large(self):
        """Test GitLabStorageOptions rejects max_pages > 10000."""
        from fsspeckit.storage_options.git import GitLabStorageOptions

        with pytest.raises(ValueError, match="max_pages must not exceed 10000"):
            GitLabStorageOptions(project_id="12345", max_pages=10001)