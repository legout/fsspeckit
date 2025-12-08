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
        expected = "/path%20with%20spaces/file-name.txt"
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
        responses = [
            # First page
            Mock(
                status_code=200,
                json.return_value=[
                    {"name": "file1.txt", "type": "blob"},
                    {"name": "file2.txt", "type": "blob"}
                ],
                headers={"X-Next-Page": "2"}
            ),
            # Second page
            Mock(
                status_code=200,
                json.return_value=[
                    {"name": "file3.txt", "type": "blob"}
                ],
                headers={}  # No next page
            )
        ]

        with patch.object(fs._session, 'get', side_effect=responses):
            result = fs.ls("/")

            # Should collect all files from both pages
            assert result == ["file1.txt", "file2.txt", "file3.txt"]
            assert mock_get.call_count == 2

    def test_gitlab_ls_pagination_failure_recovery(self):
        """Test ls method handles pagination failures gracefully."""
        from fsspeckit.core.filesystem import GitLabFileSystem

        fs = GitLabFileSystem(project_id="12345")

        # Mock responses: first page succeeds, second page fails
        responses = [
            # First page succeeds
            Mock(
                status_code=200,
                json.return_value=[
                    {"name": "file1.txt", "type": "blob"}
                ],
                headers={"X-Next-Page": "2"}
            ),
            # Second page fails
            requests.RequestException("Network error")
        ]

        with patch.object(fs._session, 'get', side_effect=responses):
            with patch('fsspeckit.core.filesystem.logger') as mock_logger:
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