"""GitLab filesystem implementation for fsspec.

This module provides a filesystem interface for GitLab repositories including:
- Public and private repositories
- Self-hosted GitLab instances
- Branch/tag/commit selection
- Token-based authentication
"""

import urllib.parse
from typing import Any

import requests
from fsspec import AbstractFileSystem

from fsspeckit.common.logging import get_logger

logger = get_logger(__name__)


class GitLabFileSystem(AbstractFileSystem):
    """Filesystem interface for GitLab repositories.

    Provides read-only access to files in GitLab repositories, including:
    - Public and private repositories
    - Self-hosted GitLab instances
    - Branch/tag/commit selection
    - Token-based authentication

    Attributes:
        protocol (str): Always "gitlab"
        base_url (str): GitLab instance URL
        project_id (str): Project ID
        project_name (str): Project name/path
        ref (str): Git reference (branch, tag, commit)
        token (str): Access token
        api_version (str): API version

    Example:
        ```python
        # Public repository
        fs = GitLabFileSystem(
            project_name="group/project",
            ref="main",
        )
        files = fs.ls("/")

        # Private repository with token
        fs = GitLabFileSystem(
            project_id="12345",
            token="glpat_xxxx",
            ref="develop",
        )
        content = fs.cat("README.md")
        ```
    """

    protocol = "gitlab"

    def __init__(
        self,
        base_url: str = "https://gitlab.com",
        project_id: str | int | None = None,
        project_name: str | None = None,
        ref: str = "main",
        token: str | None = None,
        api_version: str = "v4",
        timeout: float = 30.0,
        max_pages: int = 1000,
        **kwargs: Any,
    ):
        """Initialize GitLab filesystem.

        Args:
            base_url: GitLab instance URL
            project_id: Project ID number
            project_name: Project name/path (alternative to project_id)
            ref: Git reference (branch, tag, or commit SHA)
            token: GitLab personal access token
            api_version: API version to use
            timeout: Request timeout in seconds (must be positive, max 3600)
            max_pages: Maximum number of pages to fetch (default 1000, max 10000)
            **kwargs: Additional arguments
        """
        super().__init__(**kwargs)

        self.base_url = base_url.rstrip("/")
        self.project_id = project_id
        self.project_name = project_name
        self.ref = ref
        self.token = token
        self.api_version = api_version

        # Input validation
        if timeout <= 0:
            raise ValueError("timeout must be a positive number")
        if timeout > 3600:
            raise ValueError("timeout must not exceed 3600 seconds")
        self.timeout = timeout

        if max_pages <= 0:
            raise ValueError("max_pages must be a positive integer")
        if max_pages > 10000:
            raise ValueError("max_pages must not exceed 10000")
        self.max_pages = max_pages

        if not project_id and not project_name:
            raise ValueError("Either project_id or project_name must be provided")

        # Create a shared requests session with timeout
        self._session = requests.Session()
        if self.token:
            self._session.headers["PRIVATE-TOKEN"] = self.token

        # Track closed state for resource cleanup
        self._closed = False

    def close(self) -> None:
        """Close the filesystem and cleanup resources.

        This method closes the internal requests session to prevent resource leaks.
        It is safe to call this method multiple times.
        """
        if not self._closed:
            logger.debug("Closing GitLabFileSystem and cleaning up resources")
            self._session.close()
            self._closed = True

    def __del__(self) -> None:
        """Destructor to ensure cleanup when object is garbage collected."""
        try:
            self.close()
        except Exception:
            # Silently ignore errors in destructor
            pass

    def _get_project_identifier(self) -> str:
        """Get URL-encoded project identifier for API calls.

        Returns:
            URL-encoded project identifier (ID or path)
        """
        if self.project_id:
            identifier = str(self.project_id)
        else:
            identifier = self.project_name

        # URL-encode the project identifier to handle special characters
        return urllib.parse.quote(identifier, safe="")

    def _make_request(self, endpoint: str, params: dict = None) -> requests.Response:
        """Make API request to GitLab with proper error handling.

        Args:
            endpoint: API endpoint
            params: Query parameters

        Returns:
            Response object

        Raises:
            requests.RequestException: For HTTP errors
        """
        if params is None:
            params = {}

        # URL-encode the endpoint path
        encoded_endpoint = urllib.parse.quote(endpoint, safe="")
        project_identifier = self._get_project_identifier()

        url = f"{self.base_url}/api/{self.api_version}/projects/{project_identifier}/{encoded_endpoint}"

        try:
            response = self._session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            logger.error(
                "GitLab API request failed: %s %s - %s",
                e.response.status_code if e.response else "N/A",
                e.response.reason if e.response else str(e),
                url,
            )
            if e.response is not None:
                logger.error("Response content: %s", e.response.text[:500])
            raise

    def _get_file_path(self, path: str) -> str:
        """Get URL-encoded full file path in repository.

        Args:
            path: File path

        Returns:
            URL-encoded full file path
        """
        # Remove leading slash if present
        path = path.lstrip("/")
        # URL-encode the path to handle special characters
        encoded_path = urllib.parse.quote(path, safe="")
        return f"/{encoded_path}"

    def ls(
        self, path: str = "", detail: bool = False, **kwargs: Any
    ) -> list[Any] | Any:
        """List files in repository with pagination support.

        Args:
            path: Directory path
            detail: Whether to return detailed information
            **kwargs: Additional arguments

        Returns:
            List of files
        """
        all_files = []
        page = 1
        per_page = 100
        pages_fetched = 0

        while pages_fetched < self.max_pages:
            params = {"ref": self.ref, "per_page": per_page, "page": page}

            if path:
                params["path"] = path.lstrip("/")

            try:
                response = self._make_request("repository/tree", params)
                files = response.json()

                if not files:
                    # No more pages
                    break

                all_files.extend(files)
                pages_fetched += 1

                # Check for pagination headers
                next_page = response.headers.get("X-Next-Page")
                if not next_page:
                    # No more pages
                    break

                # Try to parse the next page number
                try:
                    page = int(next_page)
                except (ValueError, TypeError):
                    # Malformed pagination header
                    logger.warning(
                        "Malformed X-Next-Page header: '%s', stopping pagination at page %d",
                        next_page,
                        page,
                    )
                    break

            except requests.RequestException:
                # If we have some files already, return what we have
                if all_files:
                    logger.warning(
                        "GitLab API request failed for page %d, returning %d files from previous pages",
                        page,
                        len(all_files),
                    )
                    break
                else:
                    # Re-raise if no files collected yet
                    raise
        else:
            # Loop ended due to max_pages limit
            logger.warning(
                "Reached maximum pages limit (%d), returning %d files",
                self.max_pages,
                len(all_files),
            )

        if detail:
            return all_files
        else:
            return [item["name"] for item in all_files]

    def cat_file(self, path: str, **kwargs: Any) -> bytes:
        """Get file content.

        Args:
            path: File path
            **kwargs: Additional arguments

        Returns:
            File content

        Raises:
            requests.HTTPError: If file not found or other HTTP error
        """
        params = {"ref": self.ref}

        # URL-encode the file path
        encoded_path = urllib.parse.quote(path.lstrip("/"), safe="")

        response = self._make_request(f"repository/files/{encoded_path}", params)
        data = response.json()

        import base64

        return base64.b64decode(data["content"])

    def info(self, path: str, **kwargs: Any) -> dict:
        """Get file information.

        Args:
            path: File path
            **kwargs: Additional arguments

        Returns:
            File information

        Raises:
            requests.HTTPError: If file not found or other HTTP error
        """
        params = {"ref": self.ref}

        # URL-encode the file path
        encoded_path = urllib.parse.quote(path.lstrip("/"), safe="")

        response = self._make_request(f"repository/files/{encoded_path}", params)
        return response.json()

    def exists(self, path: str, **kwargs: Any) -> bool:
        """Check if file exists.

        Args:
            path: File path
            **kwargs: Additional arguments

        Returns:
            True if file exists
        """
        try:
            self.info(path, **kwargs)
            return True
        except requests.HTTPError as e:
            if e.response and e.response.status_code == 404:
                return False
            # Re-raise for other HTTP errors
            raise
        except requests.RequestException:
            # Re-raise for other request errors
            raise
