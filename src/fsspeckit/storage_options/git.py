"""Git-based storage options for GitHub and GitLab."""

import os
from typing import Union

from .base import BaseStorageOptions


class GitHubStorageOptions(BaseStorageOptions, frozen=False):
    """GitHub repository storage configuration options.

    Provides access to files in GitHub repositories with support for:
    - Public and private repositories
    - Branch/tag/commit selection
    - Token-based authentication
    - Custom GitHub Enterprise instances

    Attributes:
        protocol (str): Always "github" for GitHub storage
        org (str): Organization or user name
        repo (str): Repository name
        ref (str): Git reference (branch, tag, or commit SHA)
        token (str): GitHub personal access token
        api_url (str): Custom GitHub API URL for enterprise instances

    Example:
        ```python
        # Public repository
        options = GitHubStorageOptions(
            org="microsoft",
            repo="vscode",
            ref="main",
        )

        # Private repository
        options = GitHubStorageOptions(
            org="myorg",
            repo="private-repo",
            token="ghp_xxxx",
            ref="develop",
        )

        # Enterprise instance
        options = GitHubStorageOptions(
            org="company",
            repo="internal",
            api_url="https://github.company.com/api/v3",
            token="ghp_xxxx",
        )
        ```
    """

    protocol: str = "github"
    org: Union[str, None] = None
    repo: Union[str, None] = None
    ref: Union[str, None] = None
    token: Union[str, None] = None
    api_url: Union[str, None] = None

    @classmethod
    def from_env(cls) -> "GitHubStorageOptions":
        """Create storage options from environment variables.

        Reads standard GitHub environment variables:
        - GITHUB_ORG: Organization or user name
        - GITHUB_REPO: Repository name
        - GITHUB_REF: Git reference
        - GITHUB_TOKEN: Personal access token
        - GITHUB_API_URL: Custom API URL

        Returns:
            GitHubStorageOptions: Configured storage options

        Example:
            ```python
            # With environment variables set:
            options = GitHubStorageOptions.from_env()
            print(options.org)  # From GITHUB_ORG
            # 'microsoft'
            ```
        """
        return cls(
            protocol="github",
            org=os.getenv("GITHUB_ORG"),
            repo=os.getenv("GITHUB_REPO"),
            ref=os.getenv("GITHUB_REF"),
            token=os.getenv("GITHUB_TOKEN"),
            api_url=os.getenv("GITHUB_API_URL"),
        )

    def to_env(self) -> None:
        """Export options to environment variables.

        Sets standard GitHub environment variables.

        Example:
            ```python
            options = GitHubStorageOptions(
                org="microsoft",
                repo="vscode",
                token="ghp_xxxx",
            )
            options.to_env()
            print(os.getenv("GITHUB_ORG"))
            # 'microsoft'
            ```
        """
        env = {
            "GITHUB_ORG": self.org,
            "GITHUB_REPO": self.repo,
            "GITHUB_REF": self.ref,
            "GITHUB_TOKEN": self.token,
            "GITHUB_API_URL": self.api_url,
        }
        env = {k: v for k, v in env.items() if v is not None}
        os.environ.update(env)

    def to_fsspec_kwargs(self) -> dict:
        """Convert options to fsspec filesystem arguments.

        Returns:
            dict: Arguments suitable for GitHubFileSystem

        Example:
            >>> options = GitHubStorageOptions(
            ...     org="microsoft",
            ...     repo="vscode",
            ...     token="ghp_xxxx"
            ... )
            >>> kwargs = options.to_fsspec_kwargs()
            >>> fs = filesystem("github", **kwargs)
        """
        kwargs = {
            "org": self.org,
            "repo": self.repo,
            "ref": self.ref,
            "token": self.token,
            "api_url": self.api_url,
        }
        return {k: v for k, v in kwargs.items() if v is not None}


class GitLabStorageOptions(BaseStorageOptions, frozen=False):
    """GitLab repository storage configuration options.

    Provides access to files in GitLab repositories with support for:
    - Public and private repositories
    - Self-hosted GitLab instances
    - Project ID or name-based access
    - Branch/tag/commit selection
    - Token-based authentication

    Attributes:
        protocol (str): Always "gitlab" for GitLab storage
        base_url (str): GitLab instance URL, defaults to gitlab.com
        project_id (Union[str, int]): Project ID number
        project_name (str): Project name/path
        ref (str): Git reference (branch, tag, or commit SHA)
        token (str): GitLab personal access token
        api_version (str): API version to use

    Example:
        ```python
        # Public project on gitlab.com
        options = GitLabStorageOptions(
            project_name="group/project",
            ref="main",
        )

        # Private project with token
        options = GitLabStorageOptions(
            project_id=12345,
            token="glpat_xxxx",
            ref="develop",
        )

        # Self-hosted instance
        options = GitLabStorageOptions(
            base_url="https://gitlab.company.com",
            project_name="internal/project",
            token="glpat_xxxx",
        )
        ```
    """

    protocol: str = "gitlab"
    base_url: str = "https://gitlab.com"
    project_id: Union[str, int] | None = None
    project_name: Union[str, None] = None
    ref: Union[str, None] = None
    token: Union[str, None] = None
    api_version: str = "v4"

    def __post_init__(self) -> None:
        """Validate GitLab configuration after initialization.

        Ensures either project_id or project_name is provided.

        Raises:
            ValueError: If neither project_id nor project_name is provided
        """
        if self.project_id is None and self.project_name is None:
            raise ValueError("Either project_id or project_name must be provided")

    @classmethod
    def from_env(cls) -> "GitLabStorageOptions":
        """Create storage options from environment variables.

        Reads standard GitLab environment variables:
        - GITLAB_URL: Instance URL
        - GITLAB_PROJECT_ID: Project ID
        - GITLAB_PROJECT_NAME: Project name/path
        - GITLAB_REF: Git reference
        - GITLAB_TOKEN: Personal access token
        - GITLAB_API_VERSION: API version

        Returns:
            GitLabStorageOptions: Configured storage options

        Example:
            ```python
            # With environment variables set:
            options = GitLabStorageOptions.from_env()
            print(options.project_id)  # From GITLAB_PROJECT_ID
            # '12345'
            ```
        """
        return cls(
            protocol="gitlab",
            base_url=os.getenv("GITLAB_URL", "https://gitlab.com"),
            project_id=os.getenv("GITLAB_PROJECT_ID"),
            project_name=os.getenv("GITLAB_PROJECT_NAME"),
            ref=os.getenv("GITLAB_REF"),
            token=os.getenv("GITLAB_TOKEN"),
            api_version=os.getenv("GITLAB_API_VERSION", "v4"),
        )

    def to_env(self) -> None:
        """Export options to environment variables.

        Sets standard GitLab environment variables.

        Example:
            ```python
            options = GitLabStorageOptions(
                project_id=12345,
                token="glpat_xxxx",
            )
            options.to_env()
            print(os.getenv("GITLAB_PROJECT_ID"))
            # '12345'
            ```
        """
        env = {
            "GITLAB_URL": self.base_url,
            "GITLAB_PROJECT_ID": str(self.project_id) if self.project_id else None,
            "GITLAB_PROJECT_NAME": self.project_name,
            "GITLAB_REF": self.ref,
            "GITLAB_TOKEN": self.token,
            "GITLAB_API_VERSION": self.api_version,
        }
        env = {k: v for k, v in env.items() if v is not None}
        os.environ.update(env)

    def to_fsspec_kwargs(self) -> dict:
        """Convert options to fsspec filesystem arguments.

        Returns:
            dict: Arguments suitable for GitLabFileSystem

        Example:
            ```python
            options = GitLabStorageOptions(
                project_id=12345,
                token="glpat_xxxx",
            )
            kwargs = options.to_fsspec_kwargs()
            fs = filesystem("gitlab", **kwargs)
            ```
        """
        kwargs = {
            "base_url": self.base_url,
            "project_id": self.project_id,
            "project_name": self.project_name,
            "ref": self.ref,
            "token": self.token,
            "api_version": self.api_version,
        }
        return {k: v for k, v in kwargs.items() if v is not None}
