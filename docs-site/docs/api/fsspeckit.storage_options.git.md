# `fsspeckit.storage_options.git` API Reference

## `GitHubStorageOptions`

GitHub repository storage configuration options.

Provides access to files in GitHub repositories with support for: - Public and private repositories - Branch/tag/commit selection - Token-based authentication - Custom GitHub Enterprise instances

**Attributes:**

- `protocol` (`str`): Always "github" for GitHub storage
- `org` (`str`): Organization or user name
- `repo` (`str`): Repository name
- `ref` (`str`): Git reference (branch, tag, or commit SHA
- `token` (`str`): GitHub personal access token
- `api_url` (`str`): Custom GitHub API URL for enterprise instances

**Example:**
```python
# Public repository
options = GitHubStorageOptions(
    org="microsoft",
    repo="vscode",
    ref="main"
)

# Private repository
options = GitHubStorageOptions(
    org="myorg",
    repo="private-repo",
    token="ghp_xxxx",
    ref="develop"
)

# Enterprise instance
options = GitHubStorageOptions(
    org="company",
    repo="internal",
    api_url="https://github.company.com/api/v3",
    token="ghp_xxxx"
)
```

### `from_env()`

Create storage options from environment variables.

Reads standard GitHub environment variables: - GITHUB_ORG: Organization or user name - GITHUB_REPO: Repository name - GITHUB_REF: Git reference - GITHUB_TOKEN: Personal access token - GITHUB_API_URL: Custom API URL

**Returns:**

- `GitHubStorageOptions`: Configured storage options

**Example:**
```python
# With environment variables set:
options = GitHubStorageOptions.from_env()
print(options.org)  # From GITHUB_ORG 'microsoft'
```

### `to_env()`

Export options to environment variables.

Sets standard GitHub environment variables.

**Example:**
```python
options = GitHubStorageOptions(
    org="microsoft",
    repo="vscode",
    token="ghp_xxxx"
)
options.to_env()
print(os.getenv("GITHUB_ORG"))  # 'microsoft'
```

### `to_fsspec_kwargs()`

Convert options to fsspec filesystem arguments.

**Returns:**

- `dict`: Arguments suitable for GitHubFileSystem

**Example:**
```python
options = GitHubStorageOptions(
    org="microsoft",
    repo="vscode",
    token="ghp_xxxx"
)
kwargs = options.to_fsspec_kwargs()
fs = filesystem("github", **kwargs)
```

## `GitLabStorageOptions`

GitLab repository storage configuration options.

Provides access to files in GitLab repositories with support for:
- Public and private repositories
- Self-hosted GitLab instances
- Project ID or name-based access
- Branch/tag/commit selection
- Token-based authentication
- **Hardened HTTP handling** with URL encoding, pagination, and timeouts

**Attributes:**

- `protocol` (`str`): Always "gitlab" for GitLab storage
- `base_url` (`str`): GitLab instance URL, defaults to gitlab.com
- `project_id` (`str` | `int`): Project ID number
- `project_name` (`str`): Project name/path
- `ref` (`str`): Git reference (branch, tag, or commit SHA)
- `token` (`str`): GitLab personal access token
- `api_version` (`str`): API version to use
- `timeout` (`float`): Request timeout in seconds (default: 30.0)

**Example:**
```python
# Public project on gitlab.com
options = GitLabStorageOptions(
    project_name="group/project",
    ref="main"
)

# Private project with custom timeout
options = GitLabStorageOptions(
    project_id=12345,
    token="glpat_xxxx",
    ref="develop",
    timeout=60.0  # 1 minute timeout for slow connections
)

# Self-hosted instance with extended timeout
options = GitLabStorageOptions(
    base_url="https://gitlab.company.com",
    project_name="internal/project",
    token="glpat_xxxx",
    timeout=120.0  # 2 minutes for enterprise instances
)
```

### `from_env()`

Create storage options from environment variables.

Reads standard GitLab environment variables:
- GITLAB_URL: Instance URL
- GITLAB_PROJECT_ID: Project ID
- GITLAB_PROJECT_NAME: Project name/path
- GITLAB_REF: Git reference
- GITLAB_TOKEN: Personal access token
- GITLAB_API_VERSION: API version
- GITLAB_TIMEOUT: Request timeout in seconds

**Returns:**

- `GitLabStorageOptions`: Configured storage options

**Example:**
```python
# With environment variables set:
# export GITLAB_PROJECT_ID=12345
# export GITLAB_TIMEOUT=60.0
options = GitLabStorageOptions.from_env()
print(options.project_id)  # From GITLAB_PROJECT_ID '12345'
print(options.timeout)     # From GITLAB_TIMEOUT '60.0'
```

### `to_env()`

Export options to environment variables.

Sets standard GitLab environment variables including GITLAB_TIMEOUT.

**Example:**
```python
options = GitLabStorageOptions(
    project_id=12345,
    token="glpat_xxxx",
    timeout=60.0
)
options.to_env()
print(os.getenv("GITLAB_PROJECT_ID"))  # '12345'
print(os.getenv("GITLAB_TIMEOUT"))     # '60.0'
```

### `to_fsspec_kwargs()`

Convert options to fsspec filesystem arguments.

**Returns:**

- `dict`: Arguments suitable for GitLabFileSystem

**Example:**
```python
options = GitLabStorageOptions(
    project_id=12345,
    token="glpat_xxxx"
)
kwargs = options.to_fsspec_kwargs()
fs = filesystem("gitlab", **kwargs)
```