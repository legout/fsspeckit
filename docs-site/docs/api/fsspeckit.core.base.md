# `fsspeckit.core.base` API Documentation

This module provides core filesystem functionalities and utilities, including custom cache mappers, enhanced cached filesystems, and a GitLab filesystem implementation.

---

## `FileNameCacheMapper`

Maps remote file paths to local cache paths while preserving directory structure.

This cache mapper maintains the original file path structure in the cache directory, creating necessary subdirectories as needed.

**Attributes:**

*   `directory` (`str`): Base directory for cached files

**Example:**

```python
from fsspeckit.core.base import FileNameCacheMapper

# Create cache mapper for S3 files
mapper = FileNameCacheMapper("/tmp/cache")

# Map remote path to cache path
cache_path = mapper("bucket/data/file.csv")
print(cache_path)  # Preserves structure
```

### `__init__()`

Initialize cache mapper with base directory.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `directory` | `str` | Base directory where cached files will be stored |

### `__call__()`

Map remote file path to cache file path.

Creates necessary subdirectories in the cache directory to maintain the original path structure.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `path` | `str` | Original file path from remote filesystem |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `str` | `str` | Cache file path that preserves original structure |

**Example:**

```python
from fsspeckit.core.base import FileNameCacheMapper

mapper = FileNameCacheMapper("/tmp/cache")
# Maps maintain directory structure
print(mapper("data/nested/file.txt"))
```

---

## `MonitoredSimpleCacheFileSystem`

Enhanced caching filesystem with monitoring and improved path handling.

This filesystem extends `SimpleCacheFileSystem` to provide:

- Verbose logging of cache operations
- Improved path mapping for cache files
- Enhanced synchronization capabilities
- Better handling of parallel operations

**Attributes:**

*   `_verbose` (`bool`): Whether to print verbose cache operations
*   `_mapper` (`FileNameCacheMapper`): Maps remote paths to cache paths
*   `storage` (`list[str]`): List of cache storage locations
*   `fs` (`AbstractFileSystem`): Underlying filesystem being cached

**Example:**

```python
from fsspec import filesystem
from fsspeckit.core.base import MonitoredSimpleCacheFileSystem

s3_fs = filesystem("s3")
cached_fs = MonitoredSimpleCacheFileSystem(
    fs=s3_fs,
    cache_storage="/tmp/cache",
    verbose=True
)
# Use cached_fs like any other filesystem
files = cached_fs.ls("my-bucket/")
```

### `__init__()`

Initialize monitored cache filesystem.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `fs` | `Optional[fsspec.AbstractFileSystem]` | Underlying filesystem to cache. If None, creates a local filesystem. |
| `cache_storage` | `Union[str, list[str]]` | Cache storage location(s). Can be string path or list of paths. |
| `verbose` | `bool` | Whether to enable verbose logging of cache operations. |
| `**kwargs` | `Any` | Additional arguments passed to `SimpleCacheFileSystem`. |

**Example:**

```python
# Cache S3 filesystem
s3_fs = filesystem("s3")
cached = MonitoredSimpleCacheFileSystem(
    fs=s3_fs,
    cache_storage="/tmp/s3_cache",
    verbose=True
)
```

### `_check_cache()`

Check if file exists in cache and return cache path if found.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `path` | `str` | Remote file path to check |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `Optional[str]` | `str` or `None` | Cache file path if found, None otherwise |

**Example:**

```python
from fsspeckit.core.base import MonitoredSimpleCacheFileSystem
from fsspec import filesystem

cached_fs = MonitoredSimpleCacheFileSystem(fs=filesystem("s3"), cache_storage="/tmp/cache")
# Check if a file is in cache
cache_path = cached_fs._check_cache("my-bucket/data/file.txt")
if cache_path:
    print(f"File found in cache at: {cache_path}")
else:
    print("File not in cache.")
```

### `_check_file()`

Ensure file is in cache, downloading if necessary.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `path` | `str` | Remote file path |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `str` | `str` | Local cache path for the file |

**Example:**

```python
from fsspeckit.core.base import MonitoredSimpleCacheFileSystem
from fsspec import filesystem

cached_fs = MonitoredSimpleCacheFileSystem(fs=filesystem("s3"), cache_storage="/tmp/cache")
# Ensure file is in cache (downloads if not present)
local_path = cached_fs._check_file("my-bucket/data/large_file.parquet")
print(f"File available locally at: {local_path}")
```

---

## `GitLabFileSystem`

Filesystem interface for GitLab repositories.

Provides read-only access to files in GitLab repositories, including:

- Public and private repositories
- Self-hosted GitLab instances
- Branch/tag/commit selection
- Token-based authentication
- **URL-encoded paths** for handling special characters
- **Automatic pagination** for large directory listings
- **Configurable timeouts** to prevent hanging requests
- **Enhanced error logging** for debugging

**Attributes:**

*   `protocol` (`str`): Always "gitlab"
*   `base_url` (`str`): GitLab instance URL
*   `project_id` (`str`): Project ID
*   `project_name` (`str`): Project name/path
*   `ref` (`str`): Git reference (branch, tag, commit)
*   `token` (`str`): Access token
*   `api_version` (`str`): API version
*   `timeout` (`float`): Request timeout in seconds (default: 30.0)

**Example:**

```python
# Public repository with default timeout
fs = GitLabFileSystem(
    project_name="group/project",
    ref="main"
)
files = fs.ls("/")

# Private repository with custom timeout
fs = GitLabFileSystem(
    project_id="12345",
    token="glpat_xxxx",
    ref="develop",
    timeout=60.0  # 1 minute timeout
)
content = fs.cat("README.md")

# Repository with special characters in path
fs = GitLabFileSystem(
    project_name="group/project with spaces",
    ref="main"
)
# Handles URL encoding automatically for paths like "file with spaces.txt"
```

### `__init__()`

Initialize GitLab filesystem.

| Parameter | Type | Description |
| | :-------- | :--- | :---------- |
| `base_url` | `str` | GitLab instance URL |
| `project_id` | `Optional[Union[str, int]]` | Project ID number |
| `project_name` | `Optional[str]` | Project name/path (alternative to project_id) |
| `ref` | `str` | Git reference (branch, tag, or commit SHA) |
| `token` | `Optional[str]` | GitLab personal access token |
| `api_version` | `str` | API version to use |
| `timeout` | `float` | Request timeout in seconds (default: 30.0) |

| `**kwargs` | `Any` | Additional filesystem arguments |

| Raises | Type | Description |
| | :----- | :--- | :----------
| `ValueError` | `ValueError` | If neither `project_id` nor `project_name` is provided |

**Example:**

```python
from fsspeckit.core.base import GitLabFileSystem

# Access a public repository
fs_public = GitLabFileSystem(
    project_name="gitlab-org/gitlab",
    ref="master"
)
print(fs_public.ls("README.md"))

# Access with custom timeout for slow connections
fs_slow = GitLabFileSystem(
    project_name="large-project",
    ref="main",
    timeout=120.0  # 2 minutes for large repositories
)

# Access a private repository (replace with your token and project info)
# fs_private = GitLabFileSystem(
#     project_id="12345",
# #    token="your_private_token",
#     ref="main"
# )
# print(fs_private.ls("/"))
```

### `ls()`

List directory contents with automatic pagination support.

This method handles large directory listings by automatically following GitLab's pagination headers. For repositories with many files, it will make multiple API requests to ensure complete results.

| Parameter | Type | Description |
| | :-------- | :--- | :---------- |
| `path` | `str` | Directory path to list |
| `detail` | `bool` | Whether to return detailed information |
| `**kwargs` | `Any` | Additional options |

| Returns | Type | Description |
| | :------ | :--- | :---------- |
| `Union[List[str], List[dict]]` | `list` | List of file names (if detail=False) or file info dictionaries (if detail=True) |

**Example:**

```python
fs = GitLabFileSystem(project_name="gitlab-org/gitlab")

# List files in root directory (automatically handles pagination)
files = fs.ls("/")
print(f"Found {len(files)} files")

# Get detailed information
detailed_files = fs.ls("/", detail=True)
for file_info in detailed_files:
    print(f"{file_info['name']}: {file_info['type']}")

# List files in subdirectory
readme_files = fs.ls("doc/")
```

### `cat_file()`

Get file content from GitLab repository.

Handles URL encoding automatically for paths with special characters.

| Parameter | Type | Description |
| | :-------- | :--- | :---------- |
| `path` | `str` | File path in repository |

| Returns | Type | Description |
| | :------ | :--- | :---------- |
| `bytes` | `bytes` | File content as bytes |

| Raises | Type | Description |
| | :----- | :--- | :---------- |
| `requests.HTTPError` | `requests.HTTPError` | For HTTP errors (404 for missing files, etc.) |

**Example:**

```python
fs = GitLabFileSystem(project_name="gitlab-org/gitlab")

# Get file content
content = fs.cat_file("README.md")
print(content.decode('utf-8'))

# Files with special characters are handled automatically
content = fs.cat_file("path with spaces/file-name.txt")
```

### `exists()`

Check if a file exists in the repository.

| Parameter | Type | Description |
| | :-------- | :--- | :---------- |
| `path` | `str` | File path to check |

| Returns | Type | Description |
| | :------ | :--- | :---------- |
| `bool` | `bool` | True if file exists, False otherwise |

**Example:**

```python
fs = GitLabFileSystem(project_name="gitlab-org/gitlab")

if fs.exists("README.md"):
    print("README.md exists")
    content = fs.cat_file("README.md")
else:
    print("README.md not found")
```

### Hardened Features

The GitLabFileSystem includes several hardening improvements:

1. **URL Encoding**: All project identifiers and file paths are URL-encoded to handle special characters correctly.

2. **Shared HTTP Session**: Uses a single `requests.Session` with configured timeout for better performance and reliability.

3. **Automatic Pagination**: The `ls()` method automatically follows GitLab pagination headers to return complete directory listings.

4. **Enhanced Error Logging**: HTTP errors are logged with context information including status codes and response content for easier debugging.

5. **Timeout Protection**: All HTTP requests include a timeout to prevent hanging on slow or unresponsive GitLab instances.

### `__init__()`

Initialize GitLab filesystem.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `base_url` | `str` | GitLab instance URL |
| `project_id` | `Optional[Union[str, int]]` | Project ID number |
| `project_name` | `Optional[str]` | Project name/path (alternative to project_id) |
| `ref` | `str` | Git reference (branch, tag, or commit SHA) |
| `token` | `Optional[str]` | GitLab personal access token |
| `api_version` | `str` | API version to use |

| `**kwargs` | `Any` | Additional filesystem arguments |

| Raises | Type | Description |
| :----- | :--- | :----------
| `ValueError` | `ValueError` | If neither `project_id` nor `project_name` is provided |

**Example:**

```python
from fsspeckit.core.base import GitLabFileSystem

# Access a public repository
fs_public = GitLabFileSystem(
    project_name="gitlab-org/gitlab",
    ref="master"
)
print(fs_public.ls("README.md"))

# Access a private repository (replace with your token and project info)
# fs_private = GitLabFileSystem(
#     project_id="12345",
# #    token="your_private_token",
#     ref="main"
# )
# print(fs_private.ls("/"))
```

### `_get_file_content()`

Get file content from GitLab API.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `path` | `str` | File path in repository |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `bytes` | `bytes` | File content as bytes |

**Example:**

```python
from fsspeckit.core.base import GitLabFileSystem

fs = GitLabFileSystem(project_name="gitlab-org/gitlab")
content = fs.cat("README.md")
print(content[:50])
```

| Raises | Type | Description |
| :----- | :--- | :---------- |
| `FileNotFoundError` | `FileNotFoundError` | If file doesn't exist |
| `requests.HTTPError` | `requests.HTTPError` | For other HTTP errors |

### `_open()`

Open file for reading.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `path` | `str` | File path to open |
| `mode` | `str` | File mode (only 'rb' and 'r' supported) |
| `block_size` | `Optional[int]` | Block size for reading (unused) |
| `cache_options` | `Optional[dict]` | Cache options (unused) |
| `**kwargs` | `Any` | Additional options |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| File-like object | File-like object | File-like object for reading 

| Raises | Type | Description |
| :----- | :--- | :----------
| `ValueError` | `ValueError` | If mode is not supported |

### `cat()`

Get file contents as bytes.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `path` | `str` | File path |
| `**kwargs` | `Any` | Additional options |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `bytes` | `bytes` | File content as bytes |

### `ls()`

List directory contents.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `path` | `str` | Directory path to list |
| `detail` | `bool` | Whether to return detailed information |
| `**kwargs` | `Any` | Additional options |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `list` | `list` | List of files/directories or their details |

### `exists()`

Check if file or directory exists.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `path` | `str` | Path to check |
| `**kwargs` | `Any` | Additional options |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `bool` | `bool` | True if path exists, False otherwise |

### `info()`

Get file information.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `path` | `str` | File path |
| `**kwargs` | `Any` | Additional options |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `dict` | `dict` | Dictionary with file information |

| Raises | Type | Description |
| :----- | :--- | :---------- |
| `FileNotFoundError` | `FileNotFoundError` | If file not found |

---

## `filesystem()`

Get filesystem instance with enhanced configuration options.

Creates filesystem instances with support for storage options classes, intelligent caching, and protocol inference from paths.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `protocol_or_path` | `str` | Filesystem protocol (e.g., "s3", "file") or path with protocol prefix |
| `storage_options` | `Optional[Union[BaseStorageOptions, dict]]` | Storage configuration as `BaseStorageOptions` instance or dict |
| `cached` | `bool` | Whether to wrap filesystem in caching layer |
| `cache_storage` | `Optional[str]` | Cache directory path (if `cached=True`) |
| `verbose` | `bool` | Enable verbose logging for cache operations |
| `dirfs` | `bool` | Whether to wrap the filesystem in a `DirFileSystem`. Defaults to `True`. |
| `base_fs` | `AbstractFileSystem` | An existing filesystem to wrap. |
| `**kwargs` | `Any` | Additional filesystem arguments |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `AbstractFileSystem` | `fsspec.AbstractFileSystem` | Configured filesystem instance |

**Example:**

```python
# Basic local filesystem
fs = filesystem("file")

# S3 with storage options
from fsspeckit.storage_options.cloud import AwsStorageOptions
opts = AwsStorageOptions(region="us-west-2")
fs = filesystem("s3", storage_options=opts, cached=True)

# Infer protocol from path
fs = filesystem("s3://my-bucket/", cached=True)

# GitLab filesystem
fs = filesystem("gitlab", storage_options={
    "project_name": "group/project",
    "token": "glpat_xxxx"
})
```

---

## `get_filesystem()`

Get filesystem instance with enhanced configuration options.

!!! warning "Deprecated"
    Use [`filesystem`](#filesystem) instead. This function will be removed in a future version.

Creates filesystem instances with support for storage options classes, intelligent caching, and protocol inference from paths.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `protocol_or_path` | `str` | Filesystem protocol (e.g., "s3", "file") or path with protocol prefix |
| `storage_options` | `Optional[Union[BaseStorageOptions, dict]]` | Storage configuration as `BaseStorageOptions` instance or dict |
| `cached` | `bool` | Whether to wrap filesystem in caching layer 
| `cache_storage` | `Optional[str]` | Cache directory path (if `cached=True`) |
| `verbose` | `bool` | Enable verbose logging for cache operations |
| `**kwargs` | `Any` | Additional filesystem arguments |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `fsspec.AbstractFileSystem` | `fsspec.AbstractFileSystem` | Configured filesystem instance |

**Example:**

```python
# Basic local filesystem
fs = get_filesystem("file")

# S3 with storage options
from fsspeckit.storage_options.cloud import AwsStorageOptions
opts = AwsStorageOptions(region="us-west-2")
fs = get_filesystem("s3", storage_options=opts, cached=True)

# Infer protocol from path
fs = get_filesystem("s3://my-bucket/", cached=True)

# GitLab filesystem
fs = get_filesystem("gitlab", storage_options={
    "project_name": "group/project",
    "token": "glpat_xxxx"
})
```
