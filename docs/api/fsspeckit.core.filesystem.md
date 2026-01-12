# fsspeckit.core.filesystem

> **Package Structure Note:** fsspeckit has been refactored to use a package-based structure. The preferred import path is now `from fsspeckit.core import filesystem`, though legacy imports still work.

## Path Normalization

The `normalize_path` function is the unified path normalization utility for fsspeckit. It handles:

- String-only normalization (no filesystem context)
- Filesystem-aware normalization (local and remote filesystems)
- Optional validation for security and operation-specific checks

### normalize_path

```python
from fsspeckit.core.filesystem import normalize_path
from fsspeckit.datasets import normalize_path  # Also available from datasets module
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `path` | `str \| Path` | - | Path to normalize (string or Path object) |
| `filesystem` | `AbstractFileSystem \| None` | `None` | Optional filesystem instance for filesystem-aware normalization. If `None`, performs string-only normalization. |
| `validate` | `bool` | `False` | Whether to perform validation checks on the path. When `True`, may raise `ValueError` or `DatasetPathError`. |
| `operation` | `str \| None` | `None` | Optional operation context for validation (e.g., `'read'`, `'write'`). Used when `validate=True` to determine appropriate validation checks. |

#### Returns

`str` - The normalized path as a string.

#### Raises

- `ValueError` - If path is `None`, contains forbidden characters (when `validate=True`), or fails basic validation.
- `DatasetPathError` - If validation fails with filesystem context (when `validate=True` and `filesystem` is provided).

#### Examples

**String-only normalization (no filesystem):**

```python
from fsspeckit.core.filesystem import normalize_path

# Basic path normalization
normalize_path("data/../file.parquet")  # Returns: 'file.parquet'
normalize_path("./data/file.parquet")  # Returns: 'data/file.parquet'

# Backslash conversion (Windows paths)
normalize_path("data\\file.parquet")  # Returns: 'data/file.parquet'

# URL-like paths with protocols
normalize_path("s3://bucket/path/../file.parquet")  # Returns: 's3://bucket/file.parquet'
normalize_path("gs://bucket/path/../file.parquet")  # Returns: 'gs://bucket/file.parquet'
```

**Local filesystem normalization:**

```python
from fsspeckit.core.filesystem import normalize_path
from fsspec.implementations.local import LocalFileSystem

fs = LocalFileSystem()

# Returns absolute path for local filesystem
result = normalize_path("data/file.parquet", filesystem=fs)
# Result: '/absolute/path/to/data/file.parquet'

# Absolute paths remain absolute
result = normalize_path("/tmp/test.parquet", filesystem=fs)
# Result: '/tmp/test.parquet'
```

**Remote filesystem normalization:**

```python
from fsspeckit.core.filesystem import normalize_path
from unittest.mock import Mock

# S3 filesystem
fs_s3 = Mock()
fs_s3.protocol = "s3"

# Path without protocol - adds protocol based on filesystem
normalize_path("bucket/key/file.parquet", filesystem=fs_s3)
# Returns: 's3://bucket/key/file.parquet'

# Path with protocol - normalizes the path portion
normalize_path("s3://bucket/path/../file.parquet", filesystem=fs_s3)
# Returns: 's3://bucket/file.parquet'
```

**With validation:**

```python
from fsspeckit.core.filesystem import normalize_path
from fsspeckit.datasets.exceptions import DatasetPathError

# Basic validation (checks for forbidden characters)
normalize_path("data/file.parquet", validate=True)
# Returns: 'data/file.parquet'

# Path with null bytes raises ValueError
try:
    normalize_path("data/file\x00.parquet", validate=True)
except ValueError as e:
    print(e)  # "Path contains forbidden control character: '\x00'"

# With operation context - raises DatasetPathError
try:
    normalize_path("data/file\x00.parquet", validate=True, operation="read")
except DatasetPathError as e:
    print(e.operation)  # 'read'
```

### Legacy API

The `_normalize_path` function is deprecated but maintained for backward compatibility:

```python
from fsspeckit.core.filesystem.paths import _normalize_path
import warnings

# Emits deprecation warning
with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter("always")
    result = _normalize_path("data/../file.parquet")
    # Result: 'file.parquet'
    # Warning: DeprecationWarning about using normalize_path() instead
```

**Migration Guide:**

| Legacy Usage | New Usage |
|--------------|-----------|
| `_normalize_path(path)` | `normalize_path(path)` |
| `normalize_path(path, fs)` | `normalize_path(path, filesystem=fs)` |

### Related Functions

#### validate_dataset_path

Comprehensive path validation for dataset operations:

```python
from fsspeckit.datasets.path_utils import validate_dataset_path

validate_dataset_path(path, filesystem, operation)
```

Validates:
- Security (forbidden characters)
- Path existence for read operations
- Parent directory existence for write operations
- Protocol support

### Supported Protocols

The path normalization supports these protocols:

- `s3`, `s3a` - Amazon S3
- `gs`, `gcs` - Google Cloud Storage
- `az`, `abfs`, `abfss` - Azure Blob Storage
- `file` - Local filesystem
- `github` - GitHub
- `gitlab` - GitLab

::: fsspeckit.core.filesystem
