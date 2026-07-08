# `fsspeckit.storage_options.base` API Documentation

This module defines the base class for filesystem storage configuration options.

---

## `BaseStorageOptions`

Base class for filesystem storage configuration options.

Provides common functionality for all storage option classes including:

- YAML serialization/deserialization
- Dictionary conversion
- Filesystem instance creation
- Configuration updates

**Attributes:**

*   `protocol` (`str`): Storage protocol identifier (e.g., "s3", "gs", "file")

**Example:**

```python
from fsspeckit.storage_options.base import BaseStorageOptions

# Create and save options
options = BaseStorageOptions(protocol="s3")
options.to_yaml("config.yml")

# Load from YAML
loaded = BaseStorageOptions.from_yaml("config.yml")
print(loaded.protocol)
# 's3'
```

### `to_dict()`

Convert storage options to dictionary.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `with_protocol` | `bool` | Whether to include protocol in output dictionary |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `dict` | `dict` | Dictionary of storage options with non-None values |

**Example:**

```python
from fsspeckit.storage_options.base import BaseStorageOptions

options = BaseStorageOptions(protocol="s3")
print(options.to_dict())
# {}
print(options.to_dict(with_protocol=True))
# {'protocol': 's3'}
```

### `from_yaml()`

Load storage options from YAML file.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `path` | `str` | Path to YAML configuration file |
| `fs` | `AbstractFileSystem` | Filesystem to use for reading file |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `BaseStorageOptions` | `BaseStorageOptions` | Loaded storage options instance |

**Example:**

```python
# Load from local file
from fsspeckit.storage_options.base import BaseStorageOptions
from fsspec.implementations.local import LocalFileSystem

# Assuming 'config.yml' exists and contains valid YAML for BaseStorageOptions
# For example, a file named config.yml with content:
# protocol: s3
#
# To make this example runnable, we'll create a dummy config.yml
fs_local = LocalFileSystem()
fs_local.write_text("config.yml", "protocol: s3")

options = BaseStorageOptions.from_yaml("config.yml")
print(options.protocol)
# 's3'

# Clean up the dummy file
fs_local.rm("config.yml")
```

### `to_yaml()`

Save storage options to YAML file.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `path` | `str` | Path where to save configuration |
| `fs` | `AbstractFileSystem` | Filesystem to use for writing |

**Example:**

```python
from fsspeckit.storage_options.base import BaseStorageOptions
from fsspec.implementations.local import LocalFileSystem

options = BaseStorageOptions(protocol="s3")
fs_local = LocalFileSystem()
options.to_yaml("config.yml", fs=fs_local) # Specify filesystem for writing
```

### `to_filesystem()`

Create fsspec filesystem instance from options.

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `AbstractFileSystem` | `AbstractFileSystem` | Configured filesystem instance |

**Example:**

```python
from fsspeckit.storage_options.base import BaseStorageOptions

options = BaseStorageOptions(protocol="file")
fs = options.to_filesystem()
# Example usage: list files in a dummy directory
import os
import tempfile

with tempfile.TemporaryDirectory() as tmpdir:
    dummy_file_path = os.path.join(tmpdir, "test.txt")
    with open(dummy_file_path, "w") as f:
        f.write("dummy content")
    fs_temp = options.to_filesystem()
    files = fs_temp.ls(tmpdir)
    print(files)
```

### `update()`

Update storage options with new values.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `**kwargs` | `Any` | New option values to set |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `BaseStorageOptions` | `BaseStorageOptions` | Updated instance |

**Example:**

```python
from fsspeckit.storage_options.base import BaseStorageOptions

options = BaseStorageOptions(protocol="s3")
options = options.update(region="us-east-1")
print(options.region)
# 'us-east-1'
```