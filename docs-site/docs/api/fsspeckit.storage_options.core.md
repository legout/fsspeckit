# `fsspeckit.storage_options.core` API Reference

## `LocalStorageOptions`

Local filesystem configuration options.

Provides basic configuration for local file access. While this class is simple, it maintains consistency with other storage options and enables transparent switching between local and remote storage.

**Attributes:**

* `protocol` (`str`): Always "file" for local filesystem
* `auto_mkdir` (`bool`): Create directories automatically
* `mode` (`int`): Default file creation mode (unix-style)

**Example:**
```python
# Basic local access
options = LocalStorageOptions()
fs = options.to_filesystem()
files = fs.ls("/path/to/data")

# With auto directory creation
options = LocalStorageOptions(auto_mkdir=True)
fs = options.to_filesystem()
with fs.open("/new/path/file.txt", "w") as f:
    f.write("test")  # Creates /new/path/ automatically
```

### `to_fsspec_kwargs()`

Convert options to fsspec filesystem arguments.

**Returns:**

- `dict`: Arguments suitable for LocalFileSystem

**Example:**
```python
options = LocalStorageOptions(auto_mkdir=True)
kwargs = options.to_fsspec_kwargs()
fs = filesystem("file", **kwargs)
```

## `from_dict()`

Create appropriate storage options instance from dictionary.

Factory function that creates the correct storage options class based on protocol.

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `protocol` | `str` | Storage protocol identifier (e.g., "s3", "gs", "file") |
| `storage_options` | `dict` | Dictionary of configuration options |

**Returns:**

- `BaseStorageOptions`: Appropriate storage options instance

**Raises:**

- `ValueError`: If protocol is not supported

**Example:**
```python
# Create S3 options
options = from_dict("s3", {
    "access_key_id": "KEY",
    "secret_access_key": "SECRET"
})
print(type(options).__name__)
```

## `from_env()`

Create storage options from environment variables.

Factory function that creates and configures storage options from protocol-specific environment variables.

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `protocol` | `str` | Storage protocol identifier (e.g., "s3", "github") |

**Returns:**

- `BaseStorageOptions`: Configured storage options instance

**Raises:**

- `ValueError`: If protocol is not supported

**Example:**
```python
# With AWS credentials in environment
options = from_env("s3")
print(options.access_key_id)
```

## `infer_protocol_from_uri()`

Infer the storage protocol from a URI string.

Analyzes the URI to determine the appropriate storage protocol based on the scheme or path format.

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `uri` | `str` | URI or path string to analyze. |

Typical examples include:

- `\"s3://bucket/path\"`
- `\"gs://bucket/path\"`
- `\"github://org/repo\"`
- `\"/local/path\"`

**Returns:**

- `str`: Inferred protocol identifier

**Example:**
```python
# S3 protocol
infer_protocol_from_uri("s3://my-bucket/data")

# Local file
infer_protocol_from_uri("/home/user/data")

# GitHub repository
infer_protocol_from_uri("github://microsoft/vscode")
```

## `storage_options_from_uri()`

Create storage options instance from a URI string.

Infers the protocol and extracts relevant configuration from the URI to create appropriate storage options.

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `uri` | `str` | URI string containing protocol and optional configuration. |

Typical examples include:

- `\"s3://bucket/path\"`
- `\"gs://project/bucket/path\"`
- `\"github://org/repo\"`

**Returns:**

- `BaseStorageOptions`: Configured storage options instance

**Example:**
```python
# S3 options
opts = storage_options_from_uri("s3://my-bucket/data")
print(opts.protocol)

# GitHub options
opts = storage_options_from_uri("github://microsoft/vscode")
print(opts.org)
print(opts.repo)
```

## `merge_storage_options()`

Merge multiple storage options into a single configuration.

Combines options from multiple sources with control over precedence.

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `*options` | `BaseStorageOptions` or `dict` | Storage options to merge. |
| `overwrite` | `bool` | Whether later options override earlier ones |

Each entry in `*options` may be:

- A `BaseStorageOptions` instance
- A dictionary of options
- `None` (ignored)

**Returns:**

- `BaseStorageOptions`: Combined storage options

**Example:**
```python
# Merge with overwrite
base = AwsStorageOptions(
    region="us-east-1",
    access_key_id="OLD_KEY"
)
override = {"access_key_id": "NEW_KEY"}
merged = merge_storage_options(base, override)
print(merged.access_key_id)

# Preserve existing values
merged = merge_storage_options(
    base,
    override,
    overwrite=False
)
print(merged.access_key_id)
```

## `StorageOptions`

High-level storage options container and factory.

Provides a unified interface for creating and managing storage options for different protocols.

**Attributes:**

- `storage_options` (`BaseStorageOptions`): Underlying storage options instance

**Example:**
```python
# Create from protocol
options = StorageOptions.create(
    protocol="s3",
    access_key_id="KEY",
    secret_access_key="SECRET"
)

# Create from existing options
s3_opts = AwsStorageOptions(access_key_id="KEY")
options = StorageOptions(storage_options=s3_opts)
```

### `create()`

Create storage options from arguments.

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `**data` | `dict` | Keyword arguments describing either protocol/configuration or a pre-configured instance. |

Accepted patterns:

- `protocol=...` plus configuration fields
- `storage_options=<BaseStorageOptions instance>`

**Returns:**

- `StorageOptions`: Configured storage options instance

**Raises:**

- `ValueError`: If protocol missing or invalid

**Example:**
```python
# Direct protocol config
options = StorageOptions.create(
    protocol="s3",
    region="us-east-1"
)
```

### `from_yaml()`

Create storage options from YAML configuration.

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `path` | `str` | Path to YAML configuration file |
| `fs` | `fsspec.AbstractFileSystem`, optional | Filesystem for reading configuration |

**Returns:**

- `StorageOptions`: Configured storage options

**Example:**
```python
# Load from config file
options = StorageOptions.from_yaml("storage.yml")
print(options.storage_options.protocol)
```

### `from_env()`

Create storage options from environment variables.

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `protocol` | `str` | Storage protocol to configure |

**Returns:**

- `StorageOptions`: Environment-configured options

**Example:**
```python
# Load AWS config from environment
options = StorageOptions.from_env("s3")
```

### `to_filesystem()`

Create fsspec filesystem instance.

**Returns:**

- `AbstractFileSystem`: Configured filesystem instance

**Example:**
```python
options = StorageOptions.create(protocol="file")
fs = options.to_filesystem()
files = fs.ls("/data")
```

### `to_dict()`

Convert storage options to dictionary.

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `with_protocol` | `bool` | Whether to include protocol in output |

**Returns:**

- `dict`: Storage options as dictionary

**Example:**
```python
options = StorageOptions.create(
    protocol="s3",
    region="us-east-1"
)
print(options.to_dict())
```

### `to_object_store_kwargs()`

Get options formatted for object store clients.

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `with_conditional_put` | `bool` | Add etag-based conditional put support |

**Returns:**

- `dict`: Object store configuration dictionary

**Example:**
```python
options = StorageOptions.create(protocol="s3")
kwargs = options.to_object_store_kwargs()
# store = ObjectStore(**kwargs)
```

## `BaseStorageOptions`

Base class for filesystem storage configuration options.

Provides common functionality for all storage option classes including: - YAML serialization/deserialization - Dictionary conversion - Filesystem instance creation - Configuration updates

**Attributes:**

- `protocol` (`str`): Storage protocol identifier (e.g., "s3", "gs", "file")

**Example:**
```python
# Create and save options
options = BaseStorageOptions(protocol="s3")
options.to_yaml("config.yml")

# Load from YAML
loaded = BaseStorageOptions.from_yaml("config.yml")
print(loaded.protocol)
```

### `to_dict()`

Convert storage options to dictionary.

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `with_protocol` | `bool` | Whether to include protocol in output dictionary |

**Returns:**

- `dict`: Dictionary of storage options with non-None values

**Example:**
```python
options = BaseStorageOptions(protocol="s3")
print(options.to_dict())
print(options.to_dict(with_protocol=True))
```

### `from_yaml()`

Load storage options from YAML file.

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `path` | `str` | Path to YAML configuration file |
| `fs` | `fsspec.AbstractFileSystem`, optional | Filesystem to use for reading file |

**Returns:**

- `BaseStorageOptions`: Loaded storage options instance

**Example:**
```python
# Load from local file
options = BaseStorageOptions.from_yaml("config.yml")
print(options.protocol)
```

### `to_yaml()`

Save storage options to YAML file.

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `path` | `str` | Path where to save configuration |
| `fs` | `fsspec.AbstractFileSystem`, optional | Filesystem to use for writing |

**Example:**
```python
options = BaseStorageOptions(protocol="s3")
options.to_yaml("config.yml")
```

### `to_filesystem()`

Create fsspec filesystem instance from options.

**Returns:**

- `AbstractFileSystem`: Configured filesystem instance

**Example:**
```python
options = BaseStorageOptions(protocol="file")
fs = options.to_filesystem()
files = fs.ls("/path/to/data")
```

### `update()`

Update storage options with new values.

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `**kwargs` | `dict` | New option values to set |

**Returns:**

- `BaseStorageOptions`: Updated instance

**Example:**
```python
options = BaseStorageOptions(protocol="s3")
options = options.update(region="us-east-1")
print(options.region)
```
