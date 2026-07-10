# Work with Filesystems

This guide covers creating and using filesystems with fsspeckit: protocol
inference, path confinement, caching, and the extended I/O methods registered
on every filesystem instance.

For exact factory signatures, see
[fsspeckit.core](../api/fsspeckit.core.md). For backend-specific dataset
operations, see [Dataset Handlers](../dataset-handlers.md).

## Creating a filesystem

Use `filesystem()` when you want protocol inference, storage-options objects,
caching, or `DirFileSystem` path confinement. Use `get_filesystem()` for a
thinner factory when you already know the protocol and options.

```python
from fsspeckit import filesystem

# Local filesystem (wrapped in a DirFileSystem at the cwd by default)
fs = filesystem(".")

# Protocol inferred from the URI
s3_fs = filesystem("s3://bucket/path")

# Manual protocol with a dict of options
s3_fs = filesystem("s3", storage_options={"region": "us-east-1"})
```

To pass structured credentials, build an options object and call `to_dict()`.
Cloud options classes require their extra - see
[Configure Cloud Storage](configure-cloud-storage.md) and the
[extras matrix](../installation.md#optional-extras).

```python
from fsspeckit import filesystem, AwsStorageOptions

options = AwsStorageOptions(region="us-east-1")
fs = filesystem("s3://my-bucket/", storage_options=options.to_dict())
```

## Path safety with DirFileSystem

By default `filesystem()` wraps the underlying filesystem in a fsspec
`DirFileSystem` (`dirfs=True`), confining every operation to a base directory.
Attempts to reach a path outside that root are rejected.

```python
from fsspeckit import filesystem

fs = filesystem("/data/allowed", dirfs=True)

fs.ls("subdir")            # works: resolved under /data/allowed
fs.open("file.txt", "r")   # works: resolved under /data/allowed

try:
    fs.open("../../../etc/passwd", "r")
except (ValueError, PermissionError) as e:
    print(f"Blocked: {e}")
```

For explicit control, construct a `DirFileSystem` directly, or pass an existing
filesystem as `base_fs` to nest confinement. This is the basis for safe
multi-tenant isolation, where each tenant gets a filesystem rooted at its own
directory.

```python
from fsspeckit import filesystem, DirFileSystem

base = filesystem("file")
confined = DirFileSystem(fs=base, path="/data/allowed")
```

## Caching

Caching stores frequently accessed remote data locally. Enable it with
`cached=True`, optionally pointing `cache_storage` at fast local storage:

```python
from fsspeckit import filesystem

fs = filesystem("s3://bucket/", cached=True, cache_storage="/tmp/cache")

# First read downloads and caches; subsequent reads hit the cache
data = fs.cat("large_file.parquet")
```

Clear a filesystem's cache with `clear_cache()`. Local filesystems do not
benefit from caching, so leave it off (the default) for local work.

## Extended I/O methods

Importing `fsspeckit` registers rich format-specific I/O methods on every
fsspec `AbstractFileSystem`. Once you have a filesystem, these methods are
available directly.

### JSON

```python
# Single file: returns a dict (or list for JSON Lines)
data = fs.read_json_file("config.json")

# Multiple files (glob): returns a Polars DataFrame by default
df = fs.read_json("data/*.json")

# Batch processing with threading
for batch in fs.read_json("data/*.json", batch_size=5, use_threads=True):
    process(batch)

fs.write_json(df, "output.json")
```

### CSV

```python
df = fs.read_csv_file("data.csv")
df = fs.read_csv("data/*.csv", concat=True)          # combine multiple files
df = fs.read_csv("data/*.csv", opt_dtypes=True)      # optimize dtypes
fs.write_csv(df, "output.csv")
```

### Parquet

```python
# Single file: returns a PyArrow Table
table = fs.read_parquet_file("data.parquet")

# Multiple files, schema-unified and concatenated
table = fs.read_parquet("data/*.parquet", concat=True)

# Batch reading for large datasets
for batch in fs.read_parquet("data/*.parquet", batch_size=20):
    process(batch)

# Include the source file path as a column
table = fs.read_parquet("data/*.parquet", include_file_path=True)

fs.write_parquet(table, "output.parquet")
```

### Universal reader and writer

`read_files()` dispatches to a format-specific reader; pass `format`
explicitly (`"json"`, `"csv"`, or `"parquet"`). `write_files()` writes
multiple files in parallel:

```python
df = fs.read_files("data/*.csv", format="csv", use_threads=True)
df = fs.read_files("data/*.json", format="json", concat=True)

fs.write_files(
    data=[table1, table2],
    path=["out1.parquet", "out2.parquet"],
    format="parquet",
    use_threads=True,
)
```

For the full parameter lists, see
[fsspeckit.core](../api/fsspeckit.core.md).

## Basic file and directory operations

The standard fsspec operations work as expected on any fsspeckit filesystem:

```python
# Read and write
with fs.open("output.txt", "w") as f:
    f.write("Hello, World!")

# Listing and metadata
files = fs.ls("/path/to/dir")
files = fs.ls("/path/", detail=True)
exists = fs.exists("/path/to/file")
info = fs.info("/path/to/file")

# Directories
fs.makedirs("/new/dir", exist_ok=True)
```

## Related documentation

- [Configure Cloud Storage](configure-cloud-storage.md) - provider setup.
- [Read and Write Datasets](read-and-write-datasets.md) - dataset handler
  operations.
- [API Guide](../reference/api-guide.md) - import selection.
