# Sync and Manage Files

This guide covers synchronizing files and directories between filesystems, plus
partition-aware file management. The sync and partition helpers live in
`fsspeckit.common` and require no extra.

For exact signatures, see
[fsspeckit.common](../api/fsspeckit.common.md).

## Directory synchronization

`sync_dir()` compares the path listings of a source and destination directory
on two filesystems, copies source-only files from source to destination, and
deletes stale destination-only files. Files present in both locations are left
untouched, so in-place updates to a matching path are not re-copied. It
returns a summary of what changed.

```python
from fsspeckit import filesystem
from fsspeckit.common import sync_dir

src_fs = filesystem("/local/data/")
dst_fs = filesystem("s3://my-bucket/")

summary = sync_dir(
    src_fs=src_fs,
    dst_fs=dst_fs,
    src_path="",
    dst_path="",
    parallel=True,
    n_jobs=4,
)

print(f"Added: {len(summary['added_files'])}")
print(f"Deleted: {len(summary['deleted_files'])}")
```

`sync_dir()` derives the add and delete lists from the two directory listings,
then delegates to `sync_files()`. Paths are relative to each filesystem's
configured root, so a `DirFileSystem` automatically confines the operation.

## File-level synchronization

When you already know which files to add and delete, call `sync_files()`
directly:

```python
from fsspeckit.common import sync_files

summary = sync_files(
    add_files=["file1.txt", "file2.txt"],
    delete_files=["obsolete.txt"],
    src_fs=src_fs,
    dst_fs=dst_fs,
    src_path="source/",
    dst_path="target/",
    verbose=True,
)
```

### Copy modes

- **server_side** (default `True` on `sync_dir`, `False` on `sync_files`): when
  both filesystems are the same remote store, copy objects server-side without
  downloading them.
- **parallel** and **n_jobs**: when `parallel=True`, copy and delete operations
  run concurrently with `n_jobs` workers (defaults to all cores).
- **chunk_size**: bytes read and written per chunk for client-side copies
  (default 8 MB).

When `server_side=True` but the two filesystems are not the same store, the
operation falls back to a client-side copy automatically.

## Cross-cloud sync

Because each filesystem is configured independently, you can sync between
different providers. Build each filesystem with its options, then point
`sync_dir()` at them. Cloud options require their extra - see
[Configure Cloud Storage](configure-cloud-storage.md) and the
[extras matrix](../installation.md#optional-extras).

```python
from fsspeckit import filesystem
from fsspeckit.storage_options import from_env

src_fs = filesystem("s3://source-bucket/", storage_options=from_env("s3").to_dict())
dst_fs = filesystem("gs://dest-bucket/", storage_options=from_env("gs").to_dict())

sync_dir(src_fs=src_fs, dst_fs=dst_fs, parallel=True)
```

## Partition management

`get_partitions_from_path()` extracts partition key/value pairs from a path.
Pass `partitioning="hive"` for hive-style `key=value` directories; it returns a
`list[tuple[str, str]]`. Use it to filter or group files by partition before
syncing.

```python
from fsspeckit.common import get_partitions_from_path

path = "data/year=2023/month=01/day=15/file.parquet"
print(get_partitions_from_path(path, partitioning="hive"))
# [('year', '2023'), ('month', '01'), ('day', '15')]
```

Combined with a directory listing, this lets you sync only specific partitions.
Wrap the result in `dict()` to look up a column by name:

```python
def sync_year(src_fs, dst_fs, base, year):
    for entry in src_fs.find(base, directories=True, files=False):
        parts = dict(get_partitions_from_path(entry, partitioning="hive"))
        if parts.get("year") == str(year):
            sync_dir(src_fs=src_fs, dst_fs=dst_fs, src_path=entry)
```

## Parallel custom operations

For arbitrary per-file work, `run_parallel()` runs a function over one or more
iterables concurrently using joblib. Iterable arguments are zipped; non-iterable
arguments are passed fixed to every call.

```python
from fsspeckit.common import run_parallel

def copy_one(path, *, dst):
    ...

results = run_parallel(copy_one, file_list, dst=dst_path, n_jobs=4)
```

`run_parallel` relies on `joblib`, which ships as a core dependency.

## Related documentation

- [Work with Filesystems](work-with-filesystems.md) - filesystem creation and
  path confinement.
- [Optimize Performance](optimize-performance.md) - parallelism and caching.
- [Generated API: fsspeckit.common](../api/fsspeckit.common.md) - signatures.
