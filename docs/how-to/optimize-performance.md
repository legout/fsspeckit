# Optimize Performance

This guide covers the main performance levers in fsspeckit: filesystem caching,
parallel file I/O, parallel custom processing, and type optimization.

For exact signatures, see [fsspeckit.core](../api/fsspeckit.core.md) and
[fsspeckit.common](../api/fsspeckit.common.md).

## Filesystem caching

Caching stores remote data locally so repeated reads avoid round trips. Enable
it with `cached=True`, and point `cache_storage` at fast local storage when
possible:

```python
from fsspeckit import filesystem

fs = filesystem("s3://data/", cached=True, cache_storage="/ssd/cache")

# First read downloads and caches; the second hits the cache
fs.cat("large_file.parquet")
fs.cat("large_file.parquet")
```

- Cache remote filesystems; local filesystems gain nothing from it.
- Use fast local storage (NVMe/SSD) for `cache_storage` on read-heavy
  workloads.
- Leave caching off for write-heavy remote paths.
- Clear a cache with `fs.clear_cache()`.

For the cache configuration parameters, see
[fsspeckit.core](../api/fsspeckit.core.md).

## Parallel file I/O

The extended read methods accept `use_threads` to read multiple files
concurrently:

```python
df = fs.read_csv("data/*.csv", use_threads=True)
df = fs.read_json("data/*.json", use_threads=True)
table = fs.read_parquet("data/*.parquet", use_threads=True)
```

`use_threads` helps when reading many files; for a single file it has little
effect. Set `use_threads=False` when debugging or when parallelism causes
contention.

For very large sets of files, read in batches with `batch_size` so only one
batch is in memory at a time:

```python
for batch in fs.read_parquet("data/*.parquet", batch_size=50, use_threads=True):
    process(batch)
```

## Parallel custom processing

`run_parallel()` runs a function over one or more iterables concurrently using
joblib. Iterable arguments are zipped together; non-iterable arguments are
passed fixed to every call. It shows a progress bar by default.

```python
from fsspeckit.common import run_parallel

def process_file(path, *, output_dir):
    # per-file work
    return result

results = run_parallel(
    process_file,
    file_list,                 # iterable: one value per call
    output_dir="/out",         # fixed: same for every call
    n_jobs=8,                  # worker count (-1 = all cores)
    backend="threading",       # joblib backend
)
```

`n_jobs=-1` (the default) uses all cores for the threading backend. Choose
`backend="threading"` for I/O-bound work and `backend="loky"` or
`"multiprocessing"` for CPU-bound work. `joblib` ships as a core dependency.

## Batched dataset processing

To process a parquet dataset in memory-bounded chunks, iterate a PyArrow
dataset with `process_in_chunks`, which yields `pa.Table` slices and enforces
a memory ceiling via a `MemoryMonitor`:

```python
import pyarrow.dataset as ds
from fsspeckit.datasets.pyarrow.dataset import process_in_chunks

dataset = ds.dataset("large_dataset/")

for chunk in process_in_chunks(
    dataset,
    chunk_size_rows=500_000,
    max_memory_mb=2048,
):
    process(chunk)
```

This requires the `datasets` extra. See the
[extras matrix](../installation.md#optional-extras), and
[Memory-Constrained Environments](memory-constrained-environments.md) for
configuring the monitor.

## Type optimization

Optimizing column dtypes reduces memory and speeds up downstream processing.
The extended read methods accept `opt_dtypes=True` to optimize on read:

```python
df = fs.read_csv("data/*.csv", opt_dtypes=True)
```

For explicit control over schema and types, use the schema utilities in
`fsspeckit.datasets`. These require the `datasets` extra (or `polars` for the
Polars variants):

```python
from fsspeckit.datasets import cast_schema, convert_large_types_to_normal, opt_dtype_pa

# Cast a table to a target schema
casted = cast_schema(table, target_schema)

# Narrow large Arrow types (large_string -> string, etc.)
narrowed = convert_large_types_to_normal(table)

# Infer optimized dtypes
optimized = opt_dtype_pa(table)
```

For Polars type optimization, use `opt_dtype_pl` (requires the `polars` extra).
See the [Public API Inventory](../reference/public-api-inventory.md) for the
full symbol list.

## Dataset maintenance

Compacting small files into fewer larger ones improves read performance. Use
the coordinator-backed filesystem façade; it returns a typed result and exposes
an explicit plan when review is needed.

```python
from fsspeckit import filesystem

fs = filesystem("file")
summary = fs.compact_parquet_dataset("dataset/", target_mb_per_file=128)
```

See [Coordinator-backed Maintenance](../migration/maintenance-api.md) for the
planning workflow and guarantee levels, and
[Maintain Parquet Datasets](maintain-parquet-datasets.md) for the full task
guide including deduplication, repartitioning, and optimization.

## Related documentation

- [Memory-Constrained Environments](memory-constrained-environments.md) -
  memory monitoring and streaming merge.
- [Dataset Handlers](../dataset-handlers.md) - maintenance and backend options.
- [Work with Filesystems](work-with-filesystems.md) - caching and extended I/O.
