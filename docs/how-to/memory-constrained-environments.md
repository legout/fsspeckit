# Memory-Constrained Environments

When working with large datasets on limited hardware, memory management is
critical. This guide shows how to use fsspeckit's memory monitoring, chunked
processing, and the PyArrow backend's streaming merge controls to stay within
bounds.

`MemoryMonitor` and `MemoryPressureLevel` require the `monitoring` extra, which
the `datasets` extra already includes. The handler operations require the
`datasets` extra. See the [extras matrix](../installation.md#optional-extras).

## What is monitored

`MemoryMonitor` tracks three signals and maps them to a `MemoryPressureLevel`:

- **PyArrow allocation**: bytes allocated by PyArrow's internal allocator.
- **Process RSS** (with `psutil`): total resident memory of the Python process.
- **System available** (with `psutil`): free memory available system-wide.

When `psutil` is not installed, only the PyArrow signal is reported and the
process/system thresholds are ignored. Install `psutil` (via the `monitoring`
or `datasets` extra) to enable the full set.

## Setting up a monitor

```python
from fsspeckit.datasets.pyarrow import MemoryMonitor

monitor = MemoryMonitor(
    max_pyarrow_mb=1024,        # cap on PyArrow allocation
    max_process_memory_mb=2048, # optional cap on total process RSS
    min_system_available_mb=512 # floor on free system memory
)

status = monitor.get_memory_status()
print(f"PyArrow: {status['pyarrow_allocated_mb']:.1f} MB")
```

Each threshold maps to a pressure level (`NORMAL`, `WARNING`, `CRITICAL`,
`EMERGENCY`) at fixed ratios: below 70% is normal, 70 to 90% is warning, above
90% is critical, and exceeding a hard limit is emergency. Check the level
between work units:

```python
from fsspeckit.datasets.pyarrow import MemoryPressureLevel

pressure = monitor.check_memory_pressure()
if pressure == MemoryPressureLevel.EMERGENCY:
    raise MemoryError(monitor.get_detailed_status())
```

`should_check_memory(chunks_processed, check_interval)` is a convenience that
returns true every `check_interval` chunks, so you can throttle how often you
sample pressure on high-throughput loops.

## Chunked processing

`process_in_chunks()` yields a PyArrow dataset or table in fixed-size slices
while enforcing a memory ceiling. It creates its own `MemoryMonitor` if you do
not pass one:

```python
import pyarrow.dataset as ds
from fsspeckit.datasets.pyarrow.dataset import process_in_chunks

dataset = ds.dataset("large_dataset/")

for chunk in process_in_chunks(
    dataset,
    chunk_size_rows=50_000,
    max_memory_mb=512,
):
    # only one chunk is materialized at a time
    process(chunk)
```

If pressure reaches `EMERGENCY`, `process_in_chunks()` raises `MemoryError` with
the detailed status, so a runaway loop fails fast instead of triggering an OOM
kill.

## Streaming merge controls

The PyArrow backend's `merge()` honors streaming controls that bound peak
memory during incremental rewrites. The DuckDB backend accepts the same
parameters for compatibility but ignores them.

```python
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO

io = PyarrowDatasetIO()

result = io.merge(
    source,
    "dataset/",
    strategy="upsert",
    key_columns=["id"],
    enable_streaming_merge=True,
    merge_chunk_size_rows=100_000,
    merge_max_memory_mb=512,
    merge_min_system_available_mb=256,
)
```

| Parameter | Effect |
|-----------|--------|
| `enable_streaming_merge` | Stream merge output instead of materializing it |
| `merge_chunk_size_rows` | Rows processed per streaming chunk |
| `merge_max_memory_mb` | Cap on PyArrow memory during the merge |
| `merge_max_process_memory_mb` | Optional cap on total process RSS |
| `merge_min_system_available_mb` | Floor on free system memory |
| `merge_progress_callback` | Optional `callback(processed, total)` |

For the full parameter list, see
[Dataset Handlers](../dataset-handlers.md).

## Adaptive key tracking in merges

`PyarrowDatasetIO.merge()` uses an `AdaptiveKeyTracker` internally to track
source keys during incremental rewrites. The tracker escalates through exact,
LRU, and Bloom tiers as cardinality grows, bounding memory automatically. You
do not need to configure it for normal merges; for high-cardinality workloads,
see [Adaptive Key Tracking](adaptive-key-tracking.md) and the
[Adaptive Key Tracking reference](../reference/adaptive-key-tracking.md).

## Choosing limits

A practical starting point for a constrained environment (for example, a 4 GB
container):

```python
monitor = MemoryMonitor(
    max_pyarrow_mb=512,
    max_process_memory_mb=1024,
    min_system_available_mb=256,
)
```

For cloud containers, size the limits from the container memory limit, leaving
headroom for the system and other processes. See the
[Adaptive Key Tracking reference](../reference/adaptive-key-tracking.md) for
tiered memory guidance.

## Related documentation

- [Adaptive Key Tracking](adaptive-key-tracking.md) - tiered key tracking.
- [Dataset Handlers](../dataset-handlers.md) - streaming merge parameters.
- [Optimize Performance](optimize-performance.md) - caching and parallelism.
- [Generated API: fsspeckit.datasets.pyarrow](../api/fsspeckit.datasets.pyarrow.md) -
  `MemoryMonitor` signatures.
