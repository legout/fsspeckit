# Memory Monitoring Performance

This page has been retired as an active guide. Memory monitoring guidance is
now consolidated in a single place to avoid competing instructions.

## Where to go instead

For practical memory tuning - choosing limits, chunked processing, and the
streaming merge controls on the PyArrow backend - see
[Memory-Constrained Environments](memory-constrained-environments.md).

For the reference surface, see:

- [Adaptive Key Tracking](../reference/adaptive-key-tracking.md) - the
  `MemoryMonitor` and `MemoryPressureLevel` contract, plus tiered key tracking.
- [Dataset Handlers](../dataset-handlers.md) - the streaming merge parameters
  (`merge_max_memory_mb`, `merge_max_process_memory_mb`,
  `merge_min_system_available_mb`) and which backend honors them.
- [Generated API: fsspeckit.datasets.pyarrow](../api/fsspeckit.datasets.pyarrow.md) -
  exact signatures for `MemoryMonitor`.

## The short version

`MemoryMonitor` (requires the `monitoring` extra, which `datasets` already
includes) reports PyArrow allocation, process RSS, and system availability, and
maps them to a `MemoryPressureLevel`. Configure the three limits once and call
`check_memory_pressure()` between chunks. The full configuration walkthrough
lives on the consolidated page above.

```python
from fsspeckit.datasets.pyarrow import MemoryMonitor

monitor = MemoryMonitor(
    max_pyarrow_mb=1024,
    max_process_memory_mb=2048,
    min_system_available_mb=512,
)
status = monitor.get_memory_status()
```
