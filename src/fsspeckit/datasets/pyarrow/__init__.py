"""PyArrow dataset integration for fsspeckit.

This package contains focused submodules for PyArrow functionality:
- dataset: Dataset merge and maintenance operations
- io: PyarrowDatasetIO class for dataset operations

All public APIs are re-exported here for convenient access.
"""

# Re-export dataset creation helpers (filesystem-level).
from fsspeckit.core.ext.dataset import (
    pyarrow_dataset,
    pyarrow_parquet_dataset,
)

# Re-export dataset operations
from .dataset import (
    collect_dataset_stats_pyarrow,
    compact_parquet_dataset_pyarrow,
    optimize_parquet_dataset_pyarrow,
)

# Re-export memory monitoring and tracking utilities
from .memory import (
    MemoryMonitor,
    MemoryPressureLevel,
)
from .adaptive_tracker import AdaptiveKeyTracker

# Re-export dataset I/O class
from .io import (
    PyarrowDatasetIO,
)

__all__ = [
    # Dataset operations
    "collect_dataset_stats_pyarrow",
    "compact_parquet_dataset_pyarrow",
    "optimize_parquet_dataset_pyarrow",
    "pyarrow_dataset",
    "pyarrow_parquet_dataset",
    # Memory monitoring and tracking
    "MemoryMonitor",
    "MemoryPressureLevel",
    "AdaptiveKeyTracker",
    # Dataset I/O class
    "PyarrowDatasetIO",
]
