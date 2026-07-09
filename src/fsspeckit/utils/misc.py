"""Miscellaneous utilities façade.

DEPRECATED: This module exists only for backwards compatibility.
New code should import from:
- fsspeckit.common.parallel for run_parallel
- fsspeckit.common.sync for sync_dir and sync_files
- fsspeckit.common.partitions for get_partitions_from_path
- rich.progress for Progress class

This module re-exports symbols that were previously available at deeper
import paths like fsspeckit.utils.misc.Progress.

DEPRECATION WARNING:
Importing from fsspeckit.utils.misc is deprecated and will be removed
in a future major version. Please update imports to:
- fsspeckit.common.parallel for run_parallel
- fsspeckit.common.sync for sync_dir and sync_files
- fsspeckit.common.partitions for get_partitions_from_path
- rich.progress for Progress class

Supported re-exports:
- get_partitions_from_path
- run_parallel
- sync_dir
- sync_files
- Progress (from rich.progress)
"""

# Re-export from canonical location
from fsspeckit.common.partitions import get_partitions_from_path
from fsspeckit.common.parallel import run_parallel
from fsspeckit.common.sync import sync_dir, sync_files

# Re-export Progress from rich for backwards compatibility
from rich.progress import Progress

__all__ = [
    "get_partitions_from_path",
    "run_parallel",
    "sync_dir",
    "sync_files",
    "Progress",
]
