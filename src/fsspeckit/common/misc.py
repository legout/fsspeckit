"""Miscellaneous utility functions for fsspeckit."""

import importlib.util
import os
import posixpath
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Callable



from fsspec import AbstractFileSystem
from fsspec.implementations.dirfs import DirFileSystem

# Import canonical optional dependency checker
from fsspeckit.common.optional import check_optional_dependency
from fsspeckit.common.logging import get_logger

logger = get_logger(__name__)


if importlib.util.find_spec("joblib"):
    from rich.progress import BarColumn, Progress, TextColumn, TimeElapsedColumn

    def _prepare_parallel_args(
        args: tuple, kwargs: dict
    ) -> tuple[list, list, dict, dict, int]:
        """Prepare and validate arguments for parallel execution.

        Args:
            args: Positional arguments
            kwargs: Keyword arguments

        Returns:
            tuple: (iterables, fixed_args, iterable_kwargs, fixed_kwargs, first_iterable_len)

        Raises:
            ValueError: If no iterable arguments or length mismatch
        """
        from collections.abc import Iterable

        iterables = []
        fixed_args = []
        iterable_kwargs = {}
        fixed_kwargs = {}
        first_iterable_len = None

        # Process positional arguments
        for arg in args:
            # Accept any non-string Iterable (including generators)
            if isinstance(arg, Iterable) and not isinstance(arg, (str, bytes)):
                # Convert to list to materialize generators and get length
                materialized_arg = list(arg)
                iterables.append(materialized_arg)
                if first_iterable_len is None:
                    first_iterable_len = len(materialized_arg)
                elif len(materialized_arg) != first_iterable_len:
                    raise ValueError("All iterables must have the same length")
            else:
                fixed_args.append(arg)

        # Process keyword arguments
        for key, value in kwargs.items():
            # Accept any non-string Iterable (including generators)
            if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
                # Convert to list to materialize generators and get length
                materialized_value = list(value)
                if first_iterable_len is None:
                    first_iterable_len = len(materialized_value)
                elif len(materialized_value) != first_iterable_len:
                    raise ValueError("All iterables must have the same length")
                iterable_kwargs[key] = materialized_value
            else:
                fixed_kwargs[key] = value

        if first_iterable_len is None:
            raise ValueError("At least one iterable argument must be provided")

        return iterables, fixed_args, iterable_kwargs, fixed_kwargs, first_iterable_len

    def _execute_parallel_with_progress(
        func: Callable,
        iterables: list,
        fixed_args: list,
        iterable_kwargs: dict,
        fixed_kwargs: dict,
        param_combinations: list,
        parallel_kwargs: dict,
    ) -> list:
        """Execute parallel tasks with progress tracking.

        Args:
            func: Function to execute
            iterables: List of iterable arguments
            fixed_args: List of fixed arguments
            iterable_kwargs: Dictionary of iterable keyword arguments
            fixed_kwargs: Dictionary of fixed keyword arguments
            param_combinations: List of parameter combinations
            parallel_kwargs: Parallel execution configuration

        Returns:
            list: Results from parallel execution
        """
        from rich.progress import BarColumn, Progress, TextColumn, TimeElapsedColumn

        results = [None] * len(param_combinations)
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TimeElapsedColumn(),
            transient=True,
        ) as progress:
            task = progress.add_task(
                "Running in parallel...", total=len(param_combinations)
            )

            def wrapper(idx, param_tuple):
                res = func(
                    *(list(param_tuple[: len(iterables)]) + fixed_args),
                    **{
                        k: v
                        for k, v in zip(
                            iterable_kwargs.keys(), param_tuple[len(iterables) :]
                        )
                    },
                    **fixed_kwargs,
                )
                progress.update(task, advance=1)
                return idx, res

            from fsspeckit.common.optional import _import_joblib

            joblib_module = _import_joblib()
            from joblib import Parallel, delayed

            for idx, result in Parallel(**parallel_kwargs)(
                delayed(wrapper)(i, param_tuple)
                for i, param_tuple in enumerate(param_combinations)
            ):
                results[idx] = result
        return results

    def _execute_parallel_without_progress(
        func: Callable,
        iterables: list,
        fixed_args: list,
        iterable_kwargs: dict,
        fixed_kwargs: dict,
        param_combinations: list,
        parallel_kwargs: dict,
    ) -> list:
        """Execute parallel tasks without progress tracking.

        Args:
            func: Function to execute
            iterables: List of iterable arguments
            fixed_args: List of fixed arguments
            iterable_kwargs: Dictionary of iterable keyword arguments
            fixed_kwargs: Dictionary of fixed keyword arguments
            param_combinations: List of parameter combinations
            parallel_kwargs: Parallel execution configuration

        Returns:
            list: Results from parallel execution
        """
        from joblib import Parallel, delayed
        return Parallel(**parallel_kwargs)(
            delayed(func)(
                *(list(param_tuple[: len(iterables)]) + fixed_args),
                **{
                    k: v
                    for k, v in zip(
                        iterable_kwargs.keys(), param_tuple[len(iterables) :]
                    )
                },
                **fixed_kwargs,
            )
            for param_tuple in param_combinations
        )

    def run_parallel(
        func: Callable,
        *args,
        n_jobs: int = -1,
        backend: str = "threading",
        verbose: bool = True,
        **kwargs,
    ) -> list[Any]:
        """Runs a function for a list of parameters in parallel.

        Requires: fsspeckit[datasets] extra for joblib dependency.

        Args:
            func (Callable): function to run in parallel
            *args: Positional arguments. Can be single values or any non-string iterables (including generators)
            n_jobs (int, optional): Number of joblib workers. Defaults to -1
            backend (str, optional): joblib backend. Valid options are
                `loky`,`threading`,`multiprocessing` or `sequential`. Defaults to "threading"
            verbose (bool, optional): Show progress bar. Defaults to True
            **kwargs: Keyword arguments. Can be single values or any non-string iterables (including generators)

        Returns:
            list[any]: Function output

        Raises:
            ImportError: If joblib is not available. Install with: pip install fsspeckit[datasets]
            ValueError: If no iterable arguments are provided or iterables have different lengths

        Examples:
            >>> # Single iterable argument
            >>> run_parallel(func, [1,2,3], fixed_arg=42)

            >>> # Multiple iterables in args and kwargs
            >>> run_parallel(func, [1,2,3], val=[7,8,9], fixed=42)

            >>> # Only kwargs iterables
            >>> run_parallel(func, x=[1,2,3], y=[4,5,6], fixed=42)

            >>> # Generator support
            >>> def gen():
            ...     yield from [1, 2, 3]
            >>> run_parallel(str, gen())  # Returns ['1', '2', '3']
        """
        if backend == "threading" and n_jobs == -1:
            n_jobs = min(256, (os.cpu_count() or 1) + 4)

        parallel_kwargs = {"n_jobs": n_jobs, "backend": backend, "verbose": 0}

        # Prepare and validate arguments
        iterables, fixed_args, iterable_kwargs, fixed_kwargs, first_iterable_len = (
            _prepare_parallel_args(args, kwargs)
        )

        # Create parameter combinations
        all_iterables = iterables + list(iterable_kwargs.values())

        # Handle empty iterables case
        if first_iterable_len == 0:
            return []

        param_combinations = list(zip(*all_iterables))

        # Execute with or without progress tracking
        if not verbose:
            return _execute_parallel_without_progress(
                func,
                iterables,
                fixed_args,
                iterable_kwargs,
                fixed_kwargs,
                param_combinations,
                parallel_kwargs,
            )
        else:
            return _execute_parallel_with_progress(
                func,
                iterables,
                fixed_args,
                iterable_kwargs,
                fixed_kwargs,
                param_combinations,
                parallel_kwargs,
            )

else:

    def run_parallel(*args, **kwargs):
        raise ImportError("joblib not installed")


def get_partitions_from_path(
    path: str, partitioning: str | list | None = None
) -> dict[str, str]:
    """Extract dataset partitions from a file path.

    Parses file paths to extract partition information based on
    different partitioning schemes. By default, uses Hive-style partitioning.

    Args:
        path: File path potentially containing partition information.
        partitioning: Partitioning scheme:
            - "hive": Hive-style partitioning (key=value)
            - str: Single partition column name
            - list[str]: Multiple partition column names
            - None: Default to Hive-style partitioning

    Returns:
        Dictionary mapping partition keys to their values.

    Examples:
        >>> # Default Hive-style partitioning
        >>> get_partitions_from_path("data/year=2023/month=01/file.parquet")
        {'year': '2023', 'month': '01'}

        >>> # Explicit Hive-style partitioning
        >>> get_partitions_from_path("data/year=2023/month=01/file.parquet", "hive")
        {'year': '2023', 'month': '01'}

        >>> # Single partition column
        >>> get_partitions_from_path("data/2023/01/file.parquet", "year")
        {'year': '2023'}

        >>> # Multiple partition columns
        >>> get_partitions_from_path("data/2023/01/file.parquet", ["year", "month"])
        {'year': '2023', 'month': '01'}
    """
    # Normalize path to handle Windows and relative paths
    normalized_path = Path(path).as_posix().replace("\\", "/")

    # Remove filename if present
    if "." in normalized_path:
        normalized_path = str(Path(normalized_path).parent)

    parts = normalized_path.split("/")

    # Default to Hive-style partitioning when partitioning is None
    if partitioning is None or partitioning == "hive":
        partitions = {}
        for part in parts:
            if "=" in part:
                key, value = part.split("=", 1)  # Split only on first =
                partitions[key] = value
        return partitions
    elif isinstance(partitioning, str):
        # Single partition column
        return {partitioning: parts[0]} if parts else {}
    elif isinstance(partitioning, list):
        # Multiple partition columns
        result = {}
        for i, col_name in enumerate(partitioning):
            if i < len(parts):
                result[col_name] = parts[-(len(partitioning) - i)]
        return result
    else:
        return {}


def path_to_glob(path: str, format: str | None = None) -> str:
    """Convert a path to a glob pattern for file matching.

    Intelligently converts paths to glob patterns that match files of the specified
    format, handling various directory and wildcard patterns.

    Args:
        path: Base path to convert. Can include wildcards (* or **).
            Examples: "data/", "data/*.json", "data/**"
        format: File format to match (without dot). If None, inferred from path.
            Examples: "json", "csv", "parquet"

    Returns:
        str: Glob pattern that matches files of specified format.
            Examples: "data/**/*.json", "data/*.csv"

    Example:
        ```python
        # Basic directory
        print(path_to_glob("data", "json"))
        # 'data/**/*.json'

        # With wildcards
        print(path_to_glob("data/**", "csv"))
        # 'data/**/*.csv'

        # Format inference
        print(path_to_glob("data/file.parquet"))
        # 'data/file.parquet'
        ```
    """
    path = path.rstrip("/")
    if format is None:
        if ".json" in path:
            format = "json"
        elif ".csv" in path:
            format = "csv"
        elif ".parquet" in path:
            format = "parquet"

    if format is not None and format in path:
        return path
    else:
        if path.endswith("**"):
            return posixpath.join(path, f"*.{format}")
        elif path.endswith("*"):
            if path.endswith("*/*"):
                return path + f".{format}"
            return posixpath.join(path.rstrip("/*"), f"*.{format}")
        return posixpath.join(path, f"**/*.{format}")


# Removed duplicate check_optional_dependency function - use fsspeckit.common.optional.check_optional_dependency instead


def check_fs_identical(fs1: AbstractFileSystem, fs2: AbstractFileSystem) -> bool:
    """Check if two fsspec filesystems are identical.

    Args:
        fs1: First filesystem (fsspec AbstractFileSystem)
        fs2: Second filesystem (fsspec AbstractFileSystem)

    Returns:
        bool: True if filesystems are identical, False otherwise
    """

    def _get_root_fs(fs: AbstractFileSystem) -> AbstractFileSystem:
        while hasattr(fs, "fs"):
            fs = fs.fs
        return fs

    fs1 = _get_root_fs(fs1)
    fs2 = _get_root_fs(fs2)
    return fs1 == fs2


def sync_files(
    add_files: list[str],
    delete_files: list[str],
    src_fs: AbstractFileSystem,
    dst_fs: AbstractFileSystem,
    src_path: str = "",
    dst_path: str = "",
    server_side: bool = False,
    chunk_size: int = 8 * 1024 * 1024,
    parallel: bool = False,
    n_jobs: int = -1,
    verbose: bool = True,
) -> dict[str, list[str]]:
    """Sync files between two filesystems by copying new files and deleting old ones.

    Args:
        add_files: List of file paths to add (copy from source to destination)
        delete_files: List of file paths to delete from destination
        src_fs: Source filesystem (fsspec AbstractFileSystem)
        dst_fs: Destination filesystem (fsspec AbstractFileSystem)
        src_path: Base path in source filesystem. Default is root ('').
        dst_path: Base path in destination filesystem. Default is root ('').
        server_side: Whether to use server-side copy if supported. Default is False.
        chunk_size: Size of chunks to read/write files (in bytes). Default is 8MB.
        parallel: Whether to perform copy/delete operations in parallel. Default is False.
        n_jobs: Number of parallel jobs if parallel=True. Default is -1 (all cores).
        verbose: Whether to show progress bars. Default is True.

    Returns:
        dict: Summary of added and deleted files
    """
    CHUNK = chunk_size
    RETRIES = 3

    server_side = check_fs_identical(src_fs, dst_fs) and server_side

    src_mapper = src_fs.get_mapper(src_path)
    dst_mapper = dst_fs.get_mapper(dst_path)

    def server_side_copy_file(key, src_mapper, dst_mapper, RETRIES):
        last_exc = None
        for attempt in range(1, RETRIES + 1):
            try:
                dst_mapper[key] = src_mapper[key]
                break
            except (OSError, IOError) as e:
                last_exc = e
                if attempt == RETRIES:
                    logger.error(
                        "Failed to copy file %s after %d attempts: %s",
                        key,
                        RETRIES,
                        str(e),
                        exc_info=True,
                    )
                    raise RuntimeError(f"Failed to copy file {key} after {RETRIES} attempts") from e
            except Exception as e:
                last_exc = e
                if attempt == RETRIES:
                    logger.error(
                        "Unexpected error copying file %s: %s",
                        key,
                        str(e),
                        exc_info=True,
                    )
                    raise RuntimeError(f"Unexpected error copying file {key}: {e}") from e

    def copy_file(key, src_fs, dst_fs, src_path, dst_path, CHUNK, RETRIES):
        last_exc = None
        for attempt in range(1, RETRIES + 1):
            try:
                with (
                    src_fs.open(posixpath.join(src_path, key), "rb") as r,
                    dst_fs.open(posixpath.join(dst_path, key), "wb") as w,
                ):
                    while True:
                        chunk = r.read(CHUNK)
                        if not chunk:
                            break
                        w.write(chunk)
                break
            except (OSError, IOError) as e:
                last_exc = e
                if attempt == RETRIES:
                    logger.error(
                        "Failed to copy file %s after %d attempts: %s",
                        key,
                        RETRIES,
                        str(e),
                        exc_info=True,
                    )
                    raise RuntimeError(f"Failed to copy file {key} after {RETRIES} attempts") from e
            except Exception as e:
                last_exc = e
                if attempt == RETRIES:
                    logger.error(
                        "Unexpected error copying file %s: %s",
                        key,
                        str(e),
                        exc_info=True,
                    )
                    raise RuntimeError(f"Unexpected error copying file {key}: {e}") from e

    def delete_file(key, dst_fs, dst_path, RETRIES):
        last_exc = None
        for attempt in range(1, RETRIES + 1):
            try:
                dst_fs.rm(posixpath.join(dst_path, key))
                break
            except (OSError, IOError) as e:
                last_exc = e
                if attempt == RETRIES:
                    logger.error(
                        "Failed to delete file %s after %d attempts: %s",
                        key,
                        RETRIES,
                        str(e),
                        exc_info=True,
                    )
                    raise RuntimeError(f"Failed to delete file {key} after {RETRIES} attempts") from e
            except Exception as e:
                last_exc = e
                if attempt == RETRIES:
                    logger.error(
                        "Unexpected error deleting file %s: %s",
                        key,
                        str(e),
                        exc_info=True,
                    )
                    raise RuntimeError(f"Unexpected error deleting file {key}: {e}") from e

    if len(add_files):
        # Copy new files
        if parallel:
            if server_side:
                try:
                    run_parallel(
                        server_side_copy_file,
                        add_files,
                        src_mapper=src_mapper,
                        dst_mapper=dst_mapper,
                        RETRIES=RETRIES,
                        n_jobs=n_jobs,
                        verbose=verbose,
                    )
                except (RuntimeError, OSError) as e:
                    logger.warning(
                        "Server-side copy failed for some files, falling back to client-side: %s",
                        str(e),
                    )
                    # Fallback to client-side copy if server-side fails
                    run_parallel(
                        copy_file,
                        add_files,
                        src_fs=src_fs,
                        dst_fs=dst_fs,
                        src_path=src_path,
                        dst_path=dst_path,
                        CHUNK=CHUNK,
                        RETRIES=RETRIES,
                        n_jobs=n_jobs,
                        verbose=verbose,
                    )

            else:
                run_parallel(
                    copy_file,
                    add_files,
                    src_fs=src_fs,
                    dst_fs=dst_fs,
                    src_path=src_path,
                    dst_path=dst_path,
                    CHUNK=CHUNK,
                    RETRIES=RETRIES,
                    n_jobs=n_jobs,
                    verbose=verbose,
                )
        else:
            if verbose:
                from rich.progress import track

                for key in track(
                    add_files,
                    description="Copying new files...",
                    total=len(add_files),
                ):
                    if server_side:
                        try:
                            server_side_copy_file(key, src_mapper, dst_mapper, RETRIES)
                        except (RuntimeError, OSError):
                            copy_file(
                                key, src_fs, dst_fs, src_path, dst_path, CHUNK, RETRIES
                            )
                    else:
                        copy_file(
                            key, src_fs, dst_fs, src_path, dst_path, CHUNK, RETRIES
                        )
            else:
                for key in add_files:
                    if server_side:
                        try:
                            server_side_copy_file(key, src_mapper, dst_mapper, RETRIES)
                        except (RuntimeError, OSError):
                            copy_file(
                                key, src_fs, dst_fs, src_path, dst_path, CHUNK, RETRIES
                            )
                    else:
                        copy_file(
                            key, src_fs, dst_fs, src_path, dst_path, CHUNK, RETRIES
                        )

    if len(delete_files):
        # Delete old files from destination
        if parallel:
            run_parallel(
                delete_file,
                delete_files,
                dst_fs=dst_fs,
                dst_path=dst_path,
                RETRIES=RETRIES,
                n_jobs=n_jobs,
                verbose=verbose,
            )
        else:
            if verbose:
                from rich.progress import track

                for key in track(
                    delete_files,
                    description="Deleting stale files...",
                    total=len(delete_files),
                ):
                    delete_file(key, dst_fs, dst_path, RETRIES)
            else:
                for key in delete_files:
                    delete_file(key, dst_fs, dst_path, RETRIES)

    return {"added_files": add_files, "deleted_files": delete_files}


def sync_dir(
    src_fs: AbstractFileSystem,
    dst_fs: AbstractFileSystem,
    src_path: str = "",
    dst_path: str = "",
    server_side: bool = True,
    chunk_size: int = 8 * 1024 * 1024,
    parallel: bool = False,
    n_jobs: int = -1,
    verbose: bool = True,
) -> dict[str, list[str]]:
    """Sync two directories between different filesystems.

    Compares files in the source and destination directories, copies new or updated files from source to destination,
    and deletes stale files from destination.

    Args:
        src_fs: Source filesystem (fsspec AbstractFileSystem)
        dst_fs: Destination filesystem (fsspec AbstractFileSystem)
        src_path: Path in source filesystem to sync. Default is root ('').
        dst_path: Path in destination filesystem to sync. Default is root ('').
        chunk_size: Size of chunks to read/write files (in bytes). Default is 8MB.
        parallel: Whether to perform copy/delete operations in parallel. Default is False.
        n_jobs: Number of parallel jobs if parallel=True. Default is -1 (all cores).
        verbose: Whether to show progress bars. Default is True.

    Returns:
        dict: Summary of added and deleted files
    """

    src_mapper = src_fs.get_mapper(src_path)
    dst_mapper = dst_fs.get_mapper(dst_path)

    add_files = sorted(src_mapper.keys() - dst_mapper.keys())
    delete_files = sorted(dst_mapper.keys() - src_mapper.keys())

    return sync_files(
        add_files=add_files,
        delete_files=delete_files,
        src_fs=src_fs,
        dst_fs=dst_fs,
        src_path=src_path,
        dst_path=dst_path,
        chunk_size=chunk_size,
        server_side=server_side,
        parallel=parallel,
        n_jobs=n_jobs,
        verbose=verbose,
    )
