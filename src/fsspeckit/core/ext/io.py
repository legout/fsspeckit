"""Universal I/O helpers for fsspec filesystems.

This module contains universal interfaces that delegate to format-specific
helpers based on the file format, providing a unified API for reading and
writing data in various formats.
"""

from __future__ import annotations

import datetime as dt
import uuid
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass  # Type checking imports moved to individual functions as needed

from fsspec import AbstractFileSystem

# Import lazy helpers for optional dependencies
from fsspeckit.common.optional import _import_polars

# Import format-specific readers and writers
from fsspeckit.core.ext.json import read_json as _read_json_json
from fsspeckit.core.ext.csv import read_csv as _read_json_csv
from fsspeckit.core.ext.parquet import read_parquet as _read_json_parquet
from fsspeckit.core.ext.json import write_json as _write_json_format
from fsspeckit.core.ext.csv import write_csv as _write_csv_format
from fsspeckit.core.ext.parquet import write_parquet as _write_parquet_format
from fsspeckit.common.logging import get_logger

# Get module logger
logger = get_logger(__name__)

# Format dispatch mappings
READ_HANDLERS = {
    "json": _read_json_json,
    "csv": _read_json_csv,
    "parquet": _read_json_parquet,
}

WRITE_HANDLERS = {
    "json": _write_json_format,
    "csv": _write_csv_format,
    "parquet": _write_parquet_format,
}


def read_files(
    self: AbstractFileSystem,
    path: str | list[str],
    format: str,
    batch_size: int | None = None,
    include_file_path: bool = False,
    concat: bool = True,
    jsonlines: bool = False,
    use_threads: bool = False,
    verbose: bool = False,
    opt_dtypes: bool = False,
    **kwargs: Any,
) -> Any:
    """Universal interface for reading data files of any supported format.

    A unified API that automatically delegates to the appropriate reading function
    based on file format, while preserving all advanced features like:
    - Batch processing
    - Parallel reading
    - File path tracking
    - Format-specific optimizations

    Args:
        path: Path(s) to data file(s). Can be:
            - Single path string (globs supported)
            - List of path strings
        format: File format to read. Supported values:
            - "json": Regular JSON or JSON Lines
            - "csv": CSV files
            - "parquet": Parquet files
        batch_size: If set, enables batch reading with this many files per batch
        include_file_path: Add source filepath as column/field
        concat: Combine multiple files/batches into single result
        jsonlines: For JSON format, whether to read as JSON Lines
        use_threads: Enable parallel file reading
        verbose: Print progress information
        opt_dtypes: Optimize DataFrame/Arrow Table dtypes for performance
        **kwargs: Additional format-specific arguments

    Returns:
        Various types depending on format and arguments:
        - pl.DataFrame: For CSV and optionally JSON
        - pa.Table: For Parquet
        - list[pl.DataFrame | pa.Table]: Without concatenation
        - Generator: If batch_size set, yields batches

    Example:
        ```python
        fs = LocalFileSystem()

        # Read CSV files
        df = fs.read_files(
            "data/*.csv",
            format="csv",
            include_file_path=True,
        )
        print(type(df))
        # <class 'polars.DataFrame'>

        # Batch process Parquet files
        for batch in fs.read_files(
            "data/*.parquet",
            format="parquet",
            batch_size=100,
            use_threads=True,
        ):
            print(f"Batch type: {type(batch)}")

        # Read JSON Lines
        df = fs.read_files(
            "logs/*.jsonl",
            format="json",
            jsonlines=True,
            concat=True,
        )
        print(df.columns)
        ```
    """
    if format not in READ_HANDLERS:
        raise ValueError(
            f"Unsupported format: {format}. Supported formats: "
            f"{list(READ_HANDLERS.keys())}"
        )

    handler = READ_HANDLERS[format]

    # Prepare common arguments
    common_args = {
        "self": self,
        "path": path,
        "include_file_path": include_file_path,
        "concat": concat,
        "use_threads": use_threads,
        "verbose": verbose,
        "opt_dtypes": opt_dtypes,
        **kwargs,
    }

    # Add format-specific arguments
    if format == "json":
        common_args["jsonlines"] = jsonlines

    if batch_size is not None:
        common_args["batch_size"] = batch_size

    return handler(**common_args)


def write_file(
    self,
    data: Any,
    path: str,
    format: str,
    **kwargs,
) -> None:
    """
    Write a DataFrame to a file in the given format.

    Args:
        data: Data to write (polars DataFrame/LazyFrame, pyarrow Table,
            pandas DataFrame, or dict).
        path (str): Path to write the data.
        format (str): Format of the file.
        **kwargs: Additional keyword arguments.

    Returns:
        None
    """
    if format not in WRITE_HANDLERS:
        raise ValueError(
            f"Unsupported format: {format}. Supported formats: "
            f"{list(WRITE_HANDLERS.keys())}"
        )

    handler = WRITE_HANDLERS[format]
    handler(self, data, path, **kwargs)


def write_files(
    self,
    data: Any,
    path: str | list[str],
    basename: str = None,
    format: str = None,
    concat: bool = True,
    unique: bool | list[str] | str = False,
    mode: str = "append",  # append, overwrite, delete_matching, error_if_exists
    use_threads: bool = False,
    verbose: bool = False,
    **kwargs,
) -> None:
    """Write a DataFrame or a list of DataFrames to a file or a list of files.

    Args:
        data: Data to write. Can be a single item or list of items
            (polars DataFrame/LazyFrame, pyarrow Table/RecordBatch/RecordBatchReader,
            pandas DataFrame, or dict).
        path: Path to write the data. Can be a single path string or list of paths.
        basename: (str, optional) Basename of the files. Defaults to None.
        format: (str, optional) Format of the data. Defaults to None.
        concat: (bool, optional) If True, concatenate the DataFrames. Defaults to True.
        unique: (bool, optional) If True, remove duplicates. Defaults to False.
        mode: (str, optional) Write mode. Defaults to 'append'. Options:
            'append', 'overwrite', 'delete_matching', 'error_if_exists'.
        use_threads: (bool, optional) If True, use parallel processing.
            Defaults to True.
        verbose: (bool, optional) If True, print verbose output. Defaults to False.
        **kwargs: Additional keyword arguments.

    Returns:
        None

    Raises:
        FileExistsError: If file already exists and mode is 'error_if_exists'.
    """
    from fsspeckit.common.misc import run_parallel
    from fsspeckit.common.types import dict_to_dataframe

    # Import polars for type checking and data manipulation
    pl = _import_polars()

    # Normalize data to a list
    if not isinstance(data, list):
        data = [data]

    # Handle concatenation
    if concat:
        if isinstance(data[0], dict):
            data = dict_to_dataframe(data)
        # Import polars to check for LazyFrame
        pl = _import_polars()
        if isinstance(data[0], pl.LazyFrame):
            data = pl.concat([d.collect() for d in data], how="diagonal_relaxed")

        # Check for pyarrow types - we need to import them lazily
        try:
            from fsspeckit.common.optional import _import_pyarrow

            pa = _import_pyarrow()

            if isinstance(data[0], (pa.Table, pa.RecordBatch, pa.RecordBatchReader)):
                data = pl.concat(
                    [pl.from_arrow(d) for d in data], how="diagonal_relaxed"
                )
        except ImportError:
            pass  # PyArrow not available, skip this path

        # Check for pandas
        try:
            from fsspeckit.common.optional import _import_pandas

            pd = _import_pandas()

            if isinstance(data[0], pd.DataFrame):
                data = pl.concat(
                    [pl.from_pandas(d) for d in data], how="diagonal_relaxed"
                )
        except ImportError:
            pass  # Pandas not available, skip this path

        if unique:
            data = data.unique(
                subset=None if not isinstance(unique, str | list) else unique,
                maintain_order=True,
            )

        data = [data]

    # Determine format if not specified
    if format is None:
        format = (
            path[0].split(".")[-1]
            if isinstance(path, list) and "." in path[0]
            else path.split(".")[-1]
            if isinstance(path, str) and "." in path
            else "parquet"
        )

    # Normalize path to a list
    if isinstance(path, str):
        path = [path]

    # Ensure data and path lists have compatible lengths
    if len(data) != len(path):
        if len(data) == 1:
            # Single data item, replicate to match path length
            data = data * len(path)
        elif len(path) == 1:
            # Single path, replicate to match data length
            path = path * len(data)
        else:
            raise ValueError(
                f"Data and path lists must have compatible lengths. "
                f"Got {len(data)} data items and {len(path)} paths."
            )

    def _write(
        data_item: Any, path_item: str, basename: str | None, index: int
    ) -> None:
        """Write a single data item to a single path."""
        # Add format extension if missing
        if f".{format}" not in path_item:
            if not basename:
                basename = (
                    f"data-{dt.datetime.now().strftime('%Y%m%d_%H%M%S%f')[:-3]}-"
                    f"{uuid.uuid4().hex[:16]}"
                )
            path_item = f"{path_item}/{basename}-{index}.{format}"

        # Handle different write modes
        if mode == "delete_matching":
            write_file(self, data_item, path_item, format, **kwargs)
        elif mode == "overwrite":
            if self.exists(path_item):
                self.fs.rm(path_item, recursive=True)
            write_file(self, data_item, path_item, format, **kwargs)
        elif mode == "append":
            if not self.exists(path_item):
                write_file(self, data_item, path_item, format, **kwargs)
            else:
                path_item = path_item.replace(f".{format}", f"-{index}.{format}")
                write_file(self, data_item, path_item, format, **kwargs)
        elif mode == "error_if_exists":
            if self.exists(path_item):
                raise FileExistsError(f"File already exists: {path_item}")
            else:
                write_file(self, data_item, path_item, format, **kwargs)

    # Handle overwrite mode pre-processing
    if mode == "overwrite":
        for p in path:
            if self.exists(p):
                self.rm(p, recursive=True)

    # Execute writes
    if use_threads:
        # For parallel execution, pass iterables as positional args
        # and fixed values as kwargs
        run_parallel(
            _write,
            data,
            path,
            basename=basename,
            index=list(range(len(data))),
            verbose=verbose,
        )
    else:
        for i, (data_item, path_item) in enumerate(zip(data, path)):
            _write(data_item, path_item, basename, i)
