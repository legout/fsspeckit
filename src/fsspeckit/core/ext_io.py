"""Universal I/O helpers for fsspec filesystems.

This module contains universal interfaces that delegate to format-specific
helpers based on the file format, providing a unified API for reading and
writing data in various formats.
"""

from __future__ import annotations

import datetime as dt
import uuid
from typing import TYPE_CHECKING, Any, Generator

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa

from fsspec import AbstractFileSystem

# Import format-specific readers and writers
from fsspeckit.core.ext_json import read_json as _read_json_json
from fsspeckit.core.ext_csv import read_csv as _read_json_csv
from fsspeckit.core.ext_parquet import read_parquet as _read_json_parquet
from fsspeckit.core.ext_json import write_json as _write_json_format
from fsspeckit.core.ext_csv import write_csv as _write_csv_format
from fsspeckit.core.ext_parquet import write_parquet as _write_parquet_format
from fsspeckit.core.ext_parquet import write_pyarrow_dataset as _write_pyarrow_dataset
from fsspeckit.common.logging import get_logger

# Get module logger
logger = get_logger(__name__)


def read_files(
    self: AbstractFileSystem,
    path: str | list[str],
    format: str,
    batch_size: int | None = None,
    include_file_path: bool = False,
    concat: bool = True,
    jsonlines: bool = False,
    use_threads: bool = True,
    verbose: bool = False,
    opt_dtypes: bool = False,
    **kwargs: Any,
) -> (
    pl.DataFrame
    | pa.Table
    | list[pl.DataFrame]
    | list[pa.Table]
    | Generator[
        pl.DataFrame | pa.Table | list[pl.DataFrame] | list[pa.Table], None, None
    ]
):
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
    if format == "json":
        if batch_size is not None:
            return _read_json_json(
                self=self,
                path=path,
                batch_size=batch_size,
                include_file_path=include_file_path,
                jsonlines=jsonlines,
                concat=concat,
                use_threads=use_threads,
                verbose=verbose,
                opt_dtypes=opt_dtypes,
                **kwargs,
            )
        return _read_json_json(
            self=self,
            path=path,
            include_file_path=include_file_path,
            jsonlines=jsonlines,
            concat=concat,
            use_threads=use_threads,
            verbose=verbose,
            opt_dtypes=opt_dtypes,
            **kwargs,
        )
    elif format == "csv":
        if batch_size is not None:
            return _read_json_csv(
                self=self,
                path=path,
                batch_size=batch_size,
                include_file_path=include_file_path,
                concat=concat,
                use_threads=use_threads,
                verbose=verbose,
                opt_dtypes=opt_dtypes,
                **kwargs,
            )
        return _read_json_csv(
            self=self,
            path=path,
            include_file_path=include_file_path,
            use_threads=use_threads,
            concat=concat,
            verbose=verbose,
            opt_dtypes=opt_dtypes,
            **kwargs,
        )
    elif format == "parquet":
        if batch_size is not None:
            return _read_json_parquet(
                self=self,
                path=path,
                batch_size=batch_size,
                include_file_path=include_file_path,
                concat=concat,
                use_threads=use_threads,
                verbose=verbose,
                opt_dtypes=opt_dtypes,
                **kwargs,
            )
        return _read_json_parquet(
            self=self,
            path=path,
            include_file_path=include_file_path,
            use_threads=use_threads,
            concat=concat,
            verbose=verbose,
            opt_dtypes=opt_dtypes,
            **kwargs,
        )


def write_file(
    self,
    data: pl.DataFrame | pl.LazyFrame | pa.Table | pd.DataFrame | dict,
    path: str,
    format: str,
    **kwargs,
) -> None:
    """
    Write a DataFrame to a file in the given format.

    Args:
        data: (pl.DataFrame | pl.LazyFrame | pa.Table | pd.DataFrame) Data to write.
        path (str): Path to write the data.
        format (str): Format of the file.
        **kwargs: Additional keyword arguments.

    Returns:
        None
    """
    if format == "json":
        _write_json_format(self, data, path, **kwargs)
    elif format == "csv":
        _write_csv_format(self, data, path, **kwargs)
    elif format == "parquet":
        _write_parquet_format(self, data, path, **kwargs)


def write_files(
    self,
    data: (
        pl.DataFrame
        | pl.LazyFrame
        | pa.Table
        | pa.RecordBatch
        | pa.RecordBatchReader
        | pd.DataFrame
        | dict
        | list[
            pl.DataFrame
            | pl.LazyFrame
            | pa.Table
            | pa.RecordBatch
            | pa.RecordBatchReader
            | pd.DataFrame
            | dict
        ]
    ),
    path: str | list[str],
    basename: str = None,
    format: str = None,
    concat: bool = True,
    unique: bool | list[str] | str = False,
    mode: str = "append",  # append, overwrite, delete_matching, error_if_exists
    use_threads: bool = True,
    verbose: bool = False,
    **kwargs,
) -> None:
    """Write a DataFrame or a list of DataFrames to a file or a list of files.

    Args:
        data: (pl.DataFrame | pl.LazyFrame | pa.Table | pd.DataFrame | dict | list[pl.DataFrame | pl.LazyFrame |
            pa.Table | pd.DataFrame | dict]) Data to write.
        path: (str | list[str]) Path to write the data.
        basename: (str, optional) Basename of the files. Defaults to None.
        format: (str, optional) Format of the data. Defaults to None.
        concat: (bool, optional) If True, concatenate the DataFrames. Defaults to True.
        unique: (bool, optional) If True, remove duplicates. Defaults to False.
        mode: (str, optional) Write mode. Defaults to 'append'. Options: 'append', 'overwrite', 'delete_matching',
            'error_if_exists'.
        use_threads: (bool, optional) If True, use parallel processing. Defaults to True.
        verbose: (bool, optional) If True, print verbose output. Defaults to False.
        **kwargs: Additional keyword arguments.

    Returns:
        None

    Raises:
        FileExistsError: If file already exists and mode is 'error_if_exists'.
    """
    from fsspeckit.common.misc import run_parallel
    from fsspeckit.common.types import dict_to_dataframe
    from fsspeckit.common.optional import _import_pyarrow

    pa_mod = _import_pyarrow()

    if not isinstance(data, list):
        data = [data]

    if concat:
        if isinstance(data[0], dict):
            data = dict_to_dataframe(data)
        if isinstance(data[0], pl.LazyFrame):
            data = pl.concat([d.collect() for d in data], how="diagonal_relaxed")

        if isinstance(
            data[0], pa.Table | pa.RecordBatch | pa.RecordBatchReader | Generator
        ):
            data = pl.concat([pl.from_arrow(d) for d in data], how="diagonal_relaxed")
        elif isinstance(data[0], pd.DataFrame):
            data = pl.concat([pl.from_pandas(d) for d in data], how="diagonal_relaxed")

        if unique:
            data = data.unique(
                subset=None if not isinstance(unique, str | list) else unique,
                maintain_order=True,
            )

        data = [data]

    if format is None:
        format = (
            path[0].split(".")[-1]
            if isinstance(path, list) and "." in path[0]
            else path.split(".")[-1]
            if "." in path
            else "parquet"
        )

    def _write(d, p, basename, i):
        if f".{format}" not in p:
            if not basename:
                basename = f"data-{dt.datetime.now().strftime('%Y%m%d_%H%M%S%f')[:-3]}-{uuid.uuid4().hex[:16]}"
            p = f"{p}/{basename}-{i}.{format}"

        if mode == "delete_matching":
            write_file(self, d, p, format, **kwargs)
        elif mode == "overwrite":
            if self.exists(p):
                self.fs.rm(p, recursive=True)
            write_file(self, d, p, format, **kwargs)
        elif mode == "append":
            if not self.exists(p):
                write_file(self, d, p, format, **kwargs)
            else:
                p = p.replace(f".{format}", f"-{i}.{format}")
                write_file(self, d, p, format, **kwargs)
        elif mode == "error_if_exists":
            if self.exists(p):
                raise FileExistsError(f"File already exists: {p}")
            else:
                write_file(self, d, p, format, **kwargs)

    if mode == "overwrite":
        if isinstance(path, list):
            for p in path:
                # Remove existing files
                if self.exists(p):
                    self.rm(p, recursive=True)
        else:
            # Remove existing files
            if self.exists(path):
                self.rm(path, recursive=True)

    if use_threads:
        run_parallel(
            _write,
            d=data,
            p=path,
            basename=basename,
            i=list(range(len(data))),
            verbose=verbose,
        )
    else:
        for i, p in enumerate(path):
            _write(i, data, p, basename)
