"""CSV file I/O helpers for fsspec filesystems.

This module contains functions for reading and writing CSV files with support for:
- Single file and batch reading
- Parallel processing
- DataFrame conversion with Polars
- Dtype optimization
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generator

if TYPE_CHECKING:
    import polars as pl
    import pandas as pd
    import pyarrow as pa

from fsspec import AbstractFileSystem

# Import lazy helpers for optional dependencies
from fsspeckit.common.optional import _import_polars, _import_pandas, _import_pyarrow

from fsspeckit.common.misc import path_to_glob, run_parallel
from fsspeckit.common.logging import get_logger

# Get module logger
logger = get_logger(__name__)


def _read_csv_file(
    path: str,
    self: AbstractFileSystem,
    include_file_path: bool = False,
    opt_dtypes: bool = False,
    **kwargs: Any,
) -> pl.DataFrame:
    """Read a single CSV file from any filesystem.

    Internal function that handles reading individual CSV files and optionally
    adds the source filepath as a column.

    Args:
        path: Path to CSV file
        self: Filesystem instance to use for reading
        include_file_path: Add source filepath as a column
        opt_dtypes: Optimize DataFrame dtypes
        **kwargs: Additional arguments passed to pl.read_csv()

    Returns:
        pl.DataFrame: DataFrame containing CSV data

    Raises:
        FileNotFoundError: If the CSV file does not exist
        PermissionError: If permission is denied to read the file
        OSError: For system-level I/O errors
        ValueError: If the file cannot be parsed as CSV

    Example:
        ```python
        fs = LocalFileSystem()
        df = _read_csv_file(
            "data.csv",
            fs,
            include_file_path=True,
            delimiter="|",
        )
        print("file_path" in df.columns)
        # True
        ```
    """
    # Import polars lazily
    pl = _import_polars()

    # Try to import polars utilities, but don't fail if they're not available
    try:
        from fsspeckit.common.polars import opt_dtype as opt_dtype_pl
    except ImportError:
        opt_dtype_pl = None

    operation = "read CSV"
    context = {"path": path, "operation": operation}

    try:
        with self.open(path) as f:
            df = pl.read_csv(f, **kwargs)
        logger.debug("Successfully read CSV: {path}", **context)

        if include_file_path:
            df = df.with_columns(pl.lit(path).alias("file_path"))
        if opt_dtypes and opt_dtype_pl is not None:
            df = opt_dtype_pl(df, strict=False)
        return df

    except FileNotFoundError as e:
        logger.error("File not found during {operation}: {path}", **context)
        raise FileNotFoundError(f"File not found during {operation}: {path}") from e
    except PermissionError as e:
        logger.error("Permission denied during {operation}: {path}", **context)
        raise PermissionError(f"Permission denied during {operation}: {path}") from e
    except OSError as e:
        logger.error(
            "System error during {operation}: {path} - {error}",
            **{**context, "error": str(e)},
        )
        raise OSError(f"System error during {operation}: {path} - {e}") from e
    except ValueError as e:
        logger.error(
            "Invalid CSV format in {path}: {error}", **{**context, "error": str(e)}
        )
        raise ValueError(f"Invalid CSV format in {path}: {e}") from e
    except Exception as e:
        logger.error(
            "Unexpected error during {operation}: {path} - {error}",
            **{**context, "error": str(e)},
            exc_info=True,
        )
        raise


def read_csv_file(
    self, path: str, include_file_path: bool = False, opt_dtypes: bool = False, **kwargs
) -> pl.DataFrame:
    return _read_csv_file(
        path=path,
        self=self,
        include_file_path=include_file_path,
        opt_dtypes=opt_dtypes,
        **kwargs,
    )


def _read_csv(
    self,
    path: str | list[str],
    include_file_path: bool = False,
    use_threads: bool = False,
    concat: bool = True,
    verbose: bool = False,
    opt_dtypes: bool = False,
    **kwargs,
) -> pl.DataFrame | list[pl.DataFrame]:
    """
    Read a CSV file or a list of CSV files into a polars DataFrame.

    Args:
        path: (str | list[str]) Path to the CSV file(s).
        include_file_path: (bool, optional) If True, return a DataFrame with a
            'file_path' column.
            Defaults to False.
        use_threads: (bool, optional) If True, read files in parallel. Defaults to True.
        concat: (bool, optional) If True, concatenate the DataFrames.
            Defaults to True.
        verbose: (bool, optional) If True, print verbose output. Defaults to False.
        opt_dtypes: (bool, optional) If True, optimize DataFrame dtypes.
            Defaults to False.
        **kwargs: Additional keyword arguments.

    Returns:
        (pl.DataFrame | list[pl.DataFrame]): Polars DataFrame or list of DataFrames.
    """
    # Import polars lazily
    pl = _import_polars()

    # Handle path resolution and determine if we have multiple files
    if isinstance(path, str):
        path = path_to_glob(path, format="csv")
        path = self.glob(path)

    # Determine if we have multiple files and process accordingly
    if isinstance(path, list) and len(path) > 1:
        # Multiple files case
        if use_threads:
            dfs = run_parallel(
                _read_csv_file,
                path,
                self=self,
                include_file_path=include_file_path,
                opt_dtypes=opt_dtypes,
                n_jobs=-1,
                backend="threading",
                verbose=verbose,
                **kwargs,
            )
        else:
            dfs = [
                _read_csv_file(
                    p,
                    self=self,
                    include_file_path=include_file_path,
                    opt_dtypes=opt_dtypes,
                    **kwargs,
                )
                for p in path
            ]
    else:
        # Single file case
        single_path = path[0] if isinstance(path, list) else path
        dfs = _read_csv_file(
            single_path,
            self=self,
            include_file_path=include_file_path,
            opt_dtypes=opt_dtypes,
            **kwargs,
        )

    # Handle concatenation - ensure consistent structure
    if not isinstance(dfs, list):
        dfs = [dfs]  # Convert single DataFrame to list for consistent handling

    if concat:
        result = pl.concat(dfs, how="diagonal_relaxed")
        # if opt_dtypes:
        #    result = opt_dtype_pl(result, strict=False)
        return result
    else:
        return dfs


def _read_csv_batches(
    self: AbstractFileSystem,
    path: str | list[str],
    batch_size: int | None = None,
    include_file_path: bool = False,
    concat: bool = True,
    use_threads: bool = False,
    verbose: bool = False,
    opt_dtypes: bool = False,
    **kwargs: Any,
) -> Generator[pl.DataFrame | list[pl.DataFrame], None, None]:
    """Process CSV files in batches with optional parallel reading.

    Internal generator function that handles batched reading of CSV files
    with support for parallel processing within each batch.

    Args:
        path: Path(s) to CSV file(s). Glob patterns supported.
        batch_size: Number of files to process in each batch
        include_file_path: Add source filepath as a column
        concat: Combine files within each batch
        use_threads: Enable parallel file reading within batches
        verbose: Print progress information
        opt_dtypes: Optimize DataFrame dtypes
        **kwargs: Additional arguments passed to pl.read_csv()

    Yields:
        Each batch of data in requested format:
        - pl.DataFrame: Single DataFrame if concat=True
        - list[pl.DataFrame]: List of DataFrames if concat=False

    Example:
        >>> fs = LocalFileSystem()
        >>> # Process large dataset in batches
        >>> for batch in fs._read_csv_batches(
        ...     "data/*.csv",
        ...     batch_size=100,
        ...     include_file_path=True,
        ...     verbose=True
        ... ):
        ...     print(f"Batch columns: {batch.columns}")
        >>>
        >>> # Parallel processing without concatenation
        >>> for batch in fs._read_csv_batches(
        ...     ["file1.csv", "file2.csv"],
        ...     batch_size=1,
        ...     concat=False,
        ...     use_threads=True
        ... ):
        ...     for df in batch:
        ...         print(f"DataFrame shape: {df.shape}")
    """
    # Import polars lazily
    pl = _import_polars()

    # Handle path resolution
    if isinstance(path, str):
        path = path_to_glob(path, format="csv")
        path = self.glob(path)

    # Ensure path is a list
    if isinstance(path, str):
        path = [path]

    # Process files in batches
    for i in range(0, len(path), batch_size):
        batch_paths = path[i : i + batch_size]

        # Read batch with optional parallelization
        if use_threads and len(batch_paths) > 1:
            batch_dfs = run_parallel(
                _read_csv_file,
                batch_paths,
                self=self,
                include_file_path=include_file_path,
                n_jobs=-1,
                backend="threading",
                verbose=verbose,
                opt_dtypes=opt_dtypes,
                **kwargs,
            )
        else:
            batch_dfs = [
                _read_csv_file(
                    p,
                    self=self,
                    include_file_path=include_file_path,
                    opt_dtypes=opt_dtypes,
                    **kwargs,
                )
                for p in batch_paths
            ]

        # if opt_dtypes:
        #    batch_dfs = [opt_dtype_pl(df, strict=False) for df in batch_dfs]

        if concat and len(batch_dfs) > 1:
            result = pl.concat(batch_dfs, how="diagonal_relaxed")
            # if opt_dtypes:
            #    result = opt_dtype_pl(result, strict=False)
            yield result
        else:
            yield batch_dfs


def read_csv(
    self: AbstractFileSystem,
    path: str | list[str],
    batch_size: int | None = None,
    include_file_path: bool = False,
    concat: bool = True,
    use_threads: bool = False,
    verbose: bool = False,
    opt_dtypes: bool = False,
    **kwargs: Any,
) -> (
    pl.DataFrame
    | list[pl.DataFrame]
    | Generator[pl.DataFrame | list[pl.DataFrame], None, None]
):
    """Read CSV data from one or more files with powerful options.

    Provides a flexible interface for reading CSV files with support for:
    - Single file or multiple files
    - Batch processing for large datasets
    - Parallel processing
    - File path tracking
    - Polars DataFrame output

    Args:
        path: Path(s) to CSV file(s). Can be:
            - Single path string (globs supported)
            - List of path strings
        batch_size: If set, enables batch reading with this many files per batch
        include_file_path: Add source filepath as a column
        concat: Combine multiple files/batches into single DataFrame
        use_threads: Enable parallel file reading
        verbose: Print progress information
        **kwargs: Additional arguments passed to pl.read_csv()

    Returns:
        Various types depending on arguments:
        - pl.DataFrame: Single or concatenated DataFrame
        - list[pl.DataFrame]: List of DataFrames (if concat=False)
        - Generator: If batch_size set, yields batches of above types

    Example:
        >>> fs = LocalFileSystem()
        >>> # Read all CSVs in directory
        >>> df = fs.read_csv(
        ...     "data/*.csv",
        ...     include_file_path=True
        ... )
        >>> print(df.columns)
        ['file_path', 'col1', 'col2', ...]
        >>>
        >>> # Batch process large dataset
        >>> for batch_df in fs.read_csv(
        ...     "logs/*.csv",
        ...     batch_size=100,
        ...     use_threads=True,
        ...     verbose=True
        ... ):
        ...     print(f"Processing {len(batch_df)} rows")
        >>>
        >>> # Multiple files without concatenation
        >>> dfs = fs.read_csv(
        ...     ["file1.csv", "file2.csv"],
        ...     concat=False,
        ...     use_threads=True
        ... )
        >>> print(f"Read {len(dfs)} files")
    """
    if batch_size is not None:
        return _read_csv_batches(
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
    return _read_csv(
        self=self,
        path=path,
        include_file_path=include_file_path,
        concat=concat,
        use_threads=use_threads,
        verbose=verbose,
        opt_dtypes=opt_dtypes,
        **kwargs,
    )


def write_csv(
    self: AbstractFileSystem,
    data: pl.DataFrame | pl.LazyFrame | pa.Table | pd.DataFrame | dict | list[dict],
    path: str,
    append: bool = False,
    **kwargs: Any,
) -> None:
    """Write data to a CSV file with flexible input support.

    Handles writing data from multiple formats to CSV with options for:
    - Appending to existing files
    - Custom delimiters and formatting
    - Automatic type conversion
    - Header handling

    Args:
        data: Input data in various formats:
            - Polars DataFrame/LazyFrame
            - PyArrow Table
            - Pandas DataFrame
            - Dict or list of dicts
        path: Output CSV file path
        append: Whether to append to existing file
        **kwargs: Additional arguments for CSV writing:
            - delimiter: Field separator (default ",")
            - header: Whether to write header row
            - quote_char: Character for quoting fields
            - date_format: Format for date/time fields
            - float_precision: Decimal places for floats

    Raises:
        FileNotFoundError: If the directory path does not exist
        PermissionError: If permission is denied to write the file
        OSError: For system-level I/O errors
        ValueError: If the data cannot be converted to CSV format

    Example:
        >>> fs = LocalFileSystem()
        >>> # Write Polars DataFrame
        >>> df = pl.DataFrame({
        ...     "id": range(100),
        ...     "name": ["item_" + str(i) for i in range(100)]
        ... })
        >>> fs.write_csv(df, "items.csv")
        >>>
        >>> # Append records
        >>> new_items = pl.DataFrame({
        ...     "id": range(100, 200),
        ...     "name": ["item_" + str(i) for i in range(100, 200)]
        ... })
        >>> fs.write_csv(
        ...     new_items,
        ...     "items.csv",
        ...     append=True,
        ...     header=False
        ... )
        >>>
        >>> # Custom formatting
        >>> data = pa.table({
        ...     "date": [datetime.now()],
        ...     "value": [123.456]
        ... })
        >>> fs.write_csv(
        ...     data,
        ...     "formatted.csv",
        ...     date_format="%Y-%m-%d",
        ...     float_precision=2
        ... )
    """
    # Import dependencies lazily
    pl = _import_polars()

    operation = "write CSV"
    context = {"path": path, "operation": operation}

    try:
        if isinstance(data, pl.LazyFrame):
            data = data.collect()
        if isinstance(data, pl.DataFrame):
            if append:
                with self.open(path, "ab") as f:
                    data.write_csv(f, has_header=not append, **kwargs)
            else:
                with self.open(path, "wb") as f:
                    data.write_csv(f, **kwargs)
        else:
            # Handle other data types (pa.Table, pd.DataFrame, etc.)
            try:
                pa = _import_pyarrow()
                if isinstance(data, pa.Table):
                    pl.from_arrow(data).write_csv(path, **kwargs)
                    return
            except ImportError:
                pass

            try:
                pd = _import_pandas()
                if isinstance(data, pd.DataFrame):
                    pl.from_pandas(data).write_csv(path, **kwargs)
                    return
            except ImportError:
                pass

            # Fallback: try to convert to DataFrame
            pl.DataFrame(data).write_csv(path, **kwargs)

        logger.debug("Successfully wrote CSV: {path}", **context)

    except FileNotFoundError as e:
        logger.error("Directory not found during {operation}: {path}", **context)
        raise FileNotFoundError(
            f"Directory not found during {operation}: {path}"
        ) from e
    except PermissionError as e:
        logger.error("Permission denied during {operation}: {path}", **context)
        raise PermissionError(f"Permission denied during {operation}: {path}") from e
    except OSError as e:
        logger.error(
            "System error during {operation}: {path} - {error}",
            **{**context, "error": str(e)},
        )
        raise OSError(f"System error during {operation}: {path} - {e}") from e
    except ValueError as e:
        logger.error(
            "Invalid data format for CSV write in {path}: {error}",
            **{**context, "error": str(e)},
        )
        raise ValueError(f"Invalid data format for CSV write in {path}: {e}") from e
    except Exception as e:
        logger.error(
            "Unexpected error during {operation}: {path} - {error}",
            **{**context, "error": str(e)},
            exc_info=True,
        )
        raise
