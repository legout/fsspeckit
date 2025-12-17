"""JSON file I/O helpers for fsspec filesystems.

This module contains functions for reading and writing JSON and JSON Lines
files with support for:
- Single file and batch reading
- Parallel processing
- DataFrame conversion
- Polars dtype optimization
"""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, Any, Generator

if TYPE_CHECKING:
    # Type checking imports for optional dependencies
    import orjson
    import pyarrow as pa
    import pyarrow.dataset as pds

# Import lazy helpers for optional dependencies
from fsspeckit.common.optional import _import_orjson

from fsspec import AbstractFileSystem

from fsspeckit.common.misc import path_to_glob, run_parallel
from fsspeckit.common.logging import get_logger

# Conditionally import polars utilities
try:
    from fsspeckit.common.polars import opt_dtype as opt_dtype_pl
    from fsspeckit.common.polars import pl
except ImportError:
    opt_dtype_pl = None
    pl = None

# Get module logger
logger = get_logger(__name__)


def _read_json_file(
    path: str,
    self: AbstractFileSystem,
    include_file_path: bool = False,
    jsonlines: bool = False,
) -> dict | list[dict]:
    """Read a JSON file from any filesystem.

    Internal function that handles both regular JSON and JSON Lines formats.

    Args:
        path: Path to JSON file
        self: Filesystem instance to use for reading
        include_file_path: Whether to return dict with filepath as key
        jsonlines: Whether to read as JSON Lines format

    Returns:
        dict | list[dict]: Parsed JSON data. If include_file_path=True,
            returns {filepath: data}

    Example:
        >>> fs = LocalFileSystem()
        >>> # Regular JSON
        >>> data = _read_json_file("data.json", fs)
        >>> print(type(data))
        <class 'dict'>
        >>>
        >>> # JSON Lines with filepath
        >>> data = _read_json_file(
        ...     "data.jsonl",
        ...     fs,
        ...     include_file_path=True,
        ...     jsonlines=True
        ... )
        >>> print(list(data.keys())[0])
        'data.jsonl'
    """
    from fsspeckit.common.optional import _import_orjson

    orjson = _import_orjson()

    operation = "read" + (" (JSON Lines)" if jsonlines else " (JSON)")
    context = {"path": path, "operation": operation}

    try:
        with self.open(path) as f:
            if jsonlines:
                data = [orjson.loads(line) for line in f.readlines()]
            else:
                data = orjson.loads(f.read())
        logger.debug("Successfully read JSON: {path}", **context)
        if include_file_path:
            return {path: data}
        return data
    except FileNotFoundError as e:
        logger.error(
            "File not found during {operation}: {path}", **context
        )
        raise FileNotFoundError(
            f"File not found during {operation}: {path}"
        ) from e
    except PermissionError as e:
        logger.error(
            "Permission denied during {operation}: {path}", **context
        )
        raise PermissionError(
            f"Permission denied during {operation}: {path}"
        ) from e
    except OSError as e:
        logger.error(
            "System error during {operation}: {path} - {error}",
            **{**context, "error": str(e)}
        )
        raise OSError(
            f"System error during {operation}: {path} - {e}"
        ) from e
    except ValueError as e:
        logger.error(
            "Invalid JSON in {path}: {error}",
            **{**context, "error": str(e)}
        )
        raise ValueError(f"Invalid JSON in {path}: {e}") from e
    except Exception as e:
        logger.error(
            "Unexpected error during {operation}: {path} - {error}",
            **{**context, "error": str(e)},
            exc_info=True
        )
        raise


def read_json_file(
    self: AbstractFileSystem,
    path: str,
    include_file_path: bool = False,
    jsonlines: bool = False,
) -> dict | list[dict]:
    """Read a single JSON file from any filesystem.

    A public wrapper around _read_json_file providing a clean interface for
    reading individual JSON files.

    Args:
        path: Path to JSON file to read
        include_file_path: Whether to return dict with filepath as key
        jsonlines: Whether to read as JSON Lines format

    Returns:
        dict | list[dict]: Parsed JSON data. For regular JSON, returns a dict.
            For JSON Lines, returns a list of dicts. If include_file_path=True,
            returns {filepath: data}.

    Example:
        ```python
        fs = LocalFileSystem()

        # Read regular JSON
        data = fs.read_json_file("config.json")
        print(data["setting"])
        # 'value'

        # Read JSON Lines with filepath
        data = fs.read_json_file(
            "logs.jsonl",
            include_file_path=True,
            jsonlines=True,
        )
        print(list(data.keys())[0])
        # 'logs.jsonl'
        ```
    """
    return _read_json_file(
        path=path,
        self=self,
        include_file_path=include_file_path,
        jsonlines=jsonlines,
    )


def _read_json(
    self,
    path: str | list[str],
    include_file_path: bool = False,
    use_threads: bool = False,
    jsonlines: bool = False,
    as_dataframe: bool = True,
    concat: bool = True,
    verbose: bool = False,
    opt_dtypes: bool = False,
    **kwargs,
) -> dict | list[dict] | pl.DataFrame | list[pl.DataFrame]:
    """
    Read a JSON file or a list of JSON files.

    Args:
        path: (str | list[str]) Path to the JSON file(s).
        include_file_path: (bool, optional) If True, return a dictionary with the file path as key.
            Defaults to False.
        use_threads: (bool, optional) If True, read files in parallel. Defaults to True.
        jsonlines: (bool, optional) If True, read JSON lines. Defaults to False.
        as_dataframe: (bool, optional) If True, return a DataFrame. Defaults to True.
        concat: (bool, optional) If True, concatenate the DataFrames. Defaults to True.
        verbose: (bool, optional) If True, print verbose output. Defaults to False.
        opt_dtypes: (bool, optional) If True, optimize DataFrame dtypes. Defaults to False.
        **kwargs: Additional keyword arguments.

    Returns:
        (dict | list[dict] | pl.DataFrame | list[pl.DataFrame]):
            Dictionary, list of dictionaries, DataFrame or list of DataFrames.
    """
    if isinstance(path, str):
        path = path_to_glob(path, format="json")
        path = self.glob(path)

    if isinstance(path, list):
        if use_threads:
            data = run_parallel(
                _read_json_file,
                path,
                self=self,
                include_file_path=include_file_path,
                jsonlines=jsonlines,
                n_jobs=-1,
                backend="threading",
                verbose=verbose,
                **kwargs,
            )
        else:
            data = [
                _read_json_file(
                    path=p,
                    self=self,
                    include_file_path=include_file_path,
                    jsonlines=jsonlines,
                )
                for p in path
            ]
    else:
        data = _read_json_file(
            path=path,
            self=self,
            include_file_path=include_file_path,
            jsonlines=jsonlines,
        )
    if as_dataframe:
        if not include_file_path:
            # Handle both single file (dict) and multiple files (list) cases
            if isinstance(data, list):
                data = [pl.DataFrame(d) for d in data]
            else:
                data = [pl.DataFrame(data)]
        else:
            # Handle both single file (dict) and multiple files (list) cases
            if isinstance(data, list):
                data = [
                    [
                        pl.DataFrame(_data[k]).with_columns(
                            pl.lit(k).alias("file_path")
                        )
                        for k in _data
                    ][0]
                    for _data in data
                ]
            else:
                data = [
                    [
                        pl.DataFrame(data[k]).with_columns(pl.lit(k).alias("file_path"))
                        for k in data
                    ][0]
                ]
        if opt_dtypes:
            data = [opt_dtype_pl(df, strict=False) for df in data]
        if concat:
            result = pl.concat(data, how="diagonal_relaxed")
            # if opt_dtypes:
            #   result = opt_dtype_pl(result, strict=False)
            return result
    return data


def _read_json_batches(
    self: AbstractFileSystem,
    path: str | list[str],
    batch_size: int | None = None,
    include_file_path: bool = False,
    jsonlines: bool = False,
    as_dataframe: bool = True,
    concat: bool = True,
    use_threads: bool = False,
    verbose: bool = False,
    opt_dtypes: bool = False,
    **kwargs: Any,
) -> Generator[dict | list[dict] | pl.DataFrame | list[pl.DataFrame], None, None]:
    """Process JSON files in batches with optional parallel reading.

    Internal generator function that handles batched reading of JSON files
    with support for parallel processing within each batch.

    Args:
        path: Path(s) to JSON file(s). Glob patterns supported.
        batch_size: Number of files to process in each batch
        include_file_path: Include source filepath in output
        jsonlines: Whether to read as JSON Lines format
        as_dataframe: Convert output to Polars DataFrame(s)
        concat: Combine files within each batch
        use_threads: Enable parallel file reading within batches
        verbose: Print progress information
        opt_dtypes: Optimize DataFrame dtypes
        **kwargs: Additional arguments for DataFrame conversion

    Yields:
        Each batch of data in requested format:
        - dict | list[dict]: Raw JSON data
        - pl.DataFrame: Single DataFrame if concat=True
        - list[pl.DataFrame]: List of DataFrames if concat=False

    Example:
        >>> fs = LocalFileSystem()
        >>> # Process large dataset in batches
        >>> for batch in fs._read_json_batches(
        ...     "data/*.json",
        ...     batch_size=100,
        ...     as_dataframe=True,
        ...     verbose=True
        ... ):
        ...     print(f"Batch shape: {batch.shape}")
        >>>
        >>> # Parallel batch processing with filepath tracking
        >>> for batch in fs._read_json_batches(
        ...     ["logs1.jsonl", "logs2.jsonl"],
        ...     batch_size=1,
        ...     include_file_path=True,
        ...     use_threads=True
        ... ):
        ...     print(f"Processing {batch['file_path'][0]}")
    """
    # Handle path resolution
    if isinstance(path, str):
        path = path_to_glob(path, format="json")
        path = self.glob(path)

    # Process files in batches
    for i in range(0, len(path), batch_size):
        batch_paths = path[i : i + batch_size]

        # Read batch with optional parallelization
        if use_threads and len(batch_paths) > 1:
            batch_data = run_parallel(
                _read_json_file,
                batch_paths,
                self=self,
                include_file_path=include_file_path,
                jsonlines=jsonlines,
                n_jobs=-1,
                backend="threading",
                verbose=verbose,
                **kwargs,
            )
        else:
            batch_data = [
                _read_json_file(
                    path=p,
                    self=self,
                    include_file_path=include_file_path,
                    jsonlines=jsonlines,
                )
                for p in batch_paths
            ]

        if as_dataframe:
            if not include_file_path:
                batch_dfs = [pl.DataFrame(d) for d in batch_data]
            else:
                batch_dfs = [
                    [
                        pl.DataFrame(_data[k]).with_columns(
                            pl.lit(k).alias("file_path")
                        )
                        for k in _data
                    ][0]
                    for _data in batch_data
                ]
            if opt_dtypes:
                batch_dfs = [opt_dtype_pl(df, strict=False) for df in batch_dfs]
            if concat and len(batch_dfs) > 1:
                batch_df = pl.concat(batch_dfs, how="diagonal_relaxed")
                # if opt_dtypes:
                #    batch_df = opt_dtype_pl(batch_df, strict=False)
                yield batch_df
            else:
                # if opt_dtypes:
                #    batch_dfs = [opt_dtype_pl(df, strict=False) for df in batch_dfs]
                yield batch_dfs
        else:
            yield batch_data


def read_json(
    self: AbstractFileSystem,
    path: str | list[str],
    batch_size: int | None = None,
    include_file_path: bool = False,
    jsonlines: bool = False,
    as_dataframe: bool = True,
    concat: bool = True,
    use_threads: bool = False,
    verbose: bool = False,
    opt_dtypes: bool = False,
    **kwargs: Any,
) -> (
    dict
    | list[dict]
    | pl.DataFrame
    | list[pl.DataFrame]
    | Generator[dict | list[dict] | pl.DataFrame | list[pl.DataFrame], None, None]
):
    """Read JSON data from one or more files with powerful options.

    Provides a flexible interface for reading JSON data with support for:
    - Single file or multiple files
    - Regular JSON or JSON Lines format
    - Batch processing for large datasets
    - Parallel processing
    - DataFrame conversion
    - File path tracking

    Args:
        path: Path(s) to JSON file(s). Can be:
            - Single path string (globs supported)
            - List of path strings
        batch_size: If set, enables batch reading with this many files per batch
        include_file_path: Include source filepath in output
        jsonlines: Whether to read as JSON Lines format
        as_dataframe: Convert output to Polars DataFrame(s)
        concat: Combine multiple files/batches into single result
        use_threads: Enable parallel file reading
        verbose: Print progress information
        opt_dtypes: Optimize DataFrame dtypes for performance
        **kwargs: Additional arguments passed to DataFrame conversion

    Returns:
        Various types depending on arguments:
        - dict: Single JSON file as dictionary
        - list[dict]: Multiple JSON files as list of dictionaries
        - pl.DataFrame: Single or concatenated DataFrame
        - list[pl.DataFrame]: List of DataFrames (if concat=False)
        - Generator: If batch_size set, yields batches of above types

    Example:
        >>> fs = LocalFileSystem()
        >>> # Read all JSON files in directory
        >>> df = fs.read_json(
        ...     "data/*.json",
        ...     as_dataframe=True,
        ...     concat=True
        ... )
        >>> print(df.shape)
        (1000, 5)  # Combined data from all files
        >>>
        >>> # Batch process large dataset
        >>> for batch_df in fs.read_json(
        ...     "logs/*.jsonl",
        ...     batch_size=100,
        ...     jsonlines=True,
        ...     include_file_path=True
        ... ):
        ...     print(f"Processing {len(batch_df)} records")
        >>>
        >>> # Parallel read with custom options
        >>> dfs = fs.read_json(
        ...     ["file1.json", "file2.json"],
        ...     use_threads=True,
        ...     concat=False,
        ...     verbose=True
        ... )
        >>> print(f"Read {len(dfs)} files")
    """
    if batch_size is not None:
        return _read_json_batches(
            self=self,
            path=path,
            batch_size=batch_size,
            include_file_path=include_file_path,
            jsonlines=jsonlines,
            as_dataframe=as_dataframe,
            concat=concat,
            use_threads=use_threads,
            verbose=verbose,
            opt_dtypes=opt_dtypes,
            **kwargs,
        )
    return _read_json(
        self=self,
        path=path,
        include_file_path=include_file_path,
        jsonlines=jsonlines,
        as_dataframe=as_dataframe,
        concat=concat,
        use_threads=use_threads,
        verbose=verbose,
        opt_dtypes=opt_dtypes,
        **kwargs,
    )


def write_json(
    self: AbstractFileSystem,
    data: dict
    | pl.DataFrame
    | pl.LazyFrame
    | pa.Table
    | pd.DataFrame
    | dict
    | list[dict],
    path: str,
    append: bool = False,
) -> None:
    """Write data to a JSON file with flexible input support.

    Handles writing data in various formats to JSON or JSON Lines,
    with optional appending for streaming writes.

    Args:
        data: Input data in various formats:
            - Dict or list of dicts
            - Polars DataFrame/LazyFrame
            - PyArrow Table
            - Pandas DataFrame
        path: Output JSON file path
        append: Whether to append to existing file (JSON Lines mode)

    Example:
        >>> fs = LocalFileSystem()
        >>> # Write dictionary
        >>> data = {"name": "test", "values": [1, 2, 3]}
        >>> fs.write_json(data, "config.json")
        >>>
        >>> # Stream records
        >>> df1 = pl.DataFrame({"id": [1], "value": ["first"]})
        >>> df2 = pl.DataFrame({"id": [2], "value": ["second"]})
        >>> fs.write_json(df1, "stream.jsonl", append=False)
        >>> fs.write_json(df2, "stream.jsonl", append=True)
        >>>
        >>> # Convert PyArrow
        >>> table = pa.table({"a": [1, 2], "b": ["x", "y"]})
        >>> fs.write_json(table, "data.json")
    """
    path = str(path)

    from fsspeckit.common.optional import _import_pyarrow
    from fsspeckit.datasets.pyarrow import cast_schema, convert_large_types_to_normal

    pa_mod = _import_pyarrow()

    if isinstance(data, pl.LazyFrame):
        data = data.collect()
    # Get orjson via lazy import
    orjson = _import_orjson()

    # Try to import pandas safely
    try:
        from fsspeckit.common.optional import _import_pandas
        pd = _import_pandas()
    except ImportError:
        pd = None

    if isinstance(data, pl.DataFrame):
        data = data.to_arrow()
        data = cast_schema(data, convert_large_types_to_normal(data.schema)).to_pydict()
    elif pd is not None and isinstance(data, pd.DataFrame):
        data = pa_mod.Table.from_pandas(data, preserve_index=False).to_pydict()
    elif isinstance(data, pa_mod.Table):
        data = data.to_pydict()
    if append:
        with self.open(path, "ab") as f:
            if isinstance(data, dict):
                f.write(orjson.dumps(data) + b"\n")
            else:
                for record in data:
                    f.write(orjson.dumps(record) + b"\n")
    else:
        with self.open(path, "wb") as f:
            f.write(orjson.dumps(data))
