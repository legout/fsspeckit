"""Parquet file I/O helpers for fsspec filesystems.

This module contains functions for reading and writing Parquet files with support for:
- Single file and batch reading
- Parallel processing
- Schema unification and casting
- PyArrow Table output
- Dtype optimization
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generator

if TYPE_CHECKING:
    import pyarrow as pa

from fsspec import AbstractFileSystem

from fsspeckit.common.misc import path_to_glob, run_parallel
from fsspeckit.common.schema import cast_schema
from fsspeckit.common.schema import opt_dtype as opt_dtype_pa
from fsspeckit.common.schema import unify_schemas as unify_schemas_pa
from fsspeckit.common.logging import get_logger

# Get module logger
logger = get_logger(__name__)


def _read_parquet_file(
    path: str,
    self: AbstractFileSystem,
    include_file_path: bool = False,
    opt_dtypes: bool = False,
    **kwargs: Any,
) -> Any:
    """Read a single Parquet file from any filesystem.

    Internal function that handles reading individual Parquet files and
    optionally adds the source filepath as a column.

    Args:
        path: Path to Parquet file
        self: Filesystem instance to use for reading
        include_file_path: Add source filepath as a column
        opt_dtypes: Optimize DataFrame dtypes
        **kwargs: Additional arguments passed to pyarrow.parquet.read_table()

    Returns:
        pa.Table: PyArrow Table containing Parquet data

    Raises:
        FileNotFoundError: If the Parquet file does not exist
        PermissionError: If permission is denied to read the file
        OSError: For system-level I/O errors
        ValueError: If the path does not point to a Parquet file or file is corrupted

    Example:
        ```python
        fs = LocalFileSystem()
        table = _read_parquet_file(
            "data.parquet",
            fs,
            include_file_path=True,
            use_threads=True,
        )
        print("file_path" in table.column_names)
        # True
        ```
    """
    from fsspeckit.common.optional import _import_pyarrow, _import_pyarrow_parquet

    pa_mod = _import_pyarrow()
    pq = _import_pyarrow_parquet()

    operation = "read Parquet"
    context = {"path": path, "operation": operation}

    try:
        if not path.endswith(".parquet"):
            logger.error(
                "Invalid file extension in {path}: must end with .parquet",
                extra=context,
            )
            raise ValueError(
                f"Path '{path}' does not point to a Parquet file. "
                "Ensure the path ends with '.parquet'."
            )

        table = pq.read_table(path, filesystem=self, **kwargs)
        logger.debug("Successfully read Parquet: {path}", extra=context)

        if include_file_path:
            table = table.add_column(
                0,
                "file_path",
                pa_mod.array([path] * table.num_rows),
            )
        if opt_dtypes:
            table = opt_dtype_pa(table, strict=False)
        return table

    except FileNotFoundError as e:
        logger.error("File not found during {operation}: {path}", extra=context)
        raise FileNotFoundError(f"File not found during {operation}: {path}") from e
    except PermissionError as e:
        logger.error("Permission denied during {operation}: {path}", extra=context)
        raise PermissionError(f"Permission denied during {operation}: {path}") from e
    except OSError as e:
        logger.error(
            "System error during {operation}: {path} - {error}",
            extra={**context, "error": str(e)},
        )
        raise OSError(f"System error during {operation}: {path} - {e}") from e
    except ValueError as e:
        logger.error(
            "Invalid Parquet file in {path}: {error}",
            extra={**context, "error": str(e)},
        )
        raise ValueError(f"Invalid Parquet file in {path}: {e}") from e
    except Exception as e:
        logger.error(
            "Unexpected error during {operation}: {path} - {error}",
            extra={**context, "error": str(e)},
            exc_info=True,
        )
        raise


def read_parquet_file(
    self, path: str, include_file_path: bool = False, opt_dtypes: bool = False, **kwargs
) -> Any:
    """Read a single Parquet file from any filesystem.

    Internal function that handles reading individual Parquet files and
    optionally adds the source filepath as a column.

    Args:
        path: Path to Parquet file
        include_file_path: Add source filepath as a column
        opt_dtypes: Optimize DataFrame dtypes
        **kwargs: Additional arguments passed to pyarrow.parquet.read_table()

    Returns:
        pa.Table: PyArrow Table containing Parquet data

    Example:
        >>> fs = LocalFileSystem()
        >>> table = fs.read_parquet_file(
        ...     "data.parquet",
        ...     include_file_path=True,
        ...     use_threads=True
        ... )
        >>> print("file_path" in table.column_names)
        True
    """
    return _read_parquet_file(
        path=path,
        self=self,
        include_file_path=include_file_path,
        opt_dtypes=opt_dtypes,
        **kwargs,
    )


def _read_parquet(
    self,
    path: str | list[str],
    include_file_path: bool = False,
    use_threads: bool = False,
    concat: bool = True,
    verbose: bool = False,
    opt_dtypes: bool = False,
    **kwargs,
) -> Any:
    """
    Read a Parquet file or a list of Parquet files into a pyarrow Table.

    Args:
        path: (str | list[str]) Path to the Parquet file(s).
        include_file_path: (bool, optional) If True, return a Table with a
            'file_path' column.
            Defaults to False.
        use_threads: (bool, optional) If True, read files in parallel. Defaults to True.
        concat: (bool, optional) If True, concatenate the Tables. Defaults to True.
        **kwargs: Additional keyword arguments.

    Returns:
        (pa.Table | list[pa.Table]): Pyarrow Table or list of Pyarrow Tables.
    """
    # Import pyarrow lazily for type checking within function
    from fsspeckit.common.optional import _import_pyarrow

    pa_mod = _import_pyarrow()

    # if not include_file_path and concat:
    #    if isinstance(path, str):
    #        path = path.replace("**", "").replace("*.parquet", "")
    #    table = _read_parquet_file(path, self=self, opt_dtypes=opt_dtypes, **kwargs)
    #    return table
    # else:
    if isinstance(path, str):
        path = path_to_glob(path, format="parquet")
        path = self.glob(path)

    if isinstance(path, list):
        if use_threads:
            tables = run_parallel(
                _read_parquet_file,
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
            tables = [
                _read_parquet_file(
                    p,
                    self=self,
                    include_file_path=include_file_path,
                    opt_dtypes=opt_dtypes,
                    **kwargs,
                )
                for p in path
            ]
    else:
        tables = _read_parquet_file(
            path=path,
            self=self,
            include_file_path=include_file_path,
            opt_dtypes=opt_dtypes,
            **kwargs,
        )
    if concat:
        # Unify schemas before concatenation if opt_dtypes or multiple tables
        if isinstance(tables, list):
            if len(tables) > 0:
                schemas = [t.schema for t in tables]
                unified_schema = unify_schemas_pa(schemas, standardize_timezones=True)
                tables = [cast_schema(t, unified_schema) for t in tables]

            tables = [table for table in tables if table.num_rows > 0]
            if not tables:
                return unified_schema.empty_table()

            result = pa_mod.concat_tables(
                tables,
                promote_options="permissive",
            )
            # if opt_dtypes:
            #    result = opt_dtype_pa(result, strict=False)
            return result
        elif isinstance(tables, pa.Table):
            # if opt_dtypes:
            #    tables = opt_dtype_pa(tables, strict=False)
            return tables
        else:
            tables = [table for table in tables if table.num_rows > 0]
            if not tables:
                return unified_schema.empty_table()

            result = pa_mod.concat_tables(
                tables,
                promote_options="permissive",
            )
    return tables


def _read_parquet_batches(
    self: AbstractFileSystem,
    path: str | list[str],
    batch_size: int | None = None,
    include_file_path: bool = False,
    use_threads: bool = False,
    concat: bool = True,
    verbose: bool = False,
    opt_dtypes: bool = False,
    **kwargs: Any,
) -> Generator[Any, None, None]:
    """Process Parquet files in batches with performance optimizations.

    Internal generator function that handles batched reading of Parquet files
    with support for:
    - Parallel processing within batches
    - Metadata-based optimizations
    - Memory-efficient processing
    - Progress tracking

    Uses fast path for simple cases:
    - Single directory with _metadata
    - No need for filepath column
    - Concatenated output

    Args:
        path: Path(s) to Parquet file(s). Glob patterns supported.
        batch_size: Number of files to process in each batch
        include_file_path: Add source filepath as a column
        use_threads: Enable parallel file reading within batches
        concat: Combine files within each batch
        verbose: Print progress information
        **kwargs: Additional arguments passed to pyarrow.parquet.read_table()

    Yields:
        Each batch of data in requested format:
        - pa.Table: Single Table if concat=True
        - list[pa.Table]: List of Tables if concat=False

    Example:
        >>> fs = LocalFileSystem()
        >>> # Fast path for simple case
        >>> next(_read_parquet_batches(
        ...     fs,
        ...     "data/",  # Contains _metadata
        ...     batch_size=1000
        ... ))
        >>>
        >>> # Parallel batch processing
        >>> for batch in fs._read_parquet_batches(
        ...     fs,
        ...     ["file1.parquet", "file2.parquet"],
        ...     batch_size=1,
        ...     include_file_path=True,
        ...     use_threads=True
        ... ):
        ...     print(f"Batch schema: {batch.schema}")
    """
    # Import pyarrow lazily for type checking within function
    from fsspeckit.common.optional import _import_pyarrow

    pa_mod = _import_pyarrow()

    # Fast path for simple cases
    # if not include_file_path and concat and batch_size is None:
    #    if isinstance(path, str):
    #        path = path.replace("**", "").replace("*.parquet", "")
    #    table = _read_parquet_file(
    #        path=path, self=self, opt_dtypes=opt_dtypes, **kwargs
    #    )
    #    yield table
    #    return

    # Resolve path(s) to list
    if isinstance(path, str):
        path = path_to_glob(path, format="parquet")
        path = self.glob(path)

    if not isinstance(path, list):
        yield _read_parquet_file(
            path=path,
            self=self,
            include_file_path=include_file_path,
            opt_dtypes=opt_dtypes,
            **kwargs,
        )
        return

    # Process in batches
    for i in range(0, len(path), batch_size):
        batch_paths = path[i : i + batch_size]
        if use_threads and len(batch_paths) > 1:
            batch_tables = run_parallel(
                _read_parquet_file,
                batch_paths,
                self=self,
                include_file_path=include_file_path,
                opt_dtypes=opt_dtypes,
                n_jobs=-1,
                backend="threading",
                verbose=verbose,
                **kwargs,
            )
        else:
            batch_tables = [
                _read_parquet_file(
                    p,
                    self=self,
                    include_file_path=include_file_path,
                    opt_dtypes=opt_dtypes,
                    **kwargs,
                )
                for p in batch_paths
            ]

        if concat and batch_tables:
            # Unify schemas before concatenation
            if len(batch_tables) > 1:
                schemas = [t.schema for t in batch_tables]
                unified_schema = unify_schemas_pa(schemas, standardize_timezones=True)
                batch_tables = [cast_schema(t, unified_schema) for t in batch_tables]
            batch_tables = [table for table in batch_tables if table.num_rows > 0]
            if not batch_tables:
                yield unified_schema.empty_table()
            batch_table = pa_mod.concat_tables(
                batch_tables,
                promote_options="permissive",
            )
            # if opt_dtypes:
            #    result = opt_dtype_pa(result, strict=False)
            yield batch_table
        else:
            # if opt_dtypes and isinstance(batch_tables, list):
            #    batch_tables = [opt_dtype_pa(t, strict=False) for t in batch_tables]
            yield batch_tables


def read_parquet(
    self: AbstractFileSystem,
    path: str | list[str],
    batch_size: int | None = None,
    include_file_path: bool = False,
    concat: bool = True,
    use_threads: bool = False,
    verbose: bool = False,
    opt_dtypes: bool = False,
    **kwargs: Any,
) -> Any:
    """Read Parquet data with advanced features and optimizations.

    Provides a high-performance interface for reading Parquet files with support for:
    - Single file or multiple files
    - Batch processing for large datasets
    - Parallel processing
    - File path tracking
    - Automatic concatenation
    - PyArrow Table output

    The function automatically uses optimal reading strategies:
    - Direct dataset reading for simple cases
    - Parallel processing for multiple files
    - Batched reading for memory efficiency

    Args:
        path: Path(s) to Parquet file(s). Can be:
            - Single path string (globs supported)
            - List of path strings
            - Directory containing _metadata file
        batch_size: If set, enables batch reading with this many files per batch
        include_file_path: Add source filepath as a column
        concat: Combine multiple files/batches into single Table
        use_threads: Enable parallel file reading
        verbose: Print progress information
        opt_dtypes: Optimize Table dtypes for performance
        **kwargs: Additional arguments passed to pyarrow.parquet.read_table()

    Returns:
        Various types depending on arguments:
        - pa.Table: Single or concatenated Table
        - list[pa.Table]: List of Tables (if concat=False)
        - Generator: If batch_size set, yields batches of above types

    Example:
        >>> fs = LocalFileSystem()
        >>> # Read all Parquet files in directory
        >>> table = fs.read_parquet(
        ...     "data/*.parquet",
        ...     include_file_path=True
        ... )
        >>> print(table.column_names)
        ['file_path', 'col1', 'col2', ...]
        >>>
        >>> # Batch process large dataset
        >>> for batch in fs.read_parquet(
        ...     "data/*.parquet",
        ...     batch_size=100,
        ...     use_threads=True
        ... ):
        ...     print(f"Processing {batch.num_rows} rows")
        >>>
        >>> # Read from directory with metadata
        >>> table = fs.read_parquet(
        ...     "data/",  # Contains _metadata
        ...     use_threads=True
        ... )
        >>> print(f"Total rows: {table.num_rows}")
    """
    if batch_size is not None:
        return _read_parquet_batches(
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
    return _read_parquet(
        self=self,
        path=path,
        include_file_path=include_file_path,
        use_threads=use_threads,
        concat=concat,
        verbose=verbose,
        opt_dtypes=opt_dtypes,
        **kwargs,
    )


def write_parquet(
    self: AbstractFileSystem,
    data: Any,
    path: str,
    schema: Any | None = None,
    **kwargs: Any,
) -> Any:
    """Write data to a Parquet file with automatic format conversion.

    Handles writing data from multiple input formats to Parquet with:
    - Automatic conversion to PyArrow
    - Schema validation/coercion
    - Metadata collection
    - Compression and encoding options

    Args:
        data: Input data in various formats:
            - Polars DataFrame/LazyFrame
            - PyArrow Table
            - Pandas DataFrame
            - Dict or list of dicts
        path: Output Parquet file path
        schema: Optional schema to enforce on write
        **kwargs: Additional arguments for pq.write_table()

    Returns:
        pyarrow.parquet.FileMetaData: Metadata of written Parquet file

    Raises:
        SchemaError: If data doesn't match schema
        ValueError: If data cannot be converted

    Example:
        >>> fs = LocalFileSystem()
        >>> # Write Polars DataFrame
        >>> df = pl.DataFrame({
        ...     "id": range(1000),
        ...     "value": pl.Series(np.random.randn(1000))
        ... })
        >>> metadata = fs.write_parquet(
        ...     df,
        ...     "data.parquet",
        ...     compression="zstd",
        ...     compression_level=3
        ... )
        >>> print(f"Rows: {metadata.num_rows}")
        >>>
        >>> # Write with schema
        >>> schema = pa.schema([
        ...     ("id", pa.int64()),
        ...     ("value", pa.float64())
        ... ])
        >>> metadata = fs.write_parquet(
        ...     {"id": [1, 2], "value": [0.1, 0.2]},
        ...     "data.parquet",
        ...     schema=schema
        ... )
    """
    from fsspeckit.common.optional import _import_pyarrow, _import_pyarrow_parquet
    from fsspeckit.common.types import to_pyarrow_table

    _import_pyarrow()  # Ensure pyarrow is available
    pq = _import_pyarrow_parquet()

    # Convert data to pyarrow table
    data = to_pyarrow_table(data, concat=True, unique=False)

    # Apply schema if provided
    if schema is not None:
        data = cast_schema(data, schema)

    # Write the table
    metadata = []
    pq.write_table(data, path, filesystem=self, metadata_collector=metadata, **kwargs)
    metadata = metadata[0]
    metadata.set_file_path(path)
    return metadata
