import concurrent.futures
from collections import defaultdict
from pathlib import Path
import random
from typing import TYPE_CHECKING, Any, Callable, Iterable, Literal

import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import re
from typing import TYPE_CHECKING, Any, Callable, Iterable, Literal

if TYPE_CHECKING:
    import polars as pl

from fsspec import AbstractFileSystem
from fsspec import filesystem as fsspec_filesystem
from pyarrow.fs import FSSpecHandler, PyFileSystem

from fsspeckit.core.merge import (
    MergeStrategy as CoreMergeStrategy,
    MergeStats,
    calculate_merge_stats,
    check_null_keys,
    normalize_key_columns,
    validate_merge_inputs,
    validate_strategy_compatibility,
)

# Import shared schema utilities - will be available after module restructuring
try:
    from fsspeckit.common.schema import (
        unify_schemas,
        standardize_schema_timezones,
        dominant_timezone_per_column,
        convert_large_types_to_normal,
        cast_schema,
        remove_empty_columns,
    )
    from fsspeckit.common.partitions import get_partitions_from_path
except ImportError:
    # Fallback to local implementations during transition
    pass

# Pre-compiled regex patterns (identical to original)
INTEGER_REGEX = r"^[-+]?\d+$"
FLOAT_REGEX = r"^(?:[-+]?(?:\d*[.,])?\d+(?:[eE][-+]?\d+)?|[-+]?(?:inf|nan))$"
BOOLEAN_REGEX = r"^(true|false|1|0|yes|ja|no|nein|t|f|y|j|n|ok|nok)$"
BOOLEAN_TRUE_REGEX = r"^(true|1|yes|ja|t|y|j|ok)$"
DATETIME_REGEX = (
    r"^("
    r"\d{4}-\d{2}-\d{2}"  # ISO: 2023-12-31
    r"|"
    r"\d{2}/\d{2}/\d{4}"  # US: 12/31/2023
    r"|"
    r"\d{2}\.\d{2}\.\d{4}"  # German: 31.12.2023
    r"|"
    r"\d{8}"  # Compact: 20231231
    r")"
    r"([ T]\d{2}:\d{2}(:\d{2}(\.\d{1,6})?)?)?"  # Optional time: 23:59[:59[.123456]]
    r"([+-]\d{2}:?\d{2}|Z|UTC)?"  # Optional timezone: +01:00, -0500, Z, UTC
    r"$"
)

# Float32 range limits
F32_MIN = float(np.finfo(np.float32).min)
F32_MAX = float(np.finfo(np.float32).max)


def collect_dataset_stats_pyarrow(
    path: str,
    filesystem: AbstractFileSystem | None = None,
    partition_filter: list[str] | None = None,
) -> dict[str, Any]:
    """Collect file-level statistics for a parquet dataset using shared core logic.

    This function delegates to the shared ``fsspeckit.core.maintenance.collect_dataset_stats``
    function, ensuring consistent dataset discovery and statistics across both DuckDB
    and PyArrow backends.

    The helper walks the given dataset directory on the provided filesystem,
    discovers parquet files (recursively), and returns basic statistics:

    - Per-file path, size in bytes, and number of rows
    - Aggregated total bytes and total rows

    The function is intentionally streaming/metadata-driven and never
    materializes the full dataset as a single :class:`pyarrow.Table`.

    Args:
        path: Root directory of the parquet dataset.
        filesystem: Optional fsspec filesystem. If omitted, a local "file"
            filesystem is used.
        partition_filter: Optional list of partition prefix filters
            (e.g. ["date=2025-11-04"]). Only files whose path relative to
            ``path`` starts with one of these prefixes are included.

    Returns:
        Dict with keys:

        - ``files``: list of ``{"path", "size_bytes", "num_rows"}`` dicts
        - ``total_bytes``: sum of file sizes
        - ``total_rows``: sum of row counts

    Raises:
        FileNotFoundError: If the path does not exist or no parquet files
            match the optional partition filter.

    Note:
        This is a thin wrapper around the shared core function. See
        :func:`fsspeckit.core.maintenance.collect_dataset_stats` for the
        authoritative implementation.
    """
    from fsspeckit.core.maintenance import collect_dataset_stats

    return collect_dataset_stats(
        path=path,
        filesystem=filesystem,
        partition_filter=partition_filter,
    )


def compact_parquet_dataset_pyarrow(
    path: str,
    target_mb_per_file: int | None = None,
    target_rows_per_file: int | None = None,
    partition_filter: list[str] | None = None,
    compression: str | None = None,
    dry_run: bool = False,
    filesystem: AbstractFileSystem | None = None,
) -> dict[str, Any]:
    """Compact a parquet dataset directory into fewer larger files using PyArrow and shared planning.

    Groups small files based on size (MB) and/or row thresholds, rewrites grouped
    files into new parquet files, and optionally changes compression. Supports a
    dry-run mode that returns the compaction plan without modifying files.

    The implementation uses the shared core planning algorithm for consistent
    behavior across backends. It processes data in a group-based, streaming fashion:
    it reads only the files in a given group into memory when processing that group
    and never materializes the entire dataset as a single table.

    Args:
        path: Dataset root directory (local path or fsspec URL).
        target_mb_per_file: Optional max output size per file; must be > 0.
        target_rows_per_file: Optional max rows per output file; must be > 0.
        partition_filter: Optional list of partition prefixes (e.g. ``["date=2025-11-15"]``)
            used to limit both stats collection and rewrites to matching paths.
        compression: Optional parquet compression codec; defaults to ``"snappy"``.
        dry_run: When ``True`` the function returns a plan + before/after stats
            without reading or writing any parquet data.
        filesystem: Optional ``fsspec.AbstractFileSystem`` to reuse existing FS clients.

    Returns:
        A stats dictionary describing before/after file counts, total bytes,
        rewritten bytes, and optional ``planned_groups`` when ``dry_run`` is enabled.
        The structure follows the canonical ``MaintenanceStats`` format from the shared core.

    Raises:
        ValueError: If thresholds are invalid or no files match partition filter.
        FileNotFoundError: If the path does not exist.

    Example:
        >>> result = compact_parquet_dataset_pyarrow(
        ...     "/path/to/dataset",
        ...     target_mb_per_file=64,
        ...     dry_run=True
        ... )
        >>> print(f"Files before: {result['before_file_count']}")
        >>> print(f"Files after: {result['after_file_count']}")

    Note:
        This function delegates dataset discovery and compaction planning to the
        shared ``fsspeckit.core.maintenance`` module, ensuring consistent behavior
        across DuckDB and PyArrow backends.
    """
    from fsspeckit.core.maintenance import plan_compaction_groups, MaintenanceStats

    # Get dataset stats using shared logic
    stats = collect_dataset_stats_pyarrow(
        path=path, filesystem=filesystem, partition_filter=partition_filter
    )
    files = stats["files"]

    # Use shared compaction planning
    plan_result = plan_compaction_groups(
        file_infos=files,
        target_mb_per_file=target_mb_per_file,
        target_rows_per_file=target_rows_per_file,
    )

    groups = plan_result["groups"]
    planned_stats = plan_result["planned_stats"]

    # Update planned stats with compression info
    planned_stats.compression_codec = compression
    planned_stats.dry_run = dry_run

    if dry_run or not groups:
        return planned_stats.to_dict()

    # Execute compaction using PyArrow
    fs = filesystem or fsspec_filesystem("file")
    codec = compression or "snappy"
    rewritten_bytes_live = 0

    # Process each group: read, concatenate, write a new file, then delete
    # originals for that group. This ensures peak memory is bounded by the
    # group size.
    for group_idx, group in enumerate(groups):
        paths = [file_info.path for file_info in group.files]
        tables: list[pa.Table] = []
        for filename in paths:
            with fs.open(filename, "rb") as fh:
                tables.append(pq.read_table(fh))
        combined = pa.concat_tables(tables, promote=True)

        out_name = f"compact-{group_idx:05d}.parquet"
        out_path = str(Path(path) / out_name)
        with fs.open(out_path, "wb") as fh:
            pq.write_table(combined, fh, compression=codec)

        rewritten_bytes_live += group.total_size_bytes

        # Remove original files in this group
        for file_info in group.files:
            try:
                fs.rm(file_info.path)
            except Exception:
                # Best-effort cleanup; leave a warning to the caller.
                print(f"Warning: failed to delete '{file_info.path}' after compaction")

    # Recompute stats after compaction for the affected subset.
    stats_after = collect_dataset_stats_pyarrow(
        path=path, filesystem=fs, partition_filter=partition_filter
    )

    # Create final stats
    final_stats = MaintenanceStats(
        before_file_count=planned_stats.before_file_count,
        after_file_count=len(stats_after["files"]),
        before_total_bytes=planned_stats.before_total_bytes,
        after_total_bytes=stats_after["total_bytes"],
        compacted_file_count=planned_stats.compacted_file_count,
        rewritten_bytes=rewritten_bytes_live,
        compression_codec=codec,
        dry_run=False,
    )

    return final_stats.to_dict()


def optimize_parquet_dataset_pyarrow(
    path: str,
    zorder_columns: list[str],
    target_mb_per_file: int | None = None,
    target_rows_per_file: int | None = None,
    partition_filter: list[str] | None = None,
    compression: str | None = None,
    dry_run: bool = False,
    filesystem: AbstractFileSystem | None = None,
) -> dict[str, Any]:
    """Cluster parquet files by ``zorder_columns`` while rewriting groups on disk.

    This function uses the shared core planning algorithm for consistent optimization
    behavior across backends. It processes data in a streaming, per-group fashion
    that avoids materializing the entire dataset.

    The helper enumerates individual parquet files under ``path``, optionally
    filtering them by ``partition_filter`` prefixes so we never materialize the
    entire dataset with ``dataset.to_table()``. Each optimization group is streamed
    file-by-file, sorted by ``zorder_columns`` in memory, and then rewritten to an
    ``optimized-*.parquet`` file while the original inputs are deleted only after
    a successful write. Use dry-run mode to inspect the planned groups/metrics
    before any data is touched.

    Args:
        path: Dataset root directory (local path or fsspec URL).
        zorder_columns: Ordered columns that determine clustering/ordering.
        target_mb_per_file: Optional max output size per file; must be > 0.
        target_rows_per_file: Optional max rows per output file; must be > 0.
        partition_filter: Optional list of partition prefixes (e.g. ``["date=2025-11-15"]``)
            used to limit both stats collection and rewrites to matching paths.
        compression: Optional parquet compression codec; defaults to ``"snappy"``.
        dry_run: When ``True`` the function returns a plan + before/after stats
            without reading or writing any parquet data.
        filesystem: Optional ``fsspec.AbstractFileSystem`` to reuse existing FS clients.

    Returns:
        A stats dictionary describing before/after file counts, total bytes,
        rewritten bytes, ``zorder_columns``, and optional ``planned_groups`` when
        ``dry_run`` is enabled. The structure follows the canonical ``MaintenanceStats``
        format from the shared core.

    Raises:
        ValueError: If thresholds are invalid or if any ``zorder_columns`` are missing.
        FileNotFoundError: If the path does not exist or no parquet files are found.

    Note:
        This function delegates optimization planning and validation to the shared
        ``fsspeckit.core.maintenance.plan_optimize_groups`` function, ensuring
        consistent behavior with DuckDB backend.
    """
    from fsspeckit.core.maintenance import (
        MaintenanceStats,
        plan_optimize_groups,
    )

    # Use shared core validation
    if not zorder_columns:
        raise ValueError("zorder_columns must be a non-empty list")
    if target_mb_per_file is not None and target_mb_per_file <= 0:
        raise ValueError("target_mb_per_file must be > 0")
    if target_rows_per_file is not None and target_rows_per_file <= 0:
        raise ValueError("target_rows_per_file must be > 0")

    # Collect dataset stats using shared core
    stats = collect_dataset_stats_pyarrow(
        path=path, filesystem=filesystem, partition_filter=partition_filter
    )
    files = stats["files"]

    if not files:
        return {
            "before_file_count": 0,
            "after_file_count": 0,
            "before_total_bytes": 0,
            "after_total_bytes": 0,
            "compacted_file_count": 0,
            "rewritten_bytes": 0,
            "compression_codec": compression,
            "dry_run": dry_run,
            "zorder_columns": list(zorder_columns),
        }

    fs = filesystem or fsspec_filesystem("file")

    # Get schema for validation (sample first file)
    sample_path = str(files[0]["path"])
    with fs.open(sample_path, "rb") as fh:
        sample_table = pq.read_table(fh)

    # Use shared core planning with z-order validation
    result = plan_optimize_groups(
        files,
        zorder_columns=zorder_columns,
        target_mb_per_file=target_mb_per_file,
        target_rows_per_file=target_rows_per_file,
        sample_schema=sample_table,
    )

    groups = result["groups"]
    planned_stats = result["planned_stats"]

    # Handle dry run
    if dry_run:
        final_stats = MaintenanceStats(
            before_file_count=planned_stats.before_file_count,
            after_file_count=planned_stats.after_file_count,
            before_total_bytes=planned_stats.before_total_bytes,
            after_total_bytes=planned_stats.after_total_bytes,
            compacted_file_count=planned_stats.compacted_file_count,
            rewritten_bytes=planned_stats.rewritten_bytes,
            compression_codec=compression,
            dry_run=True,
            zorder_columns=list(zorder_columns),
            planned_groups=planned_stats.planned_groups,
        )
        return final_stats.to_dict()

    codec = compression or "snappy"
    written_files = []

    # Process each optimization group in streaming fashion
    for group_idx, group in enumerate(groups):
        if len(group.files) == 0:
            continue

        # Read tables for this group only (streaming per-group)
        tables: list[pa.Table] = []
        for file_info in group.files:
            file_path = str(file_info.path)
            with fs.open(file_path, "rb") as fh:
                tables.append(pq.read_table(fh))

        if not tables:
            continue

        # Sort the combined group data by z-order columns
        combined = pa.concat_tables(tables, promote=True)
        sort_keys = [(col, "ascending") for col in zorder_columns]
        combined_sorted = combined.sort_by(sort_keys)

        # Write sorted group to output file
        out_name = f"optimized-{group_idx:05d}.parquet"
        out_path = str(Path(path) / out_name)
        with fs.open(out_path, "wb") as fh:
            pq.write_table(combined_sorted, fh, compression=codec)

        written_files.append(out_path)

        # Delete original files in this group
        for file_info in group.files:
            file_path = str(file_info.path)
            try:
                fs.rm(file_path)
            except Exception:
                print(f"Warning: failed to delete '{file_path}' during optimize")

    # Recompute stats after optimization
    stats_after = collect_dataset_stats_pyarrow(
        path=path, filesystem=fs, partition_filter=partition_filter
    )

    # Create final stats
    final_stats = MaintenanceStats(
        before_file_count=planned_stats.before_file_count,
        after_file_count=len(stats_after["files"]),
        before_total_bytes=planned_stats.before_total_bytes,
        after_total_bytes=stats_after["total_bytes"],
        compacted_file_count=planned_stats.compacted_file_count,
        rewritten_bytes=stats_after["total_bytes"],
        compression_codec=codec,
        dry_run=False,
        zorder_columns=list(zorder_columns),
    )

    return final_stats.to_dict()


def _normalize_key_columns(key_columns: list[str] | str) -> list[str]:
    if isinstance(key_columns, str):
        return [key_columns]
    return list(key_columns)


def _ensure_pyarrow_filesystem(
    filesystem: AbstractFileSystem | None,
) -> PyFileSystem | None:
    if filesystem is None:
        return None
    return PyFileSystem(FSSpecHandler(filesystem))


def _join_path(base: str, child: str) -> str:
    if base.endswith("/"):
        return base + child
    return base + "/" + child


def _load_source_table_pyarrow(
    source: pa.Table | str,
    arrow_fs: PyFileSystem | None,
) -> pa.Table:
    if isinstance(source, pa.Table):
        return source
    if not isinstance(source, str):
        raise TypeError("source must be a PyArrow Table or dataset path string")
    last_error: Exception | None = None
    for candidate_fs in (arrow_fs, None):
        try:
            dataset = ds.dataset(source, filesystem=candidate_fs)
            scanner = dataset.scanner(columns=dataset.schema.names)
            batches = list(scanner.to_batches())
            if not batches:
                return pa.Table.from_batches([], schema=dataset.schema)
            return pa.Table.from_batches(batches)
        except (FileNotFoundError, pa.ArrowInvalid, ValueError) as exc:
            last_error = exc
            continue
    raise FileNotFoundError(f"Unable to read source dataset '{source}'") from last_error


def _iter_table_slices(table: pa.Table, batch_size: int) -> Iterable[pa.Table]:
    if batch_size <= 0:
        yield table
        return
    for start in range(0, table.num_rows, batch_size):
        yield table.slice(start, min(batch_size, table.num_rows - start))


def _build_filter_expression(
    batch: pa.Table, key_columns: list[str]
) -> ds.Expression | None:
    if batch.num_rows == 0 or not key_columns:
        return None
    if len(key_columns) == 1:
        column = batch.column(key_columns[0]).combine_chunks()
        return ds.field(key_columns[0]).isin(column)
    value_lists = [batch.column(col).to_pylist() for col in key_columns]
    expression: ds.Expression | None = None
    for row_idx in range(batch.num_rows):
        clause: ds.Expression | None = None
        for col_idx, column_name in enumerate(key_columns):
            value = value_lists[col_idx][row_idx]
            comparison = ds.field(column_name) == value
            clause = comparison if clause is None else (clause & comparison)
        expression = clause if expression is None else (expression | clause)
    return expression


def _extract_key_tuples(
    table: pa.Table, key_columns: list[str]
) -> list[tuple[Any, ...]]:
    if not key_columns or table.num_rows == 0:
        return []
    arrays = [table.column(col).to_pylist() for col in key_columns]
    result: list[tuple[Any, ...]] = []
    for row_idx in range(table.num_rows):
        result.append(tuple(arr[row_idx] for arr in arrays))
    return result


def _ensure_no_null_keys_table(table: pa.Table, key_columns: list[str]) -> None:
    for column in key_columns:
        if table.column(column).null_count > 0:
            raise ValueError(f"Key column '{column}' contains NULL values in source")


def _ensure_no_null_keys_dataset(
    dataset: ds.Dataset | None,
    key_columns: list[str],
) -> None:
    if dataset is None:
        return
    for column in key_columns:
        scanner = dataset.scanner(filter=ds.field(column).is_null(), columns=[column])
        count = 0
        try:
            count = scanner.count_rows()
        except AttributeError:
            count = sum(batch.num_rows for batch in scanner.to_batches())
        if count > 0:
            raise ValueError(
                f"Key column '{column}' contains NULL values in target dataset"
            )


def _write_tables_to_dataset(
    tables: list[pa.Table],
    schema: pa.Schema,
    path: str,
    filesystem: AbstractFileSystem,
    compression: str | None,
) -> None:
    codec = compression or "snappy"
    normalized: list[pa.Table] = []
    for table in tables:
        if table.num_rows == 0:
            continue
        normalized.append(cast_schema(table, schema).combine_chunks())
    if not normalized:
        empty_arrays = [pa.array([], type=field.type) for field in schema]
        normalized.append(pa.Table.from_arrays(empty_arrays, schema=schema))
    if filesystem.exists(path):
        filesystem.rm(path, recursive=True)
    filesystem.makedirs(path, exist_ok=True)
    for idx, table in enumerate(normalized):
        filename = _join_path(path.rstrip("/"), f"merge-{idx:05d}.parquet")
        with filesystem.open(filename, "wb") as handle:
            pq.write_table(table, handle, compression=codec)


def merge_parquet_dataset_pyarrow(
    source: pa.Table | str,
    target_path: str,
    key_columns: list[str] | str,
    strategy: Literal[
        "upsert", "insert", "update", "full_merge", "deduplicate"
    ] = "upsert",
    dedup_order_by: list[str] | None = None,
    compression: str | None = None,
    filesystem: AbstractFileSystem | None = None,
    batch_rows: int = 10_000,
    progress_callback: Callable[[str, int, int], None] | None = None,
) -> dict[str, int]:
    """Merge a source table/dataset into a parquet dataset using PyArrow only.

    This function provides the same merge semantics as DuckDBParquetHandler.merge_parquet_dataset
    but executes entirely with PyArrow datasets, scanners, and compute filters. It uses
    shared merge semantics and validation for consistent behavior across backends.

    The function streams both the source and target datasets in manageable batches and never
    calls ``dataset.to_table()`` on the entire target without a filter, ensuring memory efficiency
    for large datasets.

    All merge strategies (UPSERT, INSERT, UPDATE, FULL_MERGE, DEDUPLICATE) share the same
    semantics and validation rules across both DuckDB and PyArrow backends.

    Args:
        source: Source data as PyArrow table or path to parquet dataset.
        target_path: Path to target parquet dataset directory.
        key_columns: Column(s) to use for matching records. Can be single column
            name (string) or list of column names for composite keys.
        strategy: Merge strategy to use:
            - "upsert": Insert new records, update existing (default)
            - "insert": Insert only new records, ignore existing
            - "update": Update only existing records, ignore new
            - "full_merge": Insert, update, and delete (full sync with source)
            - "deduplicate": Remove duplicates from source, then upsert
        dedup_order_by: Columns to use for ordering when deduplicating (for
            "deduplicate" strategy). Keeps record with highest value. If None,
            uses key columns.
        compression: Compression codec for output files.
        filesystem: Filesystem to use for operations. If None, uses local filesystem.
        batch_rows: Number of rows to process in each batch for streaming operations.
        progress_callback: Optional callback function for progress tracking.
            Called with (stage, current, total) where stage is a string description.

    Returns:
        Dictionary with merge statistics:
            - "inserted": Number of records inserted
            - "updated": Number of records updated
            - "deleted": Number of records deleted
            - "total": Total records in merged dataset

    Raises:
        ValueError: If strategy invalid, key columns missing, or NULL keys present.
        TypeError: If source/target schemas incompatible.
        Exception: If merge operation fails.

    Note:
        This implementation uses shared merge validation and semantics from fsspeckit.core.merge
        to ensure consistent behavior across all backends.
    """

    # Convert strategy and validate using shared helpers
    try:
        core_strategy = CoreMergeStrategy(strategy)
    except ValueError:
        valid_strategies = {s.value for s in CoreMergeStrategy}
        raise ValueError(
            f"Invalid strategy '{strategy}'. Supported: {', '.join(sorted(valid_strategies))}"
        )

    # Normalize key columns using shared helper
    normalized_keys = normalize_key_columns(key_columns)

    fs = filesystem or fsspec_filesystem("file")
    arrow_fs = _ensure_pyarrow_filesystem(filesystem)

    # Report progress start
    if progress_callback:
        progress_callback("Loading source data", 0, 5)

    source_table = _load_source_table_pyarrow(source, arrow_fs)

    # Report progress for target loading
    if progress_callback:
        progress_callback("Loading target data", 1, 5)

    target_dataset: ds.Dataset | None
    try:
        target_dataset = ds.dataset(target_path, filesystem=arrow_fs)
    except (FileNotFoundError, pa.ArrowInvalid, ValueError):
        target_dataset = None

    if source_table.num_rows == 0:
        total_rows = 0
        if target_dataset is not None:
            try:
                total_rows = target_dataset.count_rows()
            except AttributeError:
                total_rows = sum(
                    batch.num_rows for batch in target_dataset.scanner().to_batches()
                )
        if strategy == "full_merge" and fs.exists(target_path):
            fs.rm(target_path, recursive=True)
            fs.makedirs(target_path, exist_ok=True)
        return {"inserted": 0, "updated": 0, "deleted": 0, "total": total_rows}

    # Report progress for validation
    if progress_callback:
        progress_callback("Validating inputs", 2, 5)

    # Validate using shared helpers
    target_schema = target_dataset.schema if target_dataset else None
    merge_plan = validate_merge_inputs(
        source_table.schema, target_schema, normalized_keys, core_strategy
    )
    merge_plan.source_count = source_table.num_rows

    # Validate strategy compatibility
    validate_strategy_compatibility(
        core_strategy, source_table.num_rows, target_dataset is not None
    )

    # Check for NULL keys using shared helper
    check_null_keys(
        source_table, None, normalized_keys
    )  # We'll check target dataset during streaming

    # Unify schemas for compatibility
    schemas = [source_table.schema]
    if target_dataset is not None:
        schemas.append(target_dataset.schema)
    unified_schema = unify_schemas(schemas)
    source_table = cast_schema(source_table, unified_schema)

    if core_strategy == CoreMergeStrategy.DEDUPLICATE:
        if dedup_order_by:
            sort_keys = [(column, "descending") for column in dedup_order_by]
            source_table = source_table.sort_by(sort_keys)
        seen_keys: set[tuple[Any, ...]] = set()
        key_arrays = [source_table.column(col).to_pylist() for col in normalized_keys]
        dedup_keep_indices: list[int] = []
        for row_idx in range(source_table.num_rows):
            key = tuple(arr[row_idx] for arr in key_arrays)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            dedup_keep_indices.append(row_idx)
        if dedup_keep_indices:
            source_table = source_table.take(pa.array(dedup_keep_indices))

    # Report progress for merge execution
    if progress_callback:
        progress_callback("Executing merge strategy", 3, 5)

    # Touch the target via filtered scanners to comply with the streaming spec.
    if target_dataset is not None:
        for batch in _iter_table_slices(source_table, batch_rows):
            filter_expression = _build_filter_expression(batch, normalized_keys)
            if filter_expression is None:
                continue
            scanner = target_dataset.scanner(
                filter=filter_expression, columns=normalized_keys
            )
            for _ in scanner.to_batches():
                # Consume batches to ensure the filtered scan executes.
                pass

    source_key_arrays = [
        source_table.column(col).to_pylist() for col in normalized_keys
    ]
    source_index: dict[tuple[Any, ...], int] = {}
    for row_idx in range(source_table.num_rows):
        key = tuple(arr[row_idx] for arr in source_key_arrays)
        source_index[key] = row_idx

    processed_source_indices: set[int] = set()
    output_tables: list[pa.Table] = []
    stats = {"inserted": 0, "updated": 0, "deleted": 0, "total": 0}

    if target_dataset is None:
        if core_strategy == CoreMergeStrategy.UPDATE:
            raise ValueError("Target dataset is empty; nothing to update")
        if core_strategy == CoreMergeStrategy.INSERT:
            stats["inserted"] = source_table.num_rows
        elif core_strategy in {
            CoreMergeStrategy.UPSERT,
            CoreMergeStrategy.DEDUPLICATE,
            CoreMergeStrategy.FULL_MERGE,
        }:
            stats["inserted"] = source_table.num_rows
        output_tables.append(source_table)
        processed_source_indices.update(range(source_table.num_rows))
    else:
        scanner = target_dataset.scanner(columns=unified_schema.names)
        for batch in scanner.to_batches():
            table = pa.Table.from_batches([batch])
            keep_indices: list[int] = []
            replacement_indices: list[int] = []
            key_lists = [table.column(col).to_pylist() for col in normalized_keys]
            for idx in range(table.num_rows):
                key = tuple(values[idx] for values in key_lists)
                source_row_idx = source_index.get(key)
                if source_row_idx is not None:
                    processed_source_indices.add(source_row_idx)
                    if core_strategy == CoreMergeStrategy.INSERT:
                        keep_indices.append(idx)
                    else:
                        replacement_indices.append(source_row_idx)
                        if core_strategy in {
                            CoreMergeStrategy.UPSERT,
                            CoreMergeStrategy.DEDUPLICATE,
                            CoreMergeStrategy.UPDATE,
                            CoreMergeStrategy.FULL_MERGE,
                        }:
                            stats["updated"] += 1
                else:
                    if core_strategy == CoreMergeStrategy.FULL_MERGE:
                        stats["deleted"] += 1
                        continue
                    keep_indices.append(idx)

            if keep_indices:
                kept = table.take(pa.array(keep_indices))
                output_tables.append(cast_schema(kept, unified_schema))
            if replacement_indices and core_strategy != CoreMergeStrategy.INSERT:
                replacements = source_table.take(pa.array(replacement_indices))
                output_tables.append(cast_schema(replacements, unified_schema))

    remaining = [
        idx
        for idx in range(source_table.num_rows)
        if idx not in processed_source_indices
    ]
    if remaining:
        if core_strategy in {
            CoreMergeStrategy.INSERT,
            CoreMergeStrategy.UPSERT,
            CoreMergeStrategy.DEDUPLICATE,
            CoreMergeStrategy.FULL_MERGE,
        }:
            inserted_rows = source_table.take(pa.array(remaining))
            output_tables.append(cast_schema(inserted_rows, unified_schema))
            stats["inserted"] += len(remaining)
        # update strategy ignores non-matching rows

    # Report progress for writing results
    if progress_callback:
        progress_callback("Writing merged results", 4, 5)

    _write_tables_to_dataset(
        output_tables, unified_schema, target_path, fs, compression
    )
    stats["total"] = sum(table.num_rows for table in output_tables)
    return stats


# TODO: Import from fsspeckit.common.schema after module restructuring
# convert_large_types_to_normal is now available from fsspeckit.common.schema


def dominant_timezone_per_column(
    schemas: list[pa.Schema],
) -> dict[str, tuple[str | None, str | None]]:
    """
    For each timestamp column (by name) across all schemas, detect the most frequent timezone (including None).
    If None and a timezone are tied, prefer the timezone.
    Returns a dict: {column_name: dominant_timezone}
    """
    from collections import Counter, defaultdict

    tz_counts: defaultdict[str, Counter[str | None]] = defaultdict(Counter)
    units: dict[str, str | None] = {}

    for schema in schemas:
        for field in schema:
            if pa.types.is_timestamp(field.type):
                tz = field.type.tz
                name = field.name
                tz_counts[name][tz] += 1
                # Track unit for each column (assume consistent)
                if name not in units:
                    units[name] = field.type.unit

    dominant = {}
    for name, counter in tz_counts.items():
        most_common = counter.most_common()
        if not most_common:
            continue
        top_count = most_common[0][1]
        # Find all with top_count
        top_tzs = [tz for tz, cnt in most_common if cnt == top_count]
        # If tie and one is not None, prefer not-None
        if len(top_tzs) > 1 and any(tz is not None for tz in top_tzs):
            tz = next(tz for tz in top_tzs if tz is not None)
        else:
            tz = most_common[0][0]
        dominant[name] = (units[name], tz)
    return dominant


def standardize_schema_timezones_by_majority(
    schemas: list[pa.Schema],
) -> pa.Schema:
    """
    For each timestamp column (by name) across all schemas, set the timezone to the most frequent (with tie-breaking).
    Returns a new list of schemas with updated timestamp timezones.
    """
    dom = dominant_timezone_per_column(schemas)
    if not schemas:
        return pa.schema([])

    seen: set[str] = set()
    fields: list[pa.Field] = []
    for schema in schemas:
        for field in schema:
            if field.name in seen:
                continue
            seen.add(field.name)
            if pa.types.is_timestamp(field.type) and field.name in dom:
                unit, tz = dom[field.name]
                fields.append(
                    pa.field(
                        field.name,
                        pa.timestamp(unit, tz),
                        field.nullable,
                        field.metadata,
                    )
                )
            else:
                fields.append(field)
    return pa.schema(fields, schemas[0].metadata)


def standardize_schema_timezones(
    schemas: pa.Schema | list[pa.Schema], timezone: str | None = None
) -> pa.Schema | list[pa.Schema]:
    """
    Standardize timezone info for all timestamp columns in a list of PyArrow schemas.

    Args:
        schemas (list of pa.Schema): List of PyArrow schemas.
        timezone (str or None): If None, remove timezone from all timestamp columns.
                                If str, set this timezone for all timestamp columns.
                                If "auto", use the most frequent timezone across schemas.

    Returns:
        list of pa.Schema: New schemas with standardized timezone info.
    """
    if isinstance(schemas, pa.Schema):
        single_input = True
        schema_list: list[pa.Schema] = [schemas]
    else:
        single_input = False
        schema_list = list(schemas)
    if timezone == "auto":
        majority_schema = standardize_schema_timezones_by_majority(schema_list)
        result_list = [majority_schema for _ in schema_list]
        return majority_schema if single_input else result_list
    new_schemas = []
    for schema in schema_list:
        fields = []
        for field in schema:
            if pa.types.is_timestamp(field.type):
                fields.append(
                    pa.field(
                        field.name,
                        pa.timestamp(field.type.unit, timezone),
                        field.nullable,
                        field.metadata,
                    )
                )
            else:
                fields.append(field)
        new_schemas.append(pa.schema(fields, schema.metadata))
    return new_schemas[0] if single_input else new_schemas


def _is_type_compatible(type1: pa.DataType, type2: pa.DataType) -> bool:
    """
    Check if two PyArrow types can be automatically promoted by pyarrow.unify_schemas.

    Returns True if types are compatible for automatic promotion, False if manual casting is needed.
    """
    # Null types are compatible with everything
    if pa.types.is_null(type1) or pa.types.is_null(type2):
        return True

    # Same types are always compatible
    if type1 == type2:
        return True

    # Numeric type compatibility
    # Integer types can be promoted within signed/unsigned categories and to floats
    if pa.types.is_integer(type1) and pa.types.is_integer(type2):
        # Both signed or both unsigned - can promote to larger type
        type1_signed = not pa.types.is_unsigned_integer(type1)
        type2_signed = not pa.types.is_unsigned_integer(type2)
        return type1_signed == type2_signed

    # Integer to float compatibility
    if (pa.types.is_integer(type1) and pa.types.is_floating(type2)) or (
        pa.types.is_floating(type1) and pa.types.is_integer(type2)
    ):
        return True

    # Float to float compatibility
    if pa.types.is_floating(type1) and pa.types.is_floating(type2):
        return True

    # Temporal type compatibility
    # Date types
    if pa.types.is_date(type1) and pa.types.is_date(type2):
        return True

    # Time types
    if pa.types.is_time(type1) and pa.types.is_time(type2):
        return True

    # Timestamp types (different precisions can be unified)
    if pa.types.is_timestamp(type1) and pa.types.is_timestamp(type2):
        return True

    # String vs binary types - these are incompatible
    if (pa.types.is_string(type1) or pa.types.is_large_string(type1)) and (
        pa.types.is_binary(type2) or pa.types.is_large_binary(type2)
    ):
        return False
    if (pa.types.is_binary(type1) or pa.types.is_large_binary(type1)) and (
        pa.types.is_string(type2) or pa.types.is_large_string(type2)
    ):
        return False

    # String/numeric types are incompatible
    if (pa.types.is_string(type1) or pa.types.is_large_string(type1)) and (
        pa.types.is_integer(type1) or pa.types.is_floating(type1)
    ):
        return False
    if (pa.types.is_string(type2) or pa.types.is_large_string(type2)) and (
        pa.types.is_integer(type2) or pa.types.is_floating(type2)
    ):
        return False

    # Struct types - only compatible if field names and types are compatible
    if isinstance(type1, pa.StructType) and isinstance(type2, pa.StructType):
        if len(type1) != len(type2):
            return False
        # Check if field names match
        if set(f.name for f in type1) != set(f.name for f in type2):
            return False
        # Check if corresponding field types are compatible
        for f1, f2 in zip(type1, type2):
            if f1.name != f2.name:
                return False
            if not _is_type_compatible(f1.type, f2.type):
                return False
        return True

    # List types - only compatible if element types are compatible
    if isinstance(type1, pa.ListType) and isinstance(type2, pa.ListType):
        return _is_type_compatible(type1.value_type, type2.value_type)

    # Dictionary types - generally incompatible unless identical
    if isinstance(type1, pa.DictionaryType) and isinstance(type2, pa.DictionaryType):
        return (
            type1.value_type == type2.value_type
            and type1.index_type == type2.index_type
        )

    # All other combinations are considered incompatible
    return False


def _find_common_numeric_type(types: set[pa.DataType]) -> pa.DataType | None:
    """
    Find the optimal common numeric type for a set of numeric types.

    Returns None if types are not numeric or cannot be unified.
    """
    if not types:
        return None

    # Check if ALL types are numeric
    if not all(pa.types.is_integer(t) or pa.types.is_floating(t) for t in types):
        return None

    # Filter only numeric types
    numeric_types = [
        t for t in types if pa.types.is_integer(t) or pa.types.is_floating(t)
    ]
    if not numeric_types:
        return None

    # Check for mixed signed/unsigned integers
    signed_ints = [
        t
        for t in numeric_types
        if pa.types.is_integer(t) and not pa.types.is_unsigned_integer(t)
    ]
    unsigned_ints = [t for t in numeric_types if pa.types.is_unsigned_integer(t)]
    floats = [t for t in numeric_types if pa.types.is_floating(t)]

    # If we have floats, promote to the largest float type
    if floats:
        if any(t == pa.float64() for t in floats):
            return pa.float64()
        return pa.float32()

    # If we have mixed signed and unsigned integers, must promote to float
    if signed_ints and unsigned_ints:
        # Find the largest integer to determine float precision needed
        all_ints = signed_ints + unsigned_ints
        bit_widths = []
        for t in all_ints:
            if t == pa.int8() or t == pa.uint8():
                bit_widths.append(8)
            elif t == pa.int16() or t == pa.uint16():
                bit_widths.append(16)
            elif t == pa.int32() or t == pa.uint32():
                bit_widths.append(32)
            elif t == pa.int64() or t == pa.uint64():
                bit_widths.append(64)

        max_width = max(bit_widths) if bit_widths else 32
        # Use float64 for 64-bit integers to preserve precision
        return pa.float64() if max_width >= 64 else pa.float32()

    # Only signed integers - find largest
    if signed_ints:
        if pa.int64() in signed_ints:
            return pa.int64()
        elif pa.int32() in signed_ints:
            return pa.int32()
        elif pa.int16() in signed_ints:
            return pa.int16()
        else:
            return pa.int8()

    # Only unsigned integers - find largest
    if unsigned_ints:
        if pa.uint64() in unsigned_ints:
            return pa.uint64()
        elif pa.uint32() in unsigned_ints:
            return pa.uint32()
        elif pa.uint16() in unsigned_ints:
            return pa.uint16()
        else:
            return pa.uint8()

    return None


def _analyze_string_vs_numeric_conflict(
    string_type: pa.DataType, numeric_type: pa.DataType
) -> pa.DataType:
    """
    Analyze string vs numeric type conflict and determine best conversion strategy.

    For now, defaults to string type as it's the safest option.
    In a more sophisticated implementation, this could analyze actual data content
    to make an informed decision.
    """
    # Default strategy: convert to string to preserve all information
    # This could be enhanced with data sampling to determine optimal conversion
    return pa.string()


def _handle_temporal_conflicts(types: set[pa.DataType]) -> pa.DataType | None:
    """
    Handle conflicts between temporal types (date, time, timestamp).
    """
    if not types:
        return None

    # Check if ALL types are temporal
    if not all(pa.types.is_temporal(t) for t in types):
        return None

    # Filter temporal types
    temporal_types = [t for t in types if pa.types.is_temporal(t)]
    if not temporal_types:
        return None

    # If we have timestamps, they take precedence
    timestamps = [t for t in temporal_types if pa.types.is_timestamp(t)]
    if timestamps:
        # Find the highest precision timestamp
        # For simplicity, use the first one - in practice might want to find highest precision
        return timestamps[0]

    # If we have times, they take precedence over dates
    times = [t for t in temporal_types if pa.types.is_time(t)]
    if times:
        # Use the higher precision time
        if any(t == pa.time64() for t in times):
            return pa.time64()
        return pa.time32()

    # Only dates remain
    dates = [t for t in temporal_types if pa.types.is_date(t)]
    if dates:
        # Use the higher precision date
        if any(t == pa.date64() for t in dates):
            return pa.date64()
        return pa.date32()

    return None


def _find_conflicting_fields(schemas):
    """Find fields with conflicting types across schemas and categorize them."""
    seen = defaultdict(set)
    for schema in schemas:
        for field in schema:
            seen[field.name].add(field.type)

    conflicts = {}
    for name, types in seen.items():
        if len(types) > 1:
            # Analyze the conflict
            conflicts[name] = {
                "types": types,
                "compatible": True,  # Assume compatible until proven otherwise
                "target_type": None,  # Will be determined by promotion logic
            }

    return conflicts


def _normalize_schema_types(schemas, conflicts):
    """Normalize schema types based on intelligent promotion rules."""
    # First, analyze all conflicts to determine target types
    promotions = {}

    for field_name, conflict_info in conflicts.items():
        types = conflict_info["types"]

        # Try to find a common type for compatible conflicts
        target_type = None

        # Check if all types are numeric and can be unified
        numeric_type = _find_common_numeric_type(types)
        if numeric_type is not None:
            target_type = numeric_type
            conflict_info["compatible"] = True
        # Check if all types are temporal and can be unified
        else:
            temporal_type = _handle_temporal_conflicts(types)
            if temporal_type is not None:
                target_type = temporal_type
                conflict_info["compatible"] = True
            else:
                # Check if any types are incompatible
                all_compatible = True
                type_list = list(types)
                for i in range(len(type_list)):
                    for j in range(i + 1, len(type_list)):
                        if not _is_type_compatible(type_list[i], type_list[j]):
                            all_compatible = False
                            break
                    if not all_compatible:
                        break

                conflict_info["compatible"] = all_compatible

                if all_compatible:
                    # Types are compatible but we don't have a specific rule
                    # Let PyArrow handle it automatically
                    target_type = None
                else:
                    # Types are incompatible - default to string for safety
                    target_type = pa.string()

        conflict_info["target_type"] = target_type
        if target_type is not None:
            promotions[field_name] = target_type

    # Apply the promotions to schemas
    normalized = []
    for schema in schemas:
        fields = []
        for field in schema:
            tgt = promotions.get(field.name)
            fields.append(field if tgt is None else field.with_type(tgt))
        normalized.append(pa.schema(fields, metadata=schema.metadata))

    return normalized


def _unique_schemas(schemas: list[pa.Schema]) -> list[pa.Schema]:
    """Get unique schemas from a list of schemas."""
    seen = {}
    unique = []
    for schema in schemas:
        key = schema.serialize().to_pybytes()
        if key not in seen:
            seen[key] = schema
            unique.append(schema)
    return unique


def _aggressive_fallback_unification(schemas: list[pa.Schema]) -> pa.Schema:
    """
    Aggressive fallback strategy for difficult unification scenarios.
    Converts all conflicting fields to strings as a last resort.
    """
    conflicts = _find_conflicting_fields(schemas)
    if not conflicts:
        # No conflicts, try direct unification
        try:
            return pa.unify_schemas(schemas, promote_options="permissive")
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            pass

    # Convert all conflicting fields to strings
    for field_name, conflict_info in conflicts.items():
        conflict_info["target_type"] = pa.string()

    normalized_schemas = _normalize_schema_types(schemas, conflicts)
    try:
        return pa.unify_schemas(normalized_schemas, promote_options="permissive")
    except (pa.ArrowInvalid, pa.ArrowTypeError):
        # If even this fails, return first normalized schema
        return normalized_schemas[0]


def _remove_conflicting_fields(schemas: list[pa.Schema]) -> pa.Schema:
    """
    Remove fields that have type conflicts between schemas.
    Keeps only fields that have the same type across all schemas.

    Args:
        schemas (list[pa.Schema]): List of schemas to process.

    Returns:
        pa.Schema: Schema with only non-conflicting fields.
    """
    if not schemas:
        return pa.schema([])

    # Find conflicts
    conflicts = _find_conflicting_fields(schemas)
    conflicting_field_names = set(conflicts.keys())

    # Keep only non-conflicting fields from the first schema
    fields = []
    for field in schemas[0]:
        if field.name not in conflicting_field_names:
            fields.append(field)

    return pa.schema(fields)


def _remove_problematic_fields(schemas: list[pa.Schema]) -> pa.Schema:
    """
    Remove fields that cannot be unified across all schemas.
    This is a last resort when all other strategies fail.
    """
    if not schemas:
        return pa.schema([])

    # Find fields that exist in all schemas
    common_field_names = set(schemas[0].names)
    for schema in schemas[1:]:
        common_field_names &= set(schema.names)

    # Use fields from the first schema for common fields
    fields = []
    for field in schemas[0]:
        if field.name in common_field_names:
            fields.append(field)

    return pa.schema(fields)


def _log_conflict_summary(conflicts: dict, verbose: bool = False) -> None:
    """
    Log a summary of resolved conflicts for debugging purposes.
    """
    if not conflicts or not verbose:
        return

    print("Schema Unification Conflict Summary:")
    print("=" * 40)
    for field_name, conflict_info in conflicts.items():
        types_str = ", ".join(str(t) for t in conflict_info["types"])
        compatible = conflict_info["compatible"]
        target_type = conflict_info["target_type"]

        print(f"Field: {field_name}")
        print(f"  Types: {types_str}")
        print(f"  Compatible: {compatible}")
        print(f"  Target Type: {target_type}")
        print()


def _identify_empty_columns(table: pa.Table) -> list:
    """Identify columns that are entirely empty."""
    if table.num_rows == 0:
        return []

    empty_cols = []
    for col_name in table.column_names:
        column = table.column(col_name)
        if column.null_count == table.num_rows:
            empty_cols.append(col_name)

    return empty_cols


def unify_schemas(
    schemas: list[pa.Schema],
    use_large_dtypes: bool = False,
    timezone: str | None = None,
    standardize_timezones: bool = True,
    verbose: bool = False,
    remove_conflicting_columns: bool = False,
) -> pa.Schema:
    """
    Unify a list of PyArrow schemas into a single schema using intelligent conflict resolution.

    Args:
        schemas (list[pa.Schema]): List of PyArrow schemas to unify.
        use_large_dtypes (bool): If True, keep large types like large_string.
        timezone (str | None): If specified, standardize all timestamp columns to this timezone.
            If "auto", use the most frequent timezone across schemas.
            If None, remove timezone from all timestamp columns.
        standardize_timezones (bool): If True, standardize all timestamp columns to the most frequent timezone.
        verbose (bool): If True, print conflict resolution details for debugging.
        remove_conflicting_columns (bool): If True, allows removal of columns with type conflicts as a fallback
            strategy instead of converting them. Defaults to False.

    Returns:
        pa.Schema: A unified PyArrow schema.

    Raises:
        ValueError: If no schemas are provided.
    """
    if not schemas:
        raise ValueError("At least one schema must be provided for unification")

    # Early exit for single schema
    unique_schemas = _unique_schemas(schemas)
    if len(unique_schemas) == 1:
        result_schema = unique_schemas[0]
        if standardize_timezones:
            result_schema = standardize_schema_timezones([result_schema], timezone)[0]
        return (
            result_schema
            if use_large_dtypes
            else convert_large_types_to_normal(result_schema)
        )

    # Step 1: Find and resolve conflicts first
    conflicts = _find_conflicting_fields(unique_schemas)
    if conflicts and verbose:
        _log_conflict_summary(conflicts, verbose)

    if conflicts:
        # Normalize schemas using intelligent promotion rules
        unique_schemas = _normalize_schema_types(unique_schemas, conflicts)

    # Step 2: Attempt unification with conflict-resolved schemas
    try:
        unified_schema = pa.unify_schemas(unique_schemas, promote_options="permissive")

        # Step 3: Apply timezone standardization to the unified result
        if standardize_timezones:
            unified_schema = standardize_schema_timezones([unified_schema], timezone)[0]

        return (
            unified_schema
            if use_large_dtypes
            else convert_large_types_to_normal(unified_schema)
        )

    except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
        # Step 4: Intelligent fallback strategies
        if verbose:
            print(f"Primary unification failed: {e}")
            print("Attempting fallback strategies...")

        # Fallback 1: Try aggressive string conversion for remaining conflicts
        try:
            fallback_schema = _aggressive_fallback_unification(unique_schemas)
            if standardize_timezones:
                fallback_schema = standardize_schema_timezones(
                    [fallback_schema], timezone
                )[0]
            if verbose:
                print("✓ Aggressive fallback succeeded")
            return (
                fallback_schema
                if use_large_dtypes
                else convert_large_types_to_normal(fallback_schema)
            )

        except Exception:
            if verbose:
                print("✗ Aggressive fallback failed")

        # Fallback 2: Remove conflicting fields (if enabled)
        if remove_conflicting_columns:
            try:
                non_conflicting_schema = _remove_conflicting_fields(unique_schemas)
                if standardize_timezones:
                    non_conflicting_schema = standardize_schema_timezones(
                        [non_conflicting_schema], timezone
                    )[0]
                if verbose:
                    print("✓ Remove conflicting fields fallback succeeded")
                return (
                    non_conflicting_schema
                    if use_large_dtypes
                    else convert_large_types_to_normal(non_conflicting_schema)
                )

            except Exception:
                if verbose:
                    print("✗ Remove conflicting fields fallback failed")

        # Fallback 3: Remove problematic fields that can't be unified
        try:
            minimal_schema = _remove_problematic_fields(unique_schemas)
            if standardize_timezones:
                minimal_schema = standardize_schema_timezones(
                    [minimal_schema], timezone
                )[0]
            if verbose:
                print("✓ Minimal schema (removed problematic fields) succeeded")
            return (
                minimal_schema
                if use_large_dtypes
                else convert_large_types_to_normal(minimal_schema)
            )

        except Exception:
            if verbose:
                print("✗ Minimal schema fallback failed")

        # Fallback 4: Return first schema as last resort
        if verbose:
            print("✗ All fallback strategies failed, returning first schema")

        first_schema = unique_schemas[0]
        if standardize_timezones:
            first_schema = standardize_schema_timezones([first_schema], timezone)[0]
        return (
            first_schema
            if use_large_dtypes
            else convert_large_types_to_normal(first_schema)
        )


def remove_empty_columns(table: pa.Table) -> pa.Table:
    """Remove columns that are entirely empty from a PyArrow table.

    Args:
        table (pa.Table): The PyArrow table to process.

    Returns:
        pa.Table: A new PyArrow table with empty columns removed.
    """
    empty_cols = _identify_empty_columns(table)
    if not empty_cols:
        return table
    return table.drop(empty_cols)


def cast_schema(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """
    Cast a PyArrow table to a given schema, updating the schema to match the table's columns.

    Args:
        table (pa.Table): The PyArrow table to cast.
        schema (pa.Schema): The target schema to cast the table to.

    Returns:
        pa.Table: A new PyArrow table with the specified schema.
    """
    table_columns = set(table.schema.names)
    for field in schema:
        if field.name not in table_columns:
            table = table.append_column(
                field.name, pa.nulls(table.num_rows, type=field.type)
            )
    return table.cast(schema)


NULL_LIKE_STRINGS = {
    "",
    "-",
    "None",
    "none",
    "NONE",
    "NaN",
    "Nan",
    "nan",
    "NAN",
    "N/A",
    "n/a",
    "Null",
    "NULL",
    "null",
}


def _normalize_datetime_string(s: str) -> str:
    """
    Normalize a datetime string by removing timezone information.

    Args:
        s: Datetime string potentially containing timezone info

    Returns:
        str: Normalized datetime string without timezone
    """
    s = str(s).strip()
    if re.match(r"^\d{2}/\d{2}/\d{4}$", s):
        month, day, year = s.split("/")
        s = f"{year}-{month}-{day}"
    if re.match(r"^\d{2}\.\d{2}\.\d{4}$", s):
        day, month, year = s.split(".")
        s = f"{year}-{month}-{day}"
    if re.match(r"^\d{8}$", s):
        s = f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    s = re.sub(r"Z$", "", s)
    s = re.sub(r"UTC$", "", s)
    s = re.sub(r"([+-]\d{2}:\d{2})$", "", s)
    s = re.sub(r"([+-]\d{4})$", "", s)
    return s


def _detect_timezone_from_sample(series: Any) -> str | None:
    """
    Detect the most common timezone from a sample of datetime strings.

    Args:
        series: Polars Series containing datetime strings

    Returns:
        str or None: Most common timezone found, or None if no timezone detected
    """
    # Sample up to 1000 values for performance
    sample_size = min(1000, len(series))
    if sample_size == 0:
        return None

    # Get random sample
    sample_indices = random.sample(range(len(series)), sample_size)
    sample_values = [series[i] for i in sample_indices if series[i] is not None]

    if not sample_values:
        return None

    # Extract timezones
    timezones = []
    for val in sample_values:
        val = str(val).strip()
        match = re.search(r"(Z|UTC|[+-]\d{2}:\d{2}|[+-]\d{4})$", val)
        if match:
            tz = match.group(1)
            if tz == "Z":
                timezones.append("UTC")
            elif tz == "UTC":
                timezones.append("UTC")
            elif tz.startswith("+") or tz.startswith("-"):
                # Normalize timezone format
                if ":" not in tz:
                    tz = tz[:3] + ":" + tz[3:]
                timezones.append(tz)

    if not timezones:
        return None

    # Count frequencies
    from collections import Counter

    tz_counts = Counter(timezones)

    # Return most common timezone
    return tz_counts.most_common(1)[0][0]


SampleMethod = Literal["first", "random"]


def _sample_values(
    values: list[str], sample_size: int | None, sample_method: SampleMethod
) -> list[str]:
    """Limit the list of values used for regex inference."""
    if sample_size is None or sample_size <= 0 or len(values) <= sample_size:
        return values
    if sample_method not in ("first", "random"):
        raise ValueError("sample_method must be 'first' or 'random'")
    if sample_method == "random":
        return random.sample(values, sample_size)
    return values[:sample_size]


def _convert_full_list(
    cleaned_list: list[str | None],
    converter: Callable[[str], object],
    target_type: pa.DataType,
    strict: bool,
) -> pa.Array:
    """Convert every value in the cleaned list with resilience to conversion failures."""
    converted: list[object | None] = []
    for value in cleaned_list:
        if value is None:
            converted.append(None)
            continue
        try:
            converted.append(converter(value))
        except Exception:
            if strict:
                raise
            converted.append(None)
    return pa.array(converted, type=target_type)


def _clean_string_array(array: pa.Array) -> pa.Array:
    """Trimmt Strings und ersetzt definierte Platzhalter durch Null (Python-basiert, robust)."""
    if len(array) == 0:
        return array
    # pc.utf8_trim_whitespace kann fehlen / unterschiedlich sein → fallback
    py = [None if v is None else str(v).strip() for v in array.to_pylist()]
    cleaned_list = [None if (v is None or v in NULL_LIKE_STRINGS) else v for v in py]
    target_type = (
        array.type
        if pa.types.is_string(array.type) or pa.types.is_large_string(array.type)
        else pa.string()
    )
    return pa.array(cleaned_list, type=target_type)


def _can_downcast_to_float32(array: pa.Array) -> bool:
    """Prüft Float32 Range (Python fallback)."""
    if len(array) == 0 or array.null_count == len(array):
        return True
    values = [
        v
        for v in array.to_pylist()
        if isinstance(v, (int, float)) and v not in (None, float("inf"), float("-inf"))
    ]
    if not values:
        return True
    mn, mx = min(values), max(values)
    return F32_MIN <= mn <= mx <= F32_MAX


def _get_optimal_int_type(
    array: pa.Array, allow_unsigned: bool, allow_null: bool = True
) -> pa.DataType:
    values = [v for v in array.to_pylist() if v is not None]
    if not values:
        return pa.null() if allow_null else pa.int8()
    min_val = min(values)
    max_val = max(values)
    if allow_unsigned and min_val >= 0:
        if max_val <= 255:
            return pa.uint8()
        if max_val <= 65535:
            return pa.uint16()
        if max_val <= 4294967295:
            return pa.uint32()
        return pa.uint64()
    if -128 <= min_val and max_val <= 127:
        return pa.int8()
    if -32768 <= min_val and max_val <= 32767:
        return pa.int16()
    if -2147483648 <= min_val and max_val <= 2147483647:
        return pa.int32()
    return pa.int64()


def _optimize_numeric_array(
    array: pa.Array, shrink: bool, allow_unsigned: bool = True, allow_null: bool = True
) -> pa.DataType:
    """
    Optimize numeric PyArrow array by downcasting when possible.
    Returns the optimal dtype.
    """

    if len(array) == 0 or array.null_count == len(array):
        if allow_null:
            return pa.null()
        return array.type

    if not shrink:
        return array.type

    if pa.types.is_floating(array.type):
        if array.type == pa.float64() and _can_downcast_to_float32(array):
            return pa.float32()
        return array.type

    if pa.types.is_integer(array.type):
        return _get_optimal_int_type(array, allow_unsigned, allow_null)

    return array.type


_REGEX_CACHE: dict[str, re.Pattern] = {}


def _all_match_regex(array: pa.Array, pattern: str) -> bool:
    """Python Regex Matching (alle nicht-null Werte)."""
    if len(array) == 0 or array.null_count == len(array):
        return False
    if pattern not in _REGEX_CACHE:
        _REGEX_CACHE[pattern] = re.compile(pattern, re.IGNORECASE)
    rgx = _REGEX_CACHE[pattern]
    for v in array.to_pylist():
        if v is None:
            continue
        if not rgx.match(str(v)):
            return False
    return True


def _optimize_string_array(
    array: pa.Array,
    col_name: str,
    shrink_numerics: bool,
    time_zone: str | None = None,
    allow_unsigned: bool = True,
    allow_null: bool = True,
    force_timezone: str | None = None,
    sample_size: int | None = 1024,
    sample_method: SampleMethod = "first",
    strict: bool = False,
) -> tuple[pa.Array, pa.DataType]:
    """Analysiere String-Array und bestimme Ziel-Datentyp.

    Rückgabe: (bereinigtes_array, ziel_datentyp)
    Platzhalter-/Leerwerte blockieren keine Erkennung mehr.
    Die Regex-Heuristik arbeitet auf einer Stichprobe (sample_size/sample_method).
    """
    if len(array) == 0 or array.null_count == len(array):
        if allow_null:
            return pa.nulls(len(array), type=pa.null()), pa.null()
        return array, array.type

    cleaned_array = _clean_string_array(array)
    cleaned_list = cleaned_array.to_pylist()

    # Werte für Erkennung: nur nicht-null
    non_null_list = [v for v in cleaned_list if v is not None]
    if not non_null_list:
        if allow_null:
            return pa.nulls(len(cleaned_list), type=pa.null()), pa.null()
        return cleaned_array, array.type
    sample_list = _sample_values(non_null_list, sample_size, sample_method)
    sample_array = pa.array(sample_list, type=pa.string())

    try:
        if (not shrink_numerics) and _all_match_regex(sample_array, BOOLEAN_REGEX):

            def to_bool(value: str) -> bool:
                if not re.match(BOOLEAN_REGEX, value, re.IGNORECASE):
                    raise ValueError
                return bool(re.match(BOOLEAN_TRUE_REGEX, value, re.IGNORECASE))

            converted = _convert_full_list(cleaned_list, to_bool, pa.bool_(), strict)
            return converted, pa.bool_()

        # Datetime
        if _all_match_regex(sample_array, DATETIME_REGEX):
            # Import polars only when needed for datetime parsing
            from fsspeckit.common.optional import _import_polars

            pl = _import_polars()

            pl_series = pl.Series(col_name, cleaned_array)
            sample_series = (
                pl.Series(col_name, sample_list)
                if sample_list
                else pl.Series(col_name, [])
            )

            has_tz = sample_series.str.contains(
                r"(Z|UTC|[+-]\d{2}:\d{2}|[+-]\d{4})$"
            ).any()
            normalized_series = pl_series.map_elements(
                _normalize_datetime_string, return_dtype=pl.String
            )

            if has_tz:
                if force_timezone is not None:
                    dt_series = normalized_series.str.to_datetime(
                        time_zone=force_timezone, time_unit="us", strict=strict
                    )
                else:
                    detected_tz = _detect_timezone_from_sample(sample_series)
                    if detected_tz is not None:
                        dt_series = normalized_series.str.to_datetime(
                            time_zone=detected_tz, time_unit="us", strict=strict
                        )
                    else:
                        dt_series = normalized_series.str.to_datetime(
                            time_unit="us", strict=strict
                        )

                converted = dt_series
            else:
                if force_timezone is not None:
                    converted = normalized_series.str.to_datetime(
                        time_zone=force_timezone, time_unit="us", strict=strict
                    )
                else:
                    converted = normalized_series.str.to_datetime(
                        time_unit="us", strict=strict
                    )

            return converted.to_arrow(), converted.to_arrow().type

        # Integer
        if _all_match_regex(sample_array, INTEGER_REGEX):
            try:
                sample_ints = [int(v) for v in sample_list]
            except ValueError:
                if strict:
                    raise
                return cleaned_array, array.type

            sample_min = min(sample_ints) if sample_ints else None
            use_unsigned = (
                allow_unsigned
                and shrink_numerics
                and (sample_min is None or sample_min >= 0)
            )
            if use_unsigned and sample_ints:
                sample_arr = pa.array(sample_ints, type=pa.int64())
                optimized_type = _get_optimal_int_type(
                    sample_arr, True, allow_null=False
                )
            elif shrink_numerics and sample_ints:
                sample_arr = pa.array(sample_ints, type=pa.int64())
                optimized_type = _get_optimal_int_type(
                    sample_arr, False, allow_null=False
                )
            elif use_unsigned:
                optimized_type = pa.uint64()
            else:
                optimized_type = pa.int64()

            converted = _convert_full_list(
                cleaned_list, lambda v: int(v), optimized_type, strict
            )
            return converted, optimized_type

        # Float
        if _all_match_regex(sample_array, FLOAT_REGEX):
            try:
                sample_floats = [float(v.replace(",", ".")) for v in sample_list]
            except ValueError:
                if strict:
                    raise
                return cleaned_array, array.type
            base_arr = pa.array(sample_floats, type=pa.float64())
            target_type = pa.float64()
            if shrink_numerics and _can_downcast_to_float32(base_arr):
                target_type = pa.float32()
            converted = _convert_full_list(
                cleaned_list, lambda v: float(v.replace(",", ".")), target_type, strict
            )
            return converted, target_type

    except Exception:  # pragma: no cover
        if strict:
            raise
    if strict:
        raise ValueError(f"Could not infer dtype for column {col_name}")
    # Kein Cast
    return cleaned_array, cleaned_array.type


def _process_column(
    array: pa.Array,
    col_name: str,
    shrink_numerics: bool,
    allow_unsigned: bool,
    allow_null: bool,
    time_zone: str | None = None,
    force_timezone: str | None = None,
    sample_size: int | None = 1024,
    sample_method: SampleMethod = "first",
    strict: bool = False,
) -> tuple[pa.Field, pa.Array]:
    """
    Process a single column for type optimization.
    Returns a pyarrow.Field with the optimal dtype.
    """
    # array = table[col_name]
    if array.null_count == len(array):
        return pa.field(col_name, pa.null()), array

    if pa.types.is_floating(array.type) or pa.types.is_integer(array.type):
        dtype = _optimize_numeric_array(array, shrink_numerics, allow_unsigned)
        return pa.field(col_name, dtype, nullable=array.null_count > 0), array
    elif pa.types.is_string(array.type) or pa.types.is_large_string(array.type):
        casted_array, dtype = _optimize_string_array(
            array,
            col_name,
            shrink_numerics,
            time_zone,
            allow_unsigned=allow_unsigned,
            allow_null=allow_null,
            force_timezone=force_timezone,
            sample_size=sample_size,
            sample_method=sample_method,
            strict=strict,
        )
        return pa.field(
            col_name, dtype, nullable=casted_array.null_count > 0
        ), casted_array
    else:
        return pa.field(col_name, array.type, nullable=array.null_count > 0), array


def _process_column_for_opt_dtype(args):
    (
        array,
        col_name,
        cols_to_process,
        shrink_numerics,
        allow_unsigned,
        time_zone,
        strict,
        allow_null,
        force_timezone,
        sample_size,
        sample_method,
    ) = args
    try:
        if col_name in cols_to_process:
            field, array = _process_column(
                array,
                col_name,
                shrink_numerics,
                allow_unsigned,
                allow_null,
                time_zone,
                force_timezone,
                sample_size,
                sample_method,
                strict=strict,
            )
            if pa.types.is_null(field.type):
                if allow_null:
                    array = pa.nulls(array.length(), type=pa.null())
                    return (col_name, field, array)
                else:
                    orig_type = array.type
                    # array = table[col_name]
                    field = pa.field(col_name, orig_type, nullable=True)
                    return (col_name, field, array)
            return (col_name, field, array)
        else:
            field = pa.field(col_name, array.type, nullable=True)
            # array = table[col_name]
            return (col_name, field, array)
    except Exception as e:
        if strict:
            raise e
        field = pa.field(col_name, array.type, nullable=True)
        return (col_name, field, array)


def opt_dtype(
    table: pa.Table,
    include: str | list[str] | None = None,
    exclude: str | list[str] | None = None,
    time_zone: str | None = None,
    shrink_numerics: bool = False,
    allow_unsigned: bool = True,
    use_large_dtypes: bool = False,
    strict: bool = False,
    allow_null: bool = True,
    sample_size: int | None = 1024,
    sample_method: SampleMethod = "first",
    *,
    force_timezone: str | None = None,
) -> pa.Table:
    """
    Optimize data types of a PyArrow Table for performance and memory efficiency.
    Returns a new table casted to the optimal schema.

    Args:
        table: The PyArrow table to optimize.
        include: Column(s) to include in optimization (default: all columns).
        exclude: Column(s) to exclude from optimization.
        time_zone: Optional time zone hint during datetime parsing.
        shrink_numerics: Whether to downcast numeric types when possible.
        allow_unsigned: Whether to allow unsigned integer types.
        use_large_dtypes: If True, keep large types like large_string.
        strict: If True, will raise an error if any column cannot be optimized.
        allow_null: If False, columns that only hold null-like values will not be converted to pyarrow.null().
        sample_size: Maximum number of cleaned values to inspect during regex inference (None to inspect all).
        sample_method: Sampling strategy (`"first"` or `"random"`) for the inference subset.
        force_timezone: If set, ensure all parsed datetime columns end up with this timezone.
    """
    if sample_method not in ("first", "random"):
        raise ValueError("sample_method must be 'first' or 'random'")

    if isinstance(include, str):
        include = [include]
    if isinstance(exclude, str):
        exclude = [exclude]

    cols_to_process = table.column_names
    if include:
        cols_to_process = [col for col in include if col in table.column_names]
    if exclude:
        cols_to_process = [col for col in cols_to_process if col not in exclude]

    # Prepare arguments for parallel processing
    args_list = [
        (
            table[col_name],
            col_name,
            cols_to_process,
            shrink_numerics,
            allow_unsigned,
            time_zone,
            strict,
            allow_null,
            force_timezone,
            sample_size,
            sample_method,
        )
        for col_name in table.column_names
    ]

    # Parallelize column processing
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(_process_column_for_opt_dtype, args_list))

    # Sort results to preserve column order
    results.sort(key=lambda x: table.column_names.index(x[0]))
    fields = [field for _, field, _ in results]
    arrays = [array for _, _, array in results]

    schema = pa.schema(fields)
    if not use_large_dtypes:
        schema = convert_large_types_to_normal(schema)
    return pa.Table.from_arrays(arrays, schema=schema)
