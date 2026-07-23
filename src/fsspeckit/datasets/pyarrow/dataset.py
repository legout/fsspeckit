"""PyArrow dataset operations including merge and maintenance helpers.

This module contains functions for dataset-level operations including:
- Dataset merging with various strategies
- Dataset statistics collection
- Dataset compaction and optimization
- Maintenance operations
"""

import time
from collections import defaultdict
from typing import Any, Callable, Iterable, Literal, cast

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from fsspec import AbstractFileSystem
from pyarrow.fs import FSSpecHandler, PyFileSystem

from fsspeckit.common.logging import get_logger
from fsspeckit.common.optional import _import_polars
from fsspeckit.datasets.pyarrow.memory import MemoryMonitor, MemoryPressureLevel

logger = get_logger(__name__)


def _arrow_is_in(values: Any, value_set: Any) -> Any:
    """Call Arrow's ``is_in`` kernel without relying on incomplete stubs."""
    return pc.call_function(
        "is_in", [values], options=pc.SetLookupOptions(value_set=value_set)
    )


class PerformanceMonitor:
    """Comprehensive performance monitoring and metrics collection.

    This class tracks various performance metrics including processing time,
    memory usage, throughput, and operation-specific metrics.
    """

    def __init__(
        self,
        max_pyarrow_mb: int = 2048,
        max_process_memory_mb: int | None = None,
        min_system_available_mb: int = 512,
    ):
        self.start_time = time.perf_counter()
        self.operation_breakdown: defaultdict[str, float] = defaultdict(float)
        self.memory_peak_mb = 0.0
        self.process_memory_peak_mb = 0.0
        self.files_processed = 0
        self.chunks_processed = 0
        self.total_rows_processed = 0
        self.total_bytes_processed = 0
        self.current_op: str | None = None
        self.op_start_time = 0.0

        self._memory_monitor = MemoryMonitor(
            max_pyarrow_mb=max_pyarrow_mb,
            max_process_memory_mb=max_process_memory_mb,
            min_system_available_mb=min_system_available_mb,
        )
        self.pressure_counts: dict[str, int] = defaultdict(int)
        self._last_status_time = 0.0

    def start_op(self, name: str):
        """Start tracking a specific operation phase."""
        if self.current_op:
            self.end_op()
        self.current_op = name
        self.op_start_time = time.perf_counter()
        self.track_memory()

    def end_op(self):
        """End tracking the current operation phase."""
        if self.current_op:
            duration = time.perf_counter() - self.op_start_time
            self.operation_breakdown[self.current_op] += duration
            self.current_op = None
        self.track_memory()

    def track_memory(self):
        """Track peak memory usage using MemoryMonitor."""
        now = time.perf_counter()
        # Avoid excessive psutil calls (cache for 100ms)
        if now - self._last_status_time < 0.1:
            return

        status = self._memory_monitor.get_memory_status()
        self._last_status_time = now

        # Track PyArrow peak
        current_pa_mb = status.get("pyarrow_allocated_mb", 0.0)
        if current_pa_mb > self.memory_peak_mb:
            self.memory_peak_mb = current_pa_mb

        # Track Process peak
        current_rss_mb = status.get("process_rss_mb", 0.0)
        if current_rss_mb > self.process_memory_peak_mb:
            self.process_memory_peak_mb = current_rss_mb

        # Track pressure level
        pressure = self._memory_monitor.check_memory_pressure()
        self.pressure_counts[pressure.value] += 1

    def get_memory_status(self) -> dict[str, float]:
        """Get current memory snapshot from MemoryMonitor."""
        return cast(dict[str, float], self._memory_monitor.get_memory_status())

    def get_metrics(
        self,
        total_rows_before: int,
        total_rows_after: int,
        total_bytes: int,
    ) -> dict[str, Any]:
        """Generate comprehensive performance metrics report.

        Args:
            total_rows_before: Total rows in the dataset before operation.
            total_rows_after: Total rows in the dataset after operation.
            total_bytes: Total size of the dataset in bytes.

        Returns:
            Dictionary containing performance metrics.
        """
        # Force a final memory track to ensure peaks are captured
        self._last_status_time = 0.0
        self.track_memory()

        total_time = time.perf_counter() - self.start_time
        rows_removed = total_rows_before - total_rows_after
        dedup_efficiency = (
            (rows_removed / total_rows_before) if total_rows_before > 0 else 0.0
        )

        metrics = {
            "total_process_time_sec": total_time,
            "memory_peak_mb": self.memory_peak_mb,
            "process_memory_peak_mb": self.process_memory_peak_mb,
            "throughput_mb_sec": (total_bytes / (1024 * 1024)) / total_time
            if total_time > 0
            else 0.0,
            "rows_per_sec": total_rows_before / total_time if total_time > 0 else 0.0,
            "files_processed": self.files_processed,
            "chunks_processed": self.chunks_processed,
            "dedup_efficiency": dedup_efficiency,
            "operation_breakdown": dict(self.operation_breakdown),
            "memory_pressure_stats": dict(self.pressure_counts),
        }

        # Include system info if available
        status = self._memory_monitor.get_memory_status()
        if "system_available_mb" in status:
            metrics["system_available_mb"] = status["system_available_mb"]

        return metrics


def _table_drop_duplicates(
    table: pa.Table,
    subset: list[str] | None = None,
    keep: Literal["first", "last"] = "first",
) -> pa.Table:
    """Safely drop duplicates from a PyArrow Table.

    Uses Table.drop_duplicates if available (PyArrow >= 12.0.0),
    otherwise falls back to Polars.
    """
    if hasattr(table, "drop_duplicates"):
        # Note: PyArrow Table.drop_duplicates only supports keep='first' in some versions
        # but the kwarg is supported in newer ones.
        try:
            return table.drop_duplicates(subset=subset, keep=keep)
        except (TypeError, ValueError):
            # Fallback for versions that don't support keep kwarg
            if keep == "first":
                return table.drop_duplicates(subset=subset)
            # if last requested but not supported, we'll fall back to Polars below

    # Fallback to Polars for older/weird PyArrow environments or if keep='last' not supported
    pl = _import_polars()

    df = pl.from_arrow(table)
    assert isinstance(df, pl.DataFrame)
    return df.unique(subset=subset, keep=keep).to_arrow()


def _make_struct_safe(table: pa.Table, columns: list[str]) -> pa.Array:
    """Safely create a struct array from table columns.

    Handles ChunkedArrays by combining them.
    """
    arrays = [table[c].combine_chunks() for c in columns]
    return pa.StructArray.from_arrays(arrays, names=columns)


def _create_composite_key_array(table: pa.Table, key_columns: list[str]) -> pa.Array:
    """Create a StructArray representing composite keys for efficient comparison.

    Handles ChunkedArrays by combining them. Uses pa.StructArray.from_arrays()
    to stay in Arrow space.

    Args:
        table: PyArrow table containing the key columns.
        key_columns: List of column names to include in the composite key.

    Returns:
        A StructArray where each element represents a composite key.
    """
    if not key_columns:
        raise ValueError("key_columns cannot be empty")

    # Ensure all key columns exist
    missing = [c for c in key_columns if c not in table.column_names]
    if missing:
        raise KeyError(f"Key columns not found in table: {missing}")

    # Combine chunks for each key column and create StructArray
    # This keeps operations in Arrow space for efficient comparison
    try:
        if len(key_columns) == 1:
            return table[key_columns[0]].combine_chunks()

        arrays = [table[c].combine_chunks() for c in key_columns]
        return pa.StructArray.from_arrays(arrays, names=key_columns)
    except Exception as e:
        logger.error("Failed to create composite key array: %s", e)
        raise TypeError(
            f"Failed to create composite key from columns {key_columns}. "
            f"Ensure columns have compatible types for StructArray creation. "
            f"Error: {e}"
        ) from e


def _create_fallback_key_array(table: pa.Table, key_columns: list[str]) -> pa.Array:
    """Create an efficient, null-safe representation of composite keys.

    This is used when StructArray or Join operations fail. It prefers
    memory-efficient binary views to avoid expensive string conversions.

    Null-safety is achieved with a leading discriminator byte (0x00 for null,
    0x01 for non-null) so a real value can never collide with a null marker,
    satisfying the collision-free requirement without an unescaped magic
    placeholder.

    Args:
        table: PyArrow table containing the key columns.
        key_columns: List of column names to include in the composite key.

    Returns:
        An array where each element represents a composite key.
    """
    if not key_columns:
        raise ValueError("key_columns cannot be empty")

    binary_cols = []
    for col_name in key_columns:
        col = table.column(col_name).combine_chunks()
        t = col.type
        is_null_col = col.is_null()

        # Performance optimization: Use zero-copy binary view for fixed-width types
        # instead of casting to strings.
        try:
            # Check if type has bit_width attribute and is a fixed-width type
            has_bit_width = False
            bit_width_val = 0
            try:
                bit_width_val = t.bit_width
                has_bit_width = True
            except (AttributeError, ValueError):
                has_bit_width = False

            if (
                has_bit_width
                and bit_width_val > 0
                and (
                    pa.types.is_integer(t)
                    or pa.types.is_floating(t)
                    or pa.types.is_timestamp(t)
                    or pa.types.is_duration(t)
                    or pa.types.is_date(t)
                )
            ):
                # zero-copy view as binary, then cast to variable binary for join compatibility
                bin_col = pc.cast(col.view(pa.binary(bit_width_val // 8)), pa.binary())
            else:
                # Fallback to binary cast for others (e.g. strings are already binary-compatible)
                bin_col = pc.cast(col, pa.binary())
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
            # Last resort: cast to string then binary
            bin_col = pc.cast(pc.cast(col, pa.string()), pa.binary())

        # Collision-free null encoding: prefix 0x01 for non-null, 0x00 for null.
        # Nulls are first replaced with an empty binary so the tag byte is the
        # sole discriminator; a real value can never equal a null marker.
        filled = pc.fill_null(bin_col, b"")
        tag = pc.call_function(
            "if_else", [is_null_col, pa.scalar(b"\x00"), pa.scalar(b"\x01")]
        )
        bin_col = pc.call_function(
            "binary_join_element_wise", [tag, filled, pa.scalar(b"")]
        )
        binary_cols.append(bin_col)

    if len(binary_cols) == 1:
        return binary_cols[0]

    # Join multiple binary keys with a delimiter
    return pc.call_function(
        "binary_join_element_wise", [*binary_cols, pa.scalar(b"\x1f")]
    )


def _table_has_nullable_keys(table: pa.Table, key_columns: list[str]) -> bool:
    """Return True if any key column has at least one null value."""
    for col in key_columns:
        if col not in table.column_names:
            continue
        if table.column(col).null_count > 0:
            return True
    return False


def _filter_by_key_membership_null_safe(
    table: pa.Table,
    key_columns: list[str],
    reference_keys: pa.Table,
    keep_matches: bool = True,
) -> pa.Table:
    """Null-safe membership filter using canonical key equality.

    Keys are canonicalized so that ``NULL`` matches ``NULL`` and ``NaN``
    matches ``NaN`` (matching native Arrow semantics), while non-null typed
    equality is preserved. This is the slower fallback path used only for
    batches that actually contain nullable keys.
    """
    from fsspeckit.core.merge import null_safe_key_set, null_safe_row_keys

    ref_set = null_safe_key_set(reference_keys, key_columns)
    row_keys = null_safe_row_keys(table, key_columns)
    mask = [key in ref_set for key in row_keys]
    if not keep_matches:
        mask = [not m for m in mask]
    return table.filter(pa.array(mask, type=pa.bool_()))


def _filter_by_key_membership(
    table: pa.Table,
    key_columns: list[str],
    reference_keys: pa.Table,
    keep_matches: bool = True,
) -> pa.Table:
    """Filter table rows based on key membership.

    Uses the fast native PyArrow semi/anti join when no key column contains
    nulls. When nullable keys are present, falls back to a null-safe Python
    path where ``NULL`` matches ``NULL`` (IS NOT DISTINCT FROM semantics),
    because PyArrow joins do not match null to null.

    Args:
        table: Table to filter.
        key_columns: List of column names to use as keys.
        reference_keys: Table containing the keys to match against.
        keep_matches: If True, keep rows present in reference_keys (semi-join).
            If False, keep rows NOT present in reference_keys (anti-join).

    Returns:
        Filtered PyArrow Table.
    """
    if not key_columns:
        return table

    # Null-safe slow path: PyArrow joins do not match null to null.
    if _table_has_nullable_keys(table, key_columns) or _table_has_nullable_keys(
        reference_keys, key_columns
    ):
        return _filter_by_key_membership_null_safe(
            table, key_columns, reference_keys, keep_matches
        )

    try:
        # We only need the key columns from reference_keys for the join
        ref_keys_only = reference_keys.select(key_columns)

        # Perform the join. PyArrow joins handle multi-column keys natively.
        join_type = "left semi" if keep_matches else "left anti"
        return table.join(ref_keys_only, keys=key_columns, join_type=join_type)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowKeyError) as e:
        logger.warning(
            "Primary join approach failed, falling back to efficient binary keys. "
            "This can happen with heterogeneous type combinations. Error: %s",
            e,
        )

        # Fallback mechanism using efficient binary keys (no nulls here, so
        # collision-free by construction).
        table_keys = _create_fallback_key_array(table, key_columns)
        ref_keys = _create_fallback_key_array(reference_keys, key_columns)

        # Use is_in for filtering. value_set must be an array or chunked array.
        mask = _arrow_is_in(table_keys, ref_keys)
        if not keep_matches:
            mask = pc.call_function("invert", [mask])

        return table.filter(mask)


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

    return cast(
        dict[str, Any],
        collect_dataset_stats(
            path=path,
            filesystem=filesystem,
            partition_filter=partition_filter,
        ),
    )


def _normalize_key_columns(key_columns: list[str] | str) -> list[str]:
    """Normalize key column specification to a list.

    Args:
        key_columns: Key columns as string or list

    Returns:
        List of key column names
    """
    if isinstance(key_columns, str):
        return [key_columns]
    return key_columns


def _ensure_pyarrow_filesystem(
    filesystem: AbstractFileSystem,
) -> PyFileSystem:
    """Ensure we have a PyArrow-compatible filesystem.

    Args:
        filesystem: fsspec filesystem

    Returns:
        PyArrow filesystem wrapper
    """
    if isinstance(filesystem, PyFileSystem):
        return filesystem

    handler = FSSpecHandler(filesystem)
    return PyFileSystem(handler)


def _load_source_table_pyarrow(
    source: str,
    filesystem: AbstractFileSystem,
    row_filter: Any = None,
    columns: list[str] | None = None,
) -> pa.Table:
    """Load a source table from a path.

    Args:
        source: Source path
        filesystem: Filesystem instance
        row_filter: Optional row filter
        columns: Optional column selection

    Returns:
        PyArrow table
    """
    pa_filesystem = _ensure_pyarrow_filesystem(filesystem)

    if source.endswith(".parquet"):
        return pq.read_table(
            source,
            filesystem=pa_filesystem,
            filters=row_filter,
            columns=columns,
        )
    else:
        # Assume it's a dataset directory
        dataset = ds.dataset(
            source,
            filesystem=pa_filesystem,
        )
        return dataset.to_table(filter=row_filter, columns=columns)


def process_in_chunks(
    dataset: ds.Dataset | pa.Table,
    chunk_size_rows: int = 1_000_000,
    max_memory_mb: int = 2048,
    enable_progress: bool = True,
    progress_callback: Callable[[int, int], None] | None = None,
    memory_monitor: MemoryMonitor | None = None,
) -> Iterable[pa.Table]:
    """Process a dataset or table in configurable chunks to avoid memory overflow.

    This function enables processing of datasets larger than available system memory
    by yielding data in manageable chunks. It monitors memory usage and enforces
    limits to prevent OOM errors.

    Args:
        dataset: PyArrow Dataset or Table to process.
        chunk_size_rows: Number of rows per chunk. Defaults to 1,000,000.
        max_memory_mb: Peak memory limit in MB. Defaults to 2048.
        enable_progress: Whether to track and report progress. Defaults to True.
        progress_callback: Optional callback function(rows_processed, total_rows).
        memory_monitor: Optional MemoryMonitor instance. If None, a new one is created.

    Yields:
        pa.Table: A chunk of data as a PyArrow Table.

    Raises:
        MemoryError: If peak memory usage exceeds max_memory_mb.
    """
    total_rows = (
        dataset.num_rows if isinstance(dataset, pa.Table) else dataset.count_rows()
    )
    rows_processed = 0

    # Initialize memory monitor
    monitor = memory_monitor or MemoryMonitor(max_pyarrow_mb=max_memory_mb)

    if isinstance(dataset, ds.Dataset):
        # For datasets, use a scanner with specified batch size
        scanner = dataset.scanner(batch_size=chunk_size_rows)
        batches = scanner.to_batches()
    else:
        # For tables, slice into chunks
        def _table_iterator():
            for i in range(0, total_rows, chunk_size_rows):
                yield dataset.slice(i, min(chunk_size_rows, total_rows - i))

        batches = _table_iterator()

    for batch in batches:
        # Ensure we have a Table for consistent processing
        chunk = (
            pa.Table.from_batches([batch])
            if isinstance(batch, pa.RecordBatch)
            else batch
        )

        # Monitor memory pressure
        pressure = monitor.check_memory_pressure()
        if pressure == MemoryPressureLevel.EMERGENCY:
            status = monitor.get_detailed_status()
            logger.error(f"Memory limit exceeded (EMERGENCY): {status}")
            raise MemoryError(f"Peak memory usage exceeded limit: {status}")

        yield chunk

        rows_processed += chunk.num_rows
        if enable_progress:
            if progress_callback:
                progress_callback(rows_processed, total_rows)
            else:
                logger.debug(
                    "Processed %d/%d rows (%.1f%%)",
                    rows_processed,
                    total_rows,
                    (rows_processed / total_rows) * 100 if total_rows > 0 else 100,
                )


def merge_upsert_pyarrow(
    existing: pa.Table | ds.Dataset,
    source: pa.Table,
    key_columns: list[str],
    chunk_size: int = 100_000,
    max_memory_mb: int = 1024,
    memory_monitor: MemoryMonitor | None = None,
    writer: pq.ParquetWriter | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
) -> pa.Table | None:
    """Perform UPSERT merge using PyArrow operations with streaming support.

    Args:
        existing: Existing data (Table or Dataset)
        source: Source data to merge (Table)
        key_columns: Columns to use as merge keys
        chunk_size: Number of rows per processing chunk
        max_memory_mb: Maximum PyArrow memory to use in MB
        memory_monitor: Optional MemoryMonitor for enhanced tracking
        writer: Optional ParquetWriter for streaming output
        progress_callback: Optional progress callback

    Returns:
        Merged Table if writer is None, else None
    """
    import pyarrow.compute as pc

    from fsspeckit.common.optional import _import_pyarrow

    # Align source schema with existing
    existing_schema = existing.schema
    pa_mod = _import_pyarrow()
    source_aligned = source
    for field in existing_schema:
        if field.name not in source_aligned.column_names:
            source_aligned = source_aligned.append_column(
                field.name, pa_mod.nulls(len(source_aligned), type=field.type)
            )
    source_aligned = source_aligned.select(existing_schema.names).cast(existing_schema)

    # Prepare source keys for filtering
    use_string_fallback = False
    if len(key_columns) == 1:
        source_keys = source_aligned.column(key_columns[0])
    else:
        try:
            # Primary approach: StructArray for vectorized matching
            source_keys = _create_composite_key_array(source_aligned, key_columns)
            # Test if is_in works with this StructArray
            if source_keys.length() > 0:
                _arrow_is_in(source_keys.slice(0, 1), source_keys.slice(0, 1))
        except Exception:
            use_string_fallback = True
            source_keys = _create_fallback_key_array(source_aligned, key_columns)

    def _process_chunk(chunk: pa.Table) -> pa.Table:
        if len(key_columns) == 1:
            chunk_keys = chunk.column(key_columns[0])
        elif use_string_fallback:
            chunk_keys = _create_fallback_key_array(chunk, key_columns)
        else:
            chunk_keys = _create_composite_key_array(chunk, key_columns)

        key_matches = _arrow_is_in(chunk_keys, source_keys)
        mask = pc.call_function("invert", [key_matches])
        return chunk.filter(mask)

    if writer:
        # Streaming mode
        for chunk in process_in_chunks(
            existing,
            chunk_size,
            max_memory_mb,
            progress_callback=progress_callback,
            memory_monitor=memory_monitor,
        ):
            filtered = _process_chunk(chunk)
            if filtered.num_rows > 0:
                writer.write_table(filtered)
        writer.write_table(source_aligned)
        return None
    else:
        # Batch mode
        if isinstance(existing, pa.Table) and existing.num_rows <= chunk_size:
            filtered_existing = _process_chunk(existing)
        else:
            chunks = []
            for chunk in process_in_chunks(
                existing,
                chunk_size,
                max_memory_mb,
                progress_callback=progress_callback,
                memory_monitor=memory_monitor,
            ):
                chunks.append(_process_chunk(chunk))
            filtered_existing = pa_mod.concat_tables(
                chunks, promote_options="permissive"
            )
        return pa_mod.concat_tables(
            [filtered_existing, source_aligned], promote_options="permissive"
        )


def merge_update_pyarrow(
    existing: pa.Table | ds.Dataset,
    source: pa.Table,
    key_columns: list[str],
    chunk_size: int = 100_000,
    max_memory_mb: int = 1024,
    memory_monitor: MemoryMonitor | None = None,
    writer: pq.ParquetWriter | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
) -> pa.Table | None:
    """Perform UPDATE merge using PyArrow operations with streaming support.

    Args:
        existing: Existing data (Table or Dataset)
        source: Source data to merge (Table)
        key_columns: Columns to use as merge keys
        chunk_size: Number of rows per processing chunk
        max_memory_mb: Maximum PyArrow memory to use in MB
        memory_monitor: Optional MemoryMonitor for enhanced tracking
        writer: Optional ParquetWriter for streaming output
        progress_callback: Optional progress callback

    Returns:
        Merged Table if writer is None, else None
    """
    from fsspeckit.common.optional import _import_pyarrow

    # Align source schema with existing
    existing_schema = existing.schema
    pa_mod = _import_pyarrow()
    source_aligned = source
    for field in existing_schema:
        if field.name not in source_aligned.column_names:
            source_aligned = source_aligned.append_column(
                field.name, pa_mod.nulls(len(source_aligned), type=field.type)
            )
    source_aligned = source_aligned.select(existing_schema.names).cast(existing_schema)

    # Pass 1: find which source rows are in existing
    existing_keys_table = None
    for chunk in process_in_chunks(
        existing,
        chunk_size,
        max_memory_mb,
        progress_callback=progress_callback,
        memory_monitor=memory_monitor,
    ):
        chunk_keys = chunk.select(key_columns)
        # Deduplicate within chunk to keep existing_keys_table smaller
        chunk_keys = _table_drop_duplicates(chunk_keys, subset=key_columns)

        if existing_keys_table is None:
            existing_keys_table = chunk_keys
        else:
            # Only add keys we haven't seen yet
            new_keys = _filter_by_key_membership(
                chunk_keys, key_columns, existing_keys_table, keep_matches=False
            )
            if new_keys.num_rows > 0:
                existing_keys_table = pa_mod.concat_tables(
                    [existing_keys_table, new_keys]
                )

    # Now filter source to keep only rows that exist in 'existing'
    if existing_keys_table is None:
        source_in_existing = source_aligned.schema.empty_table()
    else:
        source_in_existing = _filter_by_key_membership(
            source_aligned, key_columns, existing_keys_table, keep_matches=True
        )

    def _process_chunk_existing(chunk: pa.Table) -> pa.Table:
        # Rows in existing NOT in source
        return _filter_by_key_membership(
            chunk, key_columns, source_aligned, keep_matches=False
        )

    if writer:
        # Pass 2: Streaming output
        for chunk in process_in_chunks(
            existing,
            chunk_size,
            max_memory_mb,
            progress_callback=progress_callback,
            memory_monitor=memory_monitor,
        ):
            filtered = _process_chunk_existing(chunk)
            if filtered.num_rows > 0:
                writer.write_table(filtered)
        writer.write_table(source_in_existing)
        return None
    else:
        # Pass 2: Batch mode
        chunks = []
        for chunk in process_in_chunks(
            existing,
            chunk_size,
            max_memory_mb,
            progress_callback=progress_callback,
            memory_monitor=memory_monitor,
        ):
            chunks.append(_process_chunk_existing(chunk))
        filtered_existing = pa_mod.concat_tables(chunks, promote_options="permissive")
        return pa_mod.concat_tables(
            [filtered_existing, source_in_existing], promote_options="permissive"
        )
