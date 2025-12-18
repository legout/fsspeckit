"""
Backend-neutral merge layer for parquet dataset operations.

This module provides shared functionality for merge operations used by both
DuckDB and PyArrow merge implementations.

Key responsibilities:
1. Merge validation and normalization
2. Strategy semantics and definitions
3. Key validation and schema compatibility checking
4. Shared statistics calculation
5. NULL-key detection and edge case handling
6. Common merge planning and execution patterns
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Literal

if TYPE_CHECKING:
    import pyarrow as pa
    from fsspeckit.core.incremental import MergeResult, MergeFileMetadata

# Type for rewrite modes
RewriteMode = Literal["full", "incremental"]


class MergeStrategy(Enum):
    """Supported merge strategies with consistent semantics across backends."""

    UPSERT = "upsert"
    """Insert new records, update existing records."""

    INSERT = "insert"
    """Insert only new records, ignore existing records."""

    UPDATE = "update"
    """Update only existing records, ignore new records."""

    FULL_MERGE = "full_merge"
    """Insert, update, and delete (full sync with source)."""

    DEDUPLICATE = "deduplicate"
    """Remove duplicates from source, then upsert."""


def validate_rewrite_mode_compatibility(
    strategy: MergeStrategy,
    rewrite_mode: RewriteMode,
) -> None:
    """
    Validate that rewrite_mode is compatible with the chosen strategy.

    Args:
        strategy: Merge strategy to validate.
        rewrite_mode: Rewrite mode to validate.

    Raises:
        ValueError: If rewrite_mode is incompatible with strategy.
    """
    if rewrite_mode == "incremental":
        # Incremental rewrite is only supported for upsert and update strategies
        if strategy not in [MergeStrategy.UPSERT, MergeStrategy.UPDATE]:
            raise ValueError(
                f"rewrite_mode='incremental' is not supported for strategy='{strategy.value}'. "
                f"Incremental rewrite is only supported for 'upsert' and 'update' strategies."
            )


@dataclass
class MergePlan:
    """Plan for executing a merge operation."""

    strategy: MergeStrategy
    key_columns: list[str]
    source_count: int
    target_exists: bool
    rewrite_mode: RewriteMode = "full"
    dedup_order_by: list[str] | None = None

    # Validation results
    key_columns_valid: bool = True
    schema_compatible: bool = True
    null_keys_detected: bool = False

    # Strategy-specific settings
    allow_target_empty: bool = True
    allow_source_empty: bool = True

    def __post_init__(self) -> None:
        if not self.key_columns:
            raise ValueError("key_columns must be non-empty")
        if self.source_count < 0:
            raise ValueError("source_count must be >= 0")
        if self.strategy == MergeStrategy.DEDUPLICATE and not self.dedup_order_by:
            # Default to key columns for dedup ordering if not specified
            self.dedup_order_by = list(self.key_columns)

        # Validate rewrite_mode compatibility
        validate_rewrite_mode_compatibility(self.strategy, self.rewrite_mode)


@dataclass
class MergeStats:
    """Canonical statistics structure for merge operations."""

    strategy: MergeStrategy
    source_count: int
    target_count_before: int
    target_count_after: int

    inserted: int
    updated: int
    deleted: int

    total_processed: int = 0  # Total rows actually processed

    def __post_init__(self) -> None:
        if self.source_count < 0:
            raise ValueError("source_count must be >= 0")
        if self.target_count_before < 0:
            raise ValueError("target_count_before must be >= 0")
        if self.target_count_after < 0:
            raise ValueError("target_count_after must be >= 0")
        if self.inserted < 0:
            raise ValueError("inserted must be >= 0")
        if self.updated < 0:
            raise ValueError("updated must be >= 0")
        if self.deleted < 0:
            raise ValueError("deleted must be >= 0")

        # Set total_processed if not already set
        if self.total_processed == 0:
            self.total_processed = self.inserted + self.updated

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary format for backward compatibility."""
        return {
            "inserted": self.inserted,
            "updated": self.updated,
            "deleted": self.deleted,
            "total": self.target_count_after,
            "source_count": self.source_count,
            "target_count_before": self.target_count_before,
            "target_count_after": self.target_count_after,
            "total_processed": self.total_processed,
            "strategy": self.strategy.value,
        }


def normalize_key_columns(key_columns: list[str] | str) -> list[str]:
    """
    Normalize key columns to a consistent list format.

    Args:
        key_columns: Key column(s) as string or list of strings.

    Returns:
        List of key column names.

    Raises:
        ValueError: If key_columns is empty or contains invalid values.
    """
    if isinstance(key_columns, str):
        if not key_columns.strip():
            raise ValueError("key_columns cannot be empty string")
        return [key_columns.strip()]

    if not key_columns:
        raise ValueError("key_columns cannot be empty")

    # Filter and validate each column name
    normalized = []
    for col in key_columns:
        if not isinstance(col, str):
            raise ValueError(f"key_columns must be strings, got {type(col)}")
        stripped = col.strip()
        if not stripped:
            raise ValueError("key_columns cannot contain empty strings")
        normalized.append(stripped)

    if not normalized:
        raise ValueError("key_columns cannot be empty after normalization")

    return normalized


def validate_merge_inputs(
    source_schema: pa.Schema,
    target_schema: pa.Schema | None,
    key_columns: list[str],
    strategy: MergeStrategy,
) -> MergePlan:
    """
    Validate merge inputs and create a merge plan.

    Args:
        source_schema: Schema of the source data.
        target_schema: Schema of the target dataset, None if target doesn't exist.
        key_columns: List of key column names for matching records.
        strategy: Merge strategy to use.

    Returns:
        MergePlan with validation results and execution details.

    Raises:
        ValueError: If validation fails with specific error messages.
    """
    # Normalize key columns
    normalized_keys = normalize_key_columns(key_columns)

    # Check key columns exist in source
    source_columns = set(source_schema.names)
    missing_source_keys = [col for col in normalized_keys if col not in source_columns]
    if missing_source_keys:
        raise ValueError(
            f"Key column(s) missing from source: {', '.join(missing_source_keys)}. "
            f"Available columns: {', '.join(sorted(source_columns))}"
        )

    # Initialize validation flags
    keys_valid = True
    schema_compatible = True
    null_keys_possible = False

    # Check target schema if it exists
    target_exists = target_schema is not None
    if target_exists:
        target_columns = set(target_schema.names)

        # Check key columns exist in target
        missing_target_keys = [
            col for col in normalized_keys if col not in target_columns
        ]
        if missing_target_keys:
            raise ValueError(
                f"Key column(s) missing from target: {', '.join(missing_target_keys)}. "
                f"Available columns: {', '.join(sorted(target_columns))}"
            )

        # Check schema compatibility
        for field in source_schema:
            if field.name in target_columns:
                target_field = target_schema.field(field.name)
                if field.type != target_field.type:
                    schema_compatible = False
                    break

        # Check for column mismatches
        source_only = source_columns - target_columns
        target_only = target_columns - source_columns
        if source_only or target_only:
            schema_compatible = False

    # Check if NULL keys are possible based on schema nullability
    for key_col in normalized_keys:
        source_field = source_schema.field(key_col)
        if source_field.nullable:
            null_keys_possible = True
            break

    # Determine if empty target/source are allowed based on strategy
    allow_target_empty = True  # All strategies allow empty target
    allow_source_empty = strategy != MergeStrategy.UPDATE  # UPDATE needs source records

    return MergePlan(
        strategy=strategy,
        key_columns=normalized_keys,
        source_count=0,  # Will be set by caller
        target_exists=target_exists,
        dedup_order_by=None,  # Will be set by caller if needed
        key_columns_valid=keys_valid,
        schema_compatible=schema_compatible,
        null_keys_detected=null_keys_possible,
        allow_target_empty=allow_target_empty,
        allow_source_empty=allow_source_empty,
    )


def check_null_keys(
    source_table: pa.Table,
    target_table: pa.Table | None,
    key_columns: list[str],
) -> None:
    """
    Check for NULL values in key columns.

    Args:
        source_table: Source data table.
        target_table: Target data table, None if target doesn't exist.
        key_columns: List of key column names.

    Raises:
        ValueError: If NULL values found in key columns.
    """
    # Check source for NULL keys
    for key_col in key_columns:
        source_col = source_table.column(key_col)
        if source_col.null_count > 0:
            raise ValueError(
                f"Key column '{key_col}' contains {source_col.null_count} NULL values in source. "
                f"Key columns must not have NULLs."
            )

    # Check target for NULL keys if it exists
    if target_table is not None:
        for key_col in key_columns:
            target_col = target_table.column(key_col)
            if target_col.null_count > 0:
                raise ValueError(
                    f"Key column '{key_col}' contains {target_col.null_count} NULL values in target. "
                    f"Key columns must not have NULLs."
                )


def calculate_merge_stats(
    strategy: MergeStrategy,
    source_count: int,
    target_count_before: int,
    target_count_after: int,
) -> MergeStats:
    """
    Calculate merge operation statistics.

    Args:
        strategy: Merge strategy that was used.
        source_count: Number of rows in source data.
        target_count_before: Number of rows in target before merge.
        target_count_after: Number of rows in target after merge.

    Returns:
        MergeStats with calculated statistics.
    """
    stats = MergeStats(
        strategy=strategy,
        source_count=source_count,
        target_count_before=target_count_before,
        target_count_after=target_count_after,
        inserted=0,
        updated=0,
        deleted=0,
    )

    if strategy == MergeStrategy.INSERT:
        # INSERT: only additions, no updates or deletes
        stats.inserted = target_count_after - target_count_before
        stats.updated = 0
        stats.deleted = 0

    elif strategy == MergeStrategy.UPDATE:
        # UPDATE: no additions or deletes (all existing potentially updated)
        stats.inserted = 0
        stats.updated = target_count_before if target_count_before > 0 else 0
        stats.deleted = 0

    elif strategy == MergeStrategy.FULL_MERGE:
        # FULL_MERGE: source replaces target completely
        stats.inserted = source_count
        stats.updated = 0
        stats.deleted = target_count_before

    else:  # UPSERT or DEDUPLICATE
        # UPSERT/DEDUPLICATE: additions and updates
        net_change = target_count_after - target_count_before
        stats.inserted = max(0, net_change)
        stats.updated = source_count - stats.inserted
        stats.deleted = 0

    # Update total_processed
    stats.total_processed = stats.inserted + stats.updated

    return stats


def validate_strategy_compatibility(
    strategy: MergeStrategy,
    source_count: int,
    target_exists: bool,
) -> None:
    """
    Validate that the chosen strategy is compatible with the data state.

    Args:
        strategy: Merge strategy to validate.
        source_count: Number of rows in source data.
        target_exists: Whether target dataset exists.

    Raises:
        ValueError: If strategy is incompatible with the data state.
    """
    if strategy == MergeStrategy.UPDATE and source_count == 0:
        # UPDATE strategy with empty source doesn't make sense
        raise ValueError("UPDATE strategy requires non-empty source data")

    if strategy == MergeStrategy.FULL_MERGE and not target_exists:
        # FULL_MERGE on non-existent target is equivalent to just writing source
        # This is more of a warning situation, but we'll allow it
        pass

    # Other strategies are generally compatible with any state
    pass


def get_canonical_merge_strategies() -> list[str]:
    """
    Get the list of canonical merge strategy names.

    Returns:
        List of strategy names in canonical order.
    """
    return [strategy.value for strategy in MergeStrategy]


@dataclass
class MergePlanningResults:
    """Results from merge planning phase."""

    strategy: MergeStrategy
    key_columns: list[str]
    source_count: int
    target_exists: bool
    target_count_before: int
    source_table: pa.Table
    source_keys: list
    source_key_set: set
    partition_columns: list[str]
    target_files: list[str]
    validation_results: dict[str, Any]
    warnings: list[str]


def validate_merge_inputs_comprehensive(
    strategy: MergeStrategy,
    source_table: pa.Table,
    key_columns: list[str],
    target_files: list[str] | None = None,
    partition_columns: list[str] | None = None,
    filesystem: Any = None,
) -> tuple[dict[str, Any], list[str]]:
    """
    Comprehensive validation of merge inputs with detailed results.

    Args:
        strategy: Merge strategy to validate
        source_table: Source data table
        key_columns: Key columns for matching
        target_files: List of existing target files (None if target doesn't exist)
        partition_columns: Optional partition columns
        filesystem: Filesystem instance for file operations

    Returns:
        Tuple of (validation_results dict, warnings list)
    """
    validation_results = {
        "key_columns_valid": True,
        "partition_columns_valid": True,
        "null_keys_possible": False,
        "schema_compatible": True,
        "strategy_target_compatible": True,
    }
    warnings = []

    # Validate strategy against target existence
    target_exists = target_files is not None and len(target_files) > 0
    try:
        validate_strategy_compatibility(strategy, 0, target_exists)
    except ValueError as e:
        validation_results["strategy_target_compatible"] = False
        warnings.append(f"Strategy compatibility warning: {e}")

    # Validate key columns exist in source
    try:
        _validate_key_columns_in_table(source_table, key_columns, "source")
    except ValueError as e:
        validation_results["key_columns_valid"] = False
        warnings.append(f"Key column validation failed: {e}")

    # Validate partition columns if provided
    if partition_columns:
        try:
            _validate_key_columns_in_table(source_table, partition_columns, "source")
        except ValueError as e:
            validation_results["partition_columns_valid"] = False
            warnings.append(f"Partition column validation failed: {e}")

    # Check for potential NULL keys based on schema
    for key_col in key_columns:
        source_field = source_table.schema.field(key_col)
        if source_field.nullable:
            validation_results["null_keys_possible"] = True
            warnings.append(
                f"Key column '{key_col}' is nullable - NULL values will cause merge to fail"
            )
            break

    return validation_results, warnings


def _validate_key_columns_in_table(
    table: pa.Table,
    columns: list[str],
    context: str,
) -> None:
    """Validate that specified columns exist in table.

    Args:
        table: PyArrow table to validate against
        columns: Column names to validate
        context: Context for error messages

    Raises:
        ValueError: If any columns are missing
    """
    available_columns = set(table.column_names)
    missing = [col for col in columns if col not in available_columns]
    if missing:
        raise ValueError(
            f"Column(s) missing from {context}: {', '.join(missing)}. "
            f"Available columns: {', '.join(sorted(available_columns))}"
        )


def plan_source_processing(
    source_table: pa.Table,
    key_columns: list[str],
    target_files: list[str] | None,
    strategy: Literal["insert", "update", "upsert"],
    filesystem: Any = None,
) -> MergePlanningResults:
    """
    Plan the source data processing for merge operation.

    Args:
        source_table: Source data to process
        key_columns: Key columns for matching
        target_files: List of existing target files (None if target doesn't exist)
        strategy: Merge strategy
        filesystem: Filesystem instance for file operations

    Returns:
        MergePlanningResults with planning information
    """
    # Deduplicate source data (last-write-wins)
    source_table_deduped = _dedupe_source_last_wins_common(source_table, key_columns)

    # Extract source keys for planning
    source_keys, source_key_set = _extract_keys_from_table_common(
        source_table_deduped, key_columns
    )

    # Calculate target count if target exists
    target_exists = target_files is not None and len(target_files) > 0
    target_count_before = 0
    if target_exists and filesystem is not None:
        target_count_before = _calculate_target_count_from_files(
            target_files, filesystem
        )

    # Convert strategy string to MergeStrategy enum
    strategy_enum = MergeStrategy(strategy)

    # Perform validation
    validation_results, warnings = validate_merge_inputs_comprehensive(
        strategy=strategy_enum,
        source_table=source_table_deduped,
        key_columns=key_columns,
        target_files=target_files,
        filesystem=filesystem,
    )

    return MergePlanningResults(
        strategy=strategy_enum,
        key_columns=key_columns,
        source_count=source_table_deduped.num_rows,
        target_exists=target_exists,
        target_count_before=target_count_before,
        source_table=source_table_deduped,
        source_keys=source_keys,
        source_key_set=source_key_set,
        partition_columns=[],  # Will be set by caller
        target_files=target_files or [],
        validation_results=validation_results,
        warnings=warnings,
    )


def _dedupe_source_last_wins_common(
    table: pa.Table,
    key_columns: list[str],
) -> pa.Table:
    """Common implementation of source deduplication (last-write-wins).

    Args:
        table: Source table to deduplicate
        key_columns: Key columns for deduplication

    Returns:
        Deduplicated table
    """
    if table.num_rows == 0:
        return table

    if len(key_columns) == 1:
        keys = table.column(key_columns[0]).to_pylist()
        last_index = {}
        for idx, key in enumerate(keys):
            last_index[key] = idx
        indices = sorted(last_index.values())
    else:
        key_arrays = [table.column(c).to_pylist() for c in key_columns]
        last_index = {}
        for idx, key in enumerate(zip(*key_arrays)):
            last_index[key] = idx
        indices = sorted(last_index.values())

    import pyarrow as pa

    return table.take(pa.array(indices, type=pa.int64()))


def _extract_keys_from_table_common(
    table: pa.Table,
    key_columns: list[str],
) -> tuple[list, set]:
    """Extract keys from table as both list and set for different uses.

    Args:
        table: Source table
        key_columns: Key column names

    Returns:
        Tuple of (key_list, key_set)
    """
    if len(key_columns) == 1:
        key_list = table.column(key_columns[0]).to_pylist()
        key_set = set(key_list)
    else:
        arrays = [table.column(c).to_pylist() for c in key_columns]
        key_list = list(zip(*arrays))
        key_set = set(key_list)

    return key_list, key_set


def _calculate_target_count_from_files(
    target_files: list[str] | None,
    filesystem: Any,
) -> int:
    """Calculate total row count from list of target files.

    Args:
        target_files: List of target file paths (None if no target)
        filesystem: Filesystem instance

    Returns:
        Total row count across all files
    """
    if target_files is None:
        return 0

    total_rows = 0
    for file_path in target_files:
        try:
            import pyarrow.parquet as pq

            metadata = pq.read_metadata(file_path, filesystem=filesystem)
            total_rows += metadata.num_rows
        except Exception:
            # Conservative: if we can't read metadata, assume file is affected
            total_rows += 0  # This will be handled by affected files logic

    return total_rows


def select_rows_by_keys_common(
    table: pa.Table,
    key_columns: list[str],
    key_set: set,
) -> pa.Table:
    """Common implementation of selecting rows by keys.

    Args:
        table: Source table
        key_columns: Key column names
        key_set: Set of keys to match

    Returns:
        Filtered table containing only rows with matching keys
    """
    if not key_set:
        import pyarrow as pa

        return table.slice(0, 0)

    if len(key_columns) == 1:
        key_col = key_columns[0]
        value_set = set(key_set)
        table_keys = table.column(key_col).to_pylist()
        mask = [key in value_set for key in table_keys]
        import pyarrow as pa

        return table.filter(pa.array(mask, type=pa.bool_()))

    key_set_check = set(key_set)
    arrays = [table.column(c).to_pylist() for c in key_columns]
    table_keys = list(zip(*arrays))

    import pyarrow as pa

    mask = [key in key_set_check for key in table_keys]
    return table.filter(pa.array(mask, type=pa.bool_()))


def execute_merge_by_strategy(
    strategy: Literal["insert", "update", "upsert"],
    planning_results: MergePlanningResults,
    backend_executor: Callable[[MergePlanningResults], MergeResult],
    **kwargs,
) -> MergeResult:
    """
    Execute merge operation using template method pattern.

    Args:
        strategy: Merge strategy to execute
        planning_results: Results from planning phase
        backend_executor: Backend-specific execution function
        **kwargs: Additional arguments for backend executor

    Returns:
        MergeResult with operation results
    """
    # Template method: common validation and early exit logic
    if planning_results.source_table.num_rows == 0:
        return _create_empty_source_result(planning_results)

    if not planning_results.target_exists and strategy == "update":
        raise ValueError("UPDATE strategy requires an existing target dataset")

    # Delegate to backend-specific implementation
    return backend_executor(planning_results, **kwargs)


def _create_empty_source_result(
    planning_results: MergePlanningResults,
) -> MergeResult:
    """Create result for operations with empty source.

    Args:
        planning_results: Planning results

    Returns:
        MergeResult for empty source case
    """
    from fsspeckit.core.incremental import MergeFileMetadata, MergeResult

    files = [
        MergeFileMetadata(path=f, row_count=0, operation="preserved")
        for f in planning_results.target_files
    ]

    # Convert strategy to string value - ensure it's one of the allowed literals
    if hasattr(planning_results.strategy, "value"):
        strategy_value = planning_results.strategy.value
    else:
        strategy_value = str(planning_results.strategy)

    # Type assertion to satisfy type checker
    strategy_literal: Literal["insert", "update", "upsert"] = strategy_value  # type: ignore

    return MergeResult(
        strategy=strategy_literal,
        source_count=0,
        target_count_before=planning_results.target_count_before,
        target_count_after=planning_results.target_count_before,
        inserted=0,
        updated=0,
        deleted=0,
        files=files,
        rewritten_files=[],
        inserted_files=[],
        preserved_files=list(planning_results.target_files),
    )


def build_merge_statistics(
    strategy: Literal["insert", "update", "upsert"],
    source_count: int,
    matched_keys: set,
    inserted_keys: set,
    files_metadata: list[MergeFileMetadata],
) -> dict[str, Any]:
    """
    Build comprehensive merge statistics.

    Args:
        strategy: Merge strategy used
        source_count: Total source rows processed
        matched_keys: Set of keys that matched existing records
        inserted_keys: Set of keys that were inserted as new
        files_metadata: List of file operation metadata

    Returns:
        Dictionary with merge statistics
    """
    inserted_rows = len(inserted_keys)
    updated_rows = len(matched_keys) if strategy != "insert" else 0

    return {
        "strategy": strategy,
        "source_count": source_count,
        "inserted_rows": inserted_rows,
        "updated_rows": updated_rows,
        "total_processed": inserted_rows + updated_rows,
        "files_affected": len(files_metadata),
        "matched_keys_count": len(matched_keys),
        "inserted_keys_count": len(inserted_keys),
    }


def handle_file_io_error_conservative(
    file_path: str,
    error: Exception,
    default_behavior: str = "assume_affected",
) -> tuple[bool, str]:
    """
    Handle file I/O errors with conservative approach.

    Args:
        file_path: Path to file that caused the error
        error: Exception that occurred
        default_behavior: Default behavior when error occurs

    Returns:
        Tuple of (is_affected, message)
    """
    from fsspeckit.common.logging import get_logger

    logger = get_logger(__name__)

    logger.warning(
        "Failed to process file during merge",
        path=file_path,
        error=str(error),
        operation="merge",
        default_behavior=default_behavior,
    )

    if default_behavior == "assume_affected":
        # Conservative: assume file might contain matching keys
        return (
            True,
            f"Conservative handling: assumed file affected due to I/O error: {error}",
        )
    elif default_behavior == "assume_unaffected":
        # Optimistic: assume file doesn't contain matching keys
        return (
            False,
            f"Optimistic handling: assumed file unaffected due to I/O error: {error}",
        )
    else:
        # Fail fast
        raise error
