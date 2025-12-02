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
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    import pyarrow as pa


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


@dataclass
class MergePlan:
    """Plan for executing a merge operation."""

    strategy: MergeStrategy
    key_columns: list[str]
    source_count: int
    target_exists: bool
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
