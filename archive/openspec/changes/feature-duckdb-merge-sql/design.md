# DuckDB MERGE Statement Implementation Design

## Overview

This design details the implementation of DuckDB's native MERGE statement as the default merge strategy for `fsspeckit`, with UNION ALL as a fallback for older DuckDB versions.

## Architecture

### Component Layout

```
DuckDBDatasetIO.merge()
    │
    ├─> _get_duckdb_version(conn) -> tuple[int, int, int]
    ├─> _supports_merge(conn) -> bool
    │
    ├─> Routing Decision
    │   ├─> use_merge=True
    │   │   └─> _merge_using_duckdb_merge(...)
    │   ├─> use_merge=False
    │   │   └─> _merge_using_union_all(...)
    │   └─> use_merge=None (auto-detect)
    │       ├─> version >= 1.4.0
    │       │   └─> _merge_using_duckdb_merge(...)
    │       └─> version < 1.4.0
    │           └─> _merge_using_union_all(...)
    │
    ├─> _merge_using_duckdb_merge(conn, source, target, strategy, ...)
    │   └─> Build MERGE SQL (INSERT/UPDATE/UPSERT)
    │
    └─> _merge_using_union_all(conn, source, target, strategy, ...)
        └─> Build UNION ALL + NOT EXISTS SQL (existing logic)
```

### Version Detection Strategy

```python
def _get_duckdb_version(conn) -> tuple[int, int, int]:
    """Get DuckDB version as (major, minor, patch).

    Example:
        SELECT * FROM pragma_version()
        Returns: {'library_version': '1.4.0', ...}

    Returns:
        Version tuple for easy comparison: (1, 4, 0)
    """
    result = conn.execute("SELECT * FROM pragma_version()").fetchone()
    version_str = result['library_version']
    return tuple(map(int, version_str.split('.')))

def _supports_merge(conn) -> bool:
    """Check if DuckDB version supports MERGE statement.

    MERGE was introduced in DuckDB 1.4.0.

    Returns:
        True if version >= 1.4.0, False otherwise
    """
    version = _get_duckdb_version(conn)
    return version >= (1, 4, 0)
```

## MERGE Implementation by Strategy

### INSERT Strategy

**Goal**: Only insert rows where keys don't exist in target.

**MERGE SQL**:
```sql
MERGE INTO parquet_scan('{existing_file}') AS target
    USING {source_view} AS source
    ON ({join_conditions})
    WHEN NOT MATCHED THEN INSERT BY NAME;
```

**Example with single key**:
```sql
MERGE INTO parquet_scan('/path/file.parquet') AS target
    USING source_view AS source
    ON (target.id = source.id)
    WHEN NOT MATCHED THEN INSERT BY NAME;
```

**Example with composite key**:
```sql
MERGE INTO parquet_scan('/path/file.parquet') AS target
    USING source_view AS source
    ON (target.id = source.id AND target.category = source.category)
    WHEN NOT MATCHED THEN INSERT BY NAME;
```

**RETURNING clause for audit**:
```sql
MERGE INTO parquet_scan('/path/file.parquet') AS target
    USING source_view AS source
    ON (target.id = source.id)
    WHEN NOT MATCHED THEN INSERT BY NAME
    RETURNING merge_action, *;
-- Returns: 'INSERT' or 'UPDATE' for each row
```

**Python Implementation**:
```python
def _merge_insert_using_duckdb_merge(
    self,
    existing_file: str,
    source_view: str,
    key_columns: list[str],
    staging_file: str,
    compression: str | None = None,
    row_group_size: int | None = None,
) -> tuple[int, int]:
    """INSERT strategy using MERGE.

    Returns:
        Tuple of (updated_count, inserted_count)
    """
    conn = self._connection.connection

    # Build join condition
    key_conditions = " AND ".join([
        f"target.\"{col}\" = source.\"{col}\""
        for col in key_columns
    ])

    # Build MERGE query
    merge_sql = f"""
    MERGE INTO parquet_scan('{existing_file}') AS target
        USING {source_view} AS source
        ON ({key_conditions})
        WHEN NOT MATCHED THEN INSERT BY NAME
    """

    # Execute MERGE and collect to staging
    copy_sql = f"COPY ({merge_sql}) TO '{staging_file}' (FORMAT PARQUET"
    if compression:
        copy_sql += f", COMPRESSION {compression}"
    if row_group_size:
        copy_sql += f", ROW_GROUP_SIZE {row_group_size}"
    copy_sql += ")"

    conn.execute(copy_sql)

    # Get counts
    output_count = conn.execute(
        f"SELECT COUNT(*) FROM parquet_scan('{staging_file}')"
    ).fetchone()[0]

    return (0, output_count)
```

### UPDATE Strategy

**Goal**: Only update rows where keys exist in target, never insert new rows.

**MERGE SQL**:
```sql
MERGE INTO parquet_scan('{existing_file}') AS target
    USING {source_view} AS source
    ON ({join_conditions})
    WHEN MATCHED THEN UPDATE SET *;
```

**Python Implementation**:
```python
def _merge_update_using_duckdb_merge(
    self,
    existing_file: str,
    source_view: str,
    key_columns: list[str],
    staging_file: str,
    compression: str | None = None,
    row_group_size: int | None = None,
) -> tuple[int, int]:
    """UPDATE strategy using MERGE.

    Returns:
        Tuple of (updated_count, inserted_count)
    """
    conn = self._connection.connection

    # Build join condition
    key_conditions = " AND ".join([
        f"target.\"{col}\" = source.\"{col}\""
        for col in key_columns
    ])

    # Build MERGE query
    merge_sql = f"""
    MERGE INTO parquet_scan('{existing_file}') AS target
        USING {source_view} AS source
        ON ({key_conditions})
        WHEN MATCHED THEN UPDATE SET *
    """

    # Execute MERGE and collect to staging
    copy_sql = f"COPY ({merge_sql}) TO '{staging_file}' (FORMAT PARQUET"
    if compression:
        copy_sql += f", COMPRESSION {compression}"
    if row_group_size:
        copy_sql += f", ROW_GROUP_SIZE {row_group_size}"
    copy_sql += ")"

    conn.execute(copy_sql)

    # Get counts - UPDATE only, so all rows are updates
    output_count = conn.execute(
        f"SELECT COUNT(*) FROM parquet_scan('{staging_file}')"
    ).fetchone()[0]

    return (output_count, 0)
```

### UPSERT Strategy

**Goal**: Update existing keys and insert new keys in a single operation.

**MERGE SQL**:
```sql
MERGE INTO parquet_scan('{existing_file}') AS target
    USING {source_view} AS source
    ON ({join_conditions})
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT BY NAME;
```

**Python Implementation with RETURNING**:
```python
def _merge_upsert_using_duckdb_merge(
    self,
    existing_file: str,
    source_view: str,
    key_columns: list[str],
    staging_file: str,
    compression: str | None = None,
    row_group_size: int | None = None,
) -> tuple[int, int]:
    """UPSERT strategy using MERGE.

    Returns:
        Tuple of (updated_count, inserted_count)
    """
    conn = self._connection.connection

    # Build join condition
    key_conditions = " AND ".join([
        f"target.\"{col}\" = source.\"{col}\""
        for col in key_columns
    ])

    # Build MERGE query with RETURNING for audit
    merge_sql = f"""
    MERGE INTO parquet_scan('{existing_file}') AS target
        USING {source_view} AS source
        ON ({key_conditions})
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT BY NAME
    """

    # Execute MERGE to staging table (for counting)
    temp_table = f"temp_merge_{uuid.uuid4().hex[:16]}"
    conn.execute(f"""
        CREATE TEMP TABLE {temp_table} AS
        SELECT merge_action FROM (
            {merge_sql}
            RETURNING merge_action
        )
    """)

    # Count actions
    update_count = conn.execute(
        f"SELECT COUNT(*) FROM {temp_table} WHERE merge_action = 'UPDATE'"
    ).fetchone()[0]

    insert_count = conn.execute(
        f"SELECT COUNT(*) FROM {temp_table} WHERE merge_action = 'INSERT'"
    ).fetchone()[0]

    # Execute actual MERGE to parquet file
    copy_sql = f"COPY ({merge_sql}) TO '{staging_file}' (FORMAT PARQUET"
    if compression:
        copy_sql += f", COMPRESSION {compression}"
    if row_group_size:
        copy_sql += f", ROW_GROUP_SIZE {row_group_size}"
    copy_sql += ")"

    conn.execute(copy_sql)

    # Cleanup temp table
    _unregister_duckdb_table_safely(conn, temp_table)

    return (update_count, insert_count)
```

## Version Gating Strategy

### Decision Tree

```
start: merge() called with use_merge parameter
  │
  ├─> use_merge=True
  │   └─> Call _supports_merge()
  │       ├─> True: _merge_using_duckdb_merge()
  │       └─> False: raise ValueError("DuckDB MERGE not available")
  │
  ├─> use_merge=False
  │   └─> _merge_using_union_all() (always)
  │
  └─> use_merge=None (auto-detect)
      └─> Call _supports_merge()
          ├─> True: _merge_using_duckdb_merge()
          │   └─> Log: "Using DuckDB MERGE for merge operation"
          └─> False: _merge_using_union_all()
              └─> Log: "Using UNION ALL fallback (DuckDB < 1.4.0)"
```

### Implementation in merge() Method

```python
def merge(
    self,
    data: pa.Table | list[pa.Table],
    path: str,
    strategy: Literal["insert", "update", "upsert"],
    key_columns: list[str] | str,
    *,
    partition_columns: list[str] | str | None = None,
    schema: pa.Schema | None = None,
    compression: str | None = "snappy",
    max_rows_per_file: int | None = 5_000_000,
    row_group_size: int | None = 500_000,
    use_merge: bool | None = None,  # NEW
) -> "MergeResult":
    """Merge data into an existing parquet dataset incrementally (DuckDB backend).

    Args:
        use_merge: Explicit control over MERGE statement usage.
            - True: Force MERGE (raises error if unavailable)
            - False: Force UNION ALL fallback
            - None: Auto-detect based on DuckDB version (default)
    """

    # ... existing setup code ...

    # Determine merge strategy (NEW ROUTING)
    merge_impl = self._select_merge_implementation(use_merge)

    # Log strategy selection
    if use_merge is None:
        if merge_impl == self._merge_using_duckdb_merge:
            logger.info("Using DuckDB MERGE for merge operation (auto-detected)")
        else:
            logger.info(
                "Using UNION ALL fallback for merge operation "
                "(DuckDB < 1.4.0 or MERGE unavailable)"
            )
    elif use_merge:
        logger.info("Using DuckDB MERGE (explicitly requested)")
    else:
        logger.info("Using UNION ALL (explicitly requested)")

    # ... rest of merge logic using merge_impl ...
```

### Version Check Implementation

```python
def _select_merge_implementation(
    self,
    use_merge: bool | None,
) -> Callable:
    """Select appropriate merge implementation based on version and user preference.

    Args:
        use_merge: User preference or None for auto-detect

    Returns:
        Merge function to use
    """
    conn = self._connection.connection

    if use_merge is False:
        # Explicit fallback
        return self._merge_using_union_all

    if use_merge is True or use_merge is None:
        # Explicit request or auto-detect - check version
        supports_merge = self._supports_merge(conn)

        if use_merge is True and not supports_merge:
            raise DatasetMergeError(
                "DuckDB MERGE requested but not available. "
                f"Required version: >=1.4.0, Current: {self._get_duckdb_version(conn)}"
            )

        return (
            self._merge_using_duckdb_merge
            if supports_merge
            else self._merge_using_union_all
        )

    # Shouldn't reach here
    raise ValueError(f"Invalid use_merge value: {use_merge}")
```

## Fallback Implementation

### Rename Current Method

```python
# Existing method becomes fallback
def _merge_using_union_all(
    self,
    existing_file: str,
    source_table: pa.Table,
    strategy: CoreMergeStrategy,
    key_columns: list[str],
    staging_file: str,
    compression: str | None = None,
    row_group_size: int | None = None,
) -> tuple[int, int]:
    """Merge using UNION ALL + NOT EXISTS (fallback for DuckDB < 1.4.0).

    This is the original implementation from refactor-duckdb-module-simplification.
    """
    # ... existing UNION ALL + NOT EXISTS logic ...
```

### Backward Compatibility

```python
# Both implementations have same signature
def _merge_file_with_sql(
    self,
    existing_file: str,
    source_table: pa.Table,
    strategy: CoreMergeStrategy,
    key_columns: list[str],
    staging_file: str,
    compression: str | None = None,
    row_group_size: int | None = None,
) -> tuple[int, int]:
    """Main entry point for file-level SQL merge.

    Routes to MERGE or UNION ALL based on version detection.
    """
    merge_impl = self._select_merge_implementation(self._use_merge_preference)
    return merge_impl(
        existing_file=existing_file,
        source_table=source_table,
        strategy=strategy,
        key_columns=key_columns,
        staging_file=staging_file,
        compression=compression,
        row_group_size=row_group_size,
    )
```

## Integration with IncrementalFileManager

### Atomic Replacement Preserved

Both MERGE and UNION ALL implementations:
1. Generate merged data to staging file
2. Call `IncrementalFileManager.atomic_replace_files()`
3. Clean up staging via `IncrementalFileManager.cleanup_staging_files()`

### No Changes Needed

The IncrementalFileManager operates on file-level atomicity, independent of how merge data is generated. MERGE provides **row-level** atomicity within the merge operation, which is an additional guarantee.

## Performance Considerations

### Expected Improvements

| Operation | UNION ALL + NOT EXISTS | MERGE (DuckDB 1.4.0) |
|-----------|-------------------------|----------------------------|
| **Execution passes** | 2+ (match + insert) | 1 (optimized join) |
| **Query planning** | Multiple separate queries | Single optimized plan |
| **I/O patterns** | Read target twice | Read target once |
| **Memory usage** | Similar (both streaming) | Similar (both streaming) |
| **CPU utilization** | Higher (duplicate work) | Lower (single pass) |

### Benchmarking Requirements

1. **Small dataset test**: 10K rows, 5 files
2. **Medium dataset test**: 1M rows, 50 files
3. **Large dataset test**: 10M rows, 500 files
4. **Strategy comparison**: Test all INSERT, UPDATE, UPSERT
5. **Both implementations**: Run with MERGE and UNION ALL fallback

### Metrics to Track

```python
benchmarks = {
    "execution_time_seconds": ...,
    "peak_memory_mb": ...,
    "rows_processed": ...,
    "duckdb_execution_time": ...,  # From EXPLAIN ANALYZE
}
```

## Error Handling

### MERGE-Specific Errors

```python
try:
    conn.execute(merge_sql)
except (
    _DUCKDB_EXCEPTIONS.get("ParserException"),
    _DUCKDB_EXCEPTIONS.get("InvalidInputException"),
) as e:
    logger.error(
        "MERGE statement execution failed",
        error=safe_format_error(e),
        operation="merge_duckdb_merge",
        merge_strategy=strategy,
    )
    raise DatasetMergeError(
        f"MERGE operation failed: {safe_format_error(e)}"
    ) from e
```

### Version Detection Errors

```python
def _get_duckdb_version(conn) -> tuple[int, int, int]:
    try:
        result = conn.execute("SELECT * FROM pragma_version()").fetchone()
        version_str = result['library_version']
        return tuple(map(int, version_str.split('.')))
    except (KeyError, ValueError, IndexError) as e:
        logger.warning(
            "Could not determine DuckDB version",
            error=str(e),
            operation="version_detection",
        )
        return (0, 0, 0)  # Assume old version
```

## Testing Strategy

### Unit Tests

```python
class TestDuckDBVersionDetection:
    def test_parse_version_string(self):
        assert (1, 4, 0) == _get_duckdb_version(mock_conn_v1_4_0)
        assert (1, 3, 2) == _get_duckdb_version(mock_conn_v1_3_2)

    def test_supports_merge(self):
        assert True == _supports_merge(mock_conn_v1_4_0)
        assert False == _supports_merge(mock_conn_v1_3_2)
        assert False == _supports_merge(mock_conn_v0_9_2)

class TestMergeImplementationSelection:
    def test_use_merge_true(self):
        assert impl == _merge_using_duckdb_merge
        # Should use MERGE even if not supported (will raise error)

    def test_use_merge_false(self):
        assert impl == _merge_using_union_all

    def test_use_merge_none_supported(self):
        assert impl == _merge_using_duckdb_merge

    def test_use_merge_none_unsupported(self):
        assert impl == _merge_using_union_all
```

### Integration Tests

```python
class TestMergeWithDuckDBMerge:
    """Tests specifically for MERGE implementation."""

    def test_upsert_with_merge(self, temp_dir, duckdb_io):
        # Setup: Create dataset and source data
        # Execute: Use MERGE (auto-detect or explicit)
        # Verify: Correct rows inserted/updated

    def test_update_with_merge(self, temp_dir, duckdb_io):
        # Verify UPDATE-only behavior with MERGE

    def test_insert_with_merge(self, temp_dir, duckdb_io):
        # Verify INSERT-only behavior with MERGE

    def test_returning_clause(self, temp_dir, duckdb_io):
        # Verify audit trail from RETURNING clause

    def test_multi_column_keys_with_merge(self, temp_dir, duckdb_io):
        # Verify composite key handling

    def test_fallback_to_union_all(self, temp_dir, duckdb_io_old_version):
        # Verify UNION ALL path for DuckDB < 1.4.0
```

### Backward Compatibility Tests

All existing tests from `refactor-duckdb-module-simplification` must pass:
- TestMergeInsert
- TestMergeUpdate
- TestMergeUpsert
- TestMergeIncrementalRewrite
- TestMergeFileMetadata
- TestMergeEdgeCases

These tests should run with both MERGE and UNION ALL implementations.

## Rollback Plan

If critical issues are discovered with MERGE implementation:

1. **Feature flag**: Add `FSSPECKIT_DISABLE_MERGE` environment variable
2. **Configuration**: Document `use_merge=False` as workaround
3. **Release**: Document downgrade path to previous version

Current implementation (UNION ALL) remains as fallback, so no data rollback needed.
