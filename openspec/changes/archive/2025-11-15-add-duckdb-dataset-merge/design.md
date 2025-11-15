# Design Document: DuckDB Dataset Merge Operations

## Context

Users need to merge new data into existing parquet datasets intelligently. Current options (append, overwrite) are insufficient for real-world scenarios where:
- Records need to be updated based on a primary key
- Only new records should be added
- Existing records should be updated
- Records missing from source should be deleted (for full synchronization)
- Duplicates need to be removed before merging

This design enables efficient, database-style merge operations on parquet datasets using DuckDB's SQL capabilities.

## Goals / Non-Goals

### Goals
- Enable UPSERT operations (insert + update) based on key columns
- Support INSERT-only mode (add new, skip existing)
- Support UPDATE-only mode (update existing, skip new)
- Support FULL_MERGE mode (insert, update, delete)
- Support DEDUPLICATE mode (remove duplicates before merge)
- Perform merge efficiently using DuckDB SQL (not in-memory)
- Work with both PyArrow tables and parquet dataset paths
- Preserve dataset integrity during merge operations
- Provide clear error messages for invalid operations

### Non-Goals
- Implement complex conflict resolution strategies
- Support partial column updates (always update full record)
- Implement time-travel or versioning
- Support schema evolution (must match target schema)
- Replace dedicated CDC tools (Debezium, etc.)
- Implement distributed merge (single-node only)

## Decisions

### Design Decision 1: SQL-Based Merge Strategy

**Decision**: Implement merge using DuckDB SQL rather than application-level logic.

**Rationale**:
- DuckDB optimized for analytical operations
- Efficient join and aggregation operations
- Minimizes data movement between processes
- Leverages query optimization
- Consistent with existing handler approach

**Implementation Example**:
```sql
-- UPSERT strategy
CREATE TEMP TABLE merged AS
SELECT * FROM target_data
WHERE key_col NOT IN (SELECT key_col FROM source_data)
UNION ALL
SELECT * FROM source_data;
```

**Alternatives considered**:
- PyArrow compute: More complex, less efficient for large datasets
- Pandas merge: Memory-intensive, slower
- Manual iteration: Very slow, doesn't scale

### Design Decision 2: Five Distinct Merge Strategies

**Decision**: Implement five clear merge strategies as enumeration.

```python
from enum import Enum

class MergeStrategy(Enum):
    UPSERT = "upsert"           # Insert new + update existing
    INSERT = "insert"           # Insert new only
    UPDATE = "update"           # Update existing only
    FULL_MERGE = "full_merge"   # Insert + update + delete
    DEDUPLICATE = "deduplicate" # Remove duplicates + upsert
```

**Rationale**:
- Clear semantics for each operation
- Prevents ambiguity in behavior
- Easy to understand and document
- Covers common use cases
- Enum provides type safety

**Alternatives considered**:
- Boolean flags (insert_new, update_existing): Confusing combinations
- Single "merge" with options: Less clear what happens
- More granular strategies: Adds complexity without clear benefit

### Design Decision 3: Key Columns for Record Identification

**Decision**: Require explicit key_columns parameter for merge operations.

```python
def merge_parquet_dataset(
    self,
    source: pa.Table | str,
    target_path: str,
    key_columns: list[str] | str,
    strategy: MergeStrategy = MergeStrategy.UPSERT,
    ...
)
```

**Rationale**:
- Explicit is better than implicit (Python zen)
- No ambiguity about how records are matched
- Supports composite keys (multiple columns)
- Prevents accidental full dataset replacement
- Required for deterministic merge behavior

**Alternatives considered**:
- Auto-detect key (index columns): Unreliable, may not exist
- Use all columns: Not practical, very slow
- Optional key (default to all columns): Confusing behavior

### Design Decision 4: Source Flexibility

**Decision**: Accept both PyArrow table and parquet path as source.

```python
# From table
handler.merge_parquet_dataset(
    source=new_data_table,
    target_path="/data/customers/",
    key_columns=["customer_id"]
)

# From path
handler.merge_parquet_dataset(
    source="/staging/new_customers/",
    target_path="/data/customers/",
    key_columns=["customer_id"]
)
```

**Rationale**:
- Flexibility for different workflows
- Avoid unnecessary data loading for path-to-path merge
- Consistent with existing read/write API
- Simplifies common scenarios

### Design Decision 5: Merge Implementation Approach

**Decision**: Read target dataset, perform SQL merge, overwrite target.

**Process**:
1. Load source data (table or read from path)
2. Load target dataset into DuckDB
3. Execute merge SQL based on strategy
4. Write merged result back to target using write_parquet_dataset
5. Use temporary tables for intermediate results

**Rationale**:
- Leverages existing write_parquet_dataset functionality
- Atomic operation (write uses overwrite mode)
- DuckDB handles memory management
- Clean abstraction boundaries

**Alternatives considered**:
- In-place file updates: Complex, error-prone, not transactional
- Copy-on-write with staging: More complex, not needed for this use case
- Manual file-by-file merge: Complicated, loses DuckDB optimization

### Design Decision 6: Deduplication Strategy with QUALIFY

**Decision**: For DEDUPLICATE strategy, use DuckDB's QUALIFY clause for efficient deduplication.

```sql
-- Use QUALIFY for efficient deduplication (DuckDB optimization)
CREATE TEMP TABLE deduplicated AS
SELECT * FROM source_data
QUALIFY ROW_NUMBER() OVER (PARTITION BY key_col1, key_col2 ORDER BY timestamp_col DESC) = 1;
```

**Rationale**:
- **QUALIFY is DuckDB's optimized window function filter**: More efficient than subquery approach
- **Cleaner SQL**: No need for subquery with WHERE clause
- **Better performance**: DuckDB can optimize QUALIFY better than nested SELECT
- **No temporary columns**: Eliminates row_number column in result
- **Flexible ordering**: User controls which duplicate to keep via dedup_order_by
- Common pattern in ETL pipelines

**Why QUALIFY over DISTINCT ON**:
- QUALIFY with ROW_NUMBER gives explicit control over tie-breaking
- Works with composite keys naturally
- Supports custom sort order per use case
- More portable to other window function patterns

**Alternatives considered**:
- Subquery with ROW_NUMBER() WHERE rn = 1: Less efficient, more verbose
- DISTINCT ON: PostgreSQL-specific, less flexible ordering
- GROUP BY with MAX/MIN: Requires aggregate logic, more complex
- Keep first duplicate: "Last wins" is more common in CDC scenarios

### Design Decision 7: Leverage QUALIFY for Window Function Filtering

**Decision**: Use DuckDB's `QUALIFY` clause throughout merge operations wherever window functions need filtering.

**QUALIFY Use Cases in Merge**:

1. **Deduplication** (primary use):
```sql
SELECT * FROM source_data
QUALIFY ROW_NUMBER() OVER (PARTITION BY key1, key2 ORDER BY timestamp DESC) = 1;
```

2. **Latest record selection** (when source has multiple updates):
```sql
SELECT * FROM updates
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY update_time DESC) = 1;
```

3. **Filtering top N per group**:
```sql
-- Keep only most recent 5 updates per customer
SELECT * FROM source_data
QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY date DESC) <= 5;
```

**Rationale**:
- **Performance**: DuckDB optimizes QUALIFY as a late-stage filter applied after window functions
- **Readability**: Clearer intent than nested subqueries
- **Efficiency**: Avoids creating temporary columns (like row_number) in intermediate results
- **DuckDB native**: Takes advantage of DuckDB-specific optimizations
- **Composability**: Can combine with other window functions easily

**Why QUALIFY is Better**:

Traditional approach (subquery):
```sql
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY ts DESC) as rn
    FROM data
) WHERE rn = 1;
```

With QUALIFY:
```sql
SELECT * FROM data
QUALIFY ROW_NUMBER() OVER (PARTITION BY key ORDER BY ts DESC) = 1;
```

Benefits:
- 3 lines → 2 lines (33% reduction)
- No `rn` column in result set
- Better query plan (DuckDB can optimize better)
- Clearer semantic meaning
- Works with multiple window functions

**Implementation Strategy**:
- Use QUALIFY for all deduplication operations
- Consider QUALIFY for identifying records to update/insert
- Document QUALIFY usage in examples
- Leverage in UPDATE strategy to find matching records efficiently

## Implementation Details

### Merge Strategy SQL Patterns

**Note**: All strategies leverage DuckDB-specific optimizations like `QUALIFY` for better performance.

#### 1. UPSERT Strategy

```sql
-- Remove target records that exist in source (will be replaced)
CREATE TEMP TABLE target_without_source AS
SELECT t.* FROM target_dataset t
LEFT JOIN source_data s ON t.key1 = s.key1 AND t.key2 = s.key2
WHERE s.key1 IS NULL;

-- Combine non-matching target records with all source records
CREATE TEMP TABLE merged AS
SELECT * FROM target_without_source
UNION ALL
SELECT * FROM source_data;

-- Alternative: Use QUALIFY to handle source duplicates if needed
-- SELECT * FROM source_data
-- QUALIFY ROW_NUMBER() OVER (PARTITION BY key1, key2 ORDER BY timestamp DESC) = 1;
```

#### 2. INSERT Strategy

```sql
-- Get only new records from source (not in target)
CREATE TEMP TABLE new_records AS
SELECT s.* FROM source_data s
LEFT JOIN target_dataset t ON s.key1 = t.key1
WHERE t.key1 IS NULL;

-- Combine target with new records
CREATE TEMP TABLE merged AS
SELECT * FROM target_dataset
UNION ALL
SELECT * FROM new_records;

-- Optional: If source may have duplicates, deduplicate first with QUALIFY
-- SELECT * FROM source_data
-- QUALIFY ROW_NUMBER() OVER (PARTITION BY key1 ORDER BY timestamp DESC) = 1
-- Then proceed with LEFT JOIN
```

#### 3. UPDATE Strategy

```sql
-- Get only existing records from source (in target)
-- Use QUALIFY to ensure we take latest if source has duplicates
CREATE TEMP TABLE existing_records AS
SELECT s.* FROM source_data s
INNER JOIN target_dataset t ON s.key1 = t.key1;

-- Remove matched target records
CREATE TEMP TABLE unmatched_target AS
SELECT t.* FROM target_dataset t
LEFT JOIN source_data s ON t.key1 = s.key1
WHERE s.key1 IS NULL;

-- Combine unmatched target with updated records
CREATE TEMP TABLE merged AS
SELECT * FROM unmatched_target
UNION ALL
SELECT * FROM existing_records;

-- Alternative: Deduplicate source first if needed
-- WITH deduped_source AS (
--     SELECT * FROM source_data
--     QUALIFY ROW_NUMBER() OVER (PARTITION BY key1 ORDER BY timestamp DESC) = 1
-- )
-- SELECT s.* FROM deduped_source s INNER JOIN target_dataset t ON s.key1 = t.key1;
```

#### 4. FULL_MERGE Strategy

```sql
-- Simply replace with source (source is ground truth)
CREATE TEMP TABLE merged AS
SELECT * FROM source_data;

-- This effectively deletes records not in source
```

#### 5. DEDUPLICATE Strategy (Using QUALIFY)

```sql
-- Deduplicate source using QUALIFY (more efficient than subquery)
CREATE TEMP TABLE deduplicated_source AS
SELECT * FROM source_data
QUALIFY ROW_NUMBER() OVER (PARTITION BY key1, key2 ORDER BY timestamp DESC) = 1;

-- Then perform UPSERT
-- (Use UPSERT SQL from above with deduplicated_source)

-- Note: QUALIFY is DuckDB's optimized way to filter window function results
-- More efficient than subquery with WHERE rn = 1
-- Eliminates need for temporary row_number column
```

### Method Signature

```python
def merge_parquet_dataset(
    self,
    source: pa.Table | str,
    target_path: str,
    key_columns: list[str] | str,
    strategy: Literal["upsert", "insert", "update", "full_merge", "deduplicate"] = "upsert",
    dedup_order_by: list[str] | None = None,
    compression: str = "snappy",
) -> dict[str, int]:
    """
    Merge source data into target parquet dataset.
    
    Returns:
        Dictionary with merge statistics:
        - inserted: Number of records inserted
        - updated: Number of records updated
        - deleted: Number of records deleted
        - total: Total records in merged dataset
    """
```

### Key Column Handling

```python
# Normalize key_columns to list
if isinstance(key_columns, str):
    key_columns = [key_columns]

# Validate key columns exist in both source and target
source_cols = set(source_table.column_names)
target_cols = set(target_table.column_names)

for key_col in key_columns:
    if key_col not in source_cols:
        raise ValueError(f"Key column '{key_col}' not found in source")
    if key_col not in target_cols:
        raise ValueError(f"Key column '{key_col}' not found in target")
```

### Merge Statistics Calculation

```python
def _calculate_merge_stats(
    target_before: pa.Table,
    target_after: pa.Table,
    source: pa.Table,
    key_columns: list[str]
) -> dict[str, int]:
    """Calculate merge statistics."""
    
    # Convert to sets of key tuples for comparison
    before_keys = set(extract_keys(target_before, key_columns))
    after_keys = set(extract_keys(target_after, key_columns))
    source_keys = set(extract_keys(source, key_columns))
    
    inserted = len(source_keys - before_keys)
    deleted = len(before_keys - after_keys)
    updated = len(source_keys & before_keys)
    
    return {
        "inserted": inserted,
        "updated": updated,
        "deleted": deleted,
        "total": len(after_keys)
    }
```

## Risks / Trade-offs

### Risk 1: Memory Usage for Large Datasets

**Risk**: Loading entire target dataset into DuckDB memory for merge.

**Mitigation**:
- DuckDB designed for larger-than-memory operations
- Document memory requirements
- Recommend splitting very large datasets
- Future: Implement streaming merge for huge datasets

### Risk 2: Merge Performance

**Risk**: Merge operation may be slow for large datasets.

**Trade-off**: Correctness vs speed. In-database merge is more correct than partial merges.

**Mitigation**:
- DuckDB's optimized joins are fast
- Document performance characteristics
- Recommend appropriate key columns (indexed in DuckDB)
- Consider batch merging for very large sources

### Risk 3: Concurrent Merge Operations

**Risk**: Multiple processes merging to same dataset simultaneously.

**Mitigation**:
- Document that merge is not concurrency-safe
- Users should implement external locking if needed
- Overwrite is atomic at file level
- Future: Add merge_strategy that appends with dedup

### Risk 4: Key Column Duplicates

**Risk**: Duplicate keys in target dataset lead to undefined behavior.

**Mitigation**:
- DEDUPLICATE strategy removes duplicates
- Validate uniqueness of keys in strict mode
- Document expected behavior
- Provide utility to check for duplicates

### Risk 5: Schema Mismatch

**Risk**: Source and target have different schemas.

**Mitigation**:
- Validate schemas match before merge
- Provide clear error messages
- Document schema compatibility requirements
- Future: Support column subset updates

## Migration Plan

This is additive functionality with no migration required.

**Rollout**:
1. Implement `merge_parquet_dataset` method
2. Add comprehensive tests for each strategy
3. Create example scripts demonstrating use cases
4. Document merge semantics and performance

**Backward Compatibility**:
- No changes to existing methods
- New method is optional
- Existing write patterns continue working

## Open Questions

1. **Q**: Should we support schema evolution (adding/removing columns)?
   **A**: No in initial version. Require exact schema match. Add as enhancement if needed.

2. **Q**: Should we support custom deduplication logic?
   **A**: Allow dedup_order_by parameter to control which duplicate to keep. Sufficient for most cases.

3. **Q**: Should we return merge statistics?
   **A**: Yes, return dict with inserted/updated/deleted counts. Helpful for monitoring.

4. **Q**: Should we support transaction rollback on error?
   **A**: No explicit transaction support. DuckDB write is atomic. Document that partial merge doesn't occur.

5. **Q**: Should we support merge to subset of partitions?
   **A**: No in initial version. User can filter target before merge. Add as enhancement if needed.

6. **Q**: How to handle NULL values in key columns?
   **A**: NULLs in key columns cause error (keys must be non-null). Validate before merge.
