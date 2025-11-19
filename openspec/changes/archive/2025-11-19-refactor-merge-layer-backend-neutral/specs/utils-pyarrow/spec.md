# Merge Dataset Operations

## ADDED Requirements

### Requirement: Shared Merge Core Integration

merge_parquet_dataset_pyarrow SHALL use shared validation and statistics from fsspeckit.core.merge.

#### Scenario: Backend-neutral validation usage
- WHEN calling merge_parquet_dataset_pyarrow
- THEN function calls normalize_keys() for key column normalization
- AND function calls validate_keys_and_schema() for shared validation
- AND function calls compute_merge_stats() for canonical statistics

### Requirement: Canonical Key Validation

merge_parquet_dataset_pyarrow SHALL perform canonical key validation using backend-neutral helpers.

#### Scenario: Key normalization
- WHEN user provides key_columns as string or list
- THEN function calls normalize_keys() to convert to list format
- AND returns normalized list for internal use

#### Scenario: Key existence validation
- WHEN source table and target dataset schemas are available
- THEN function validates all key columns exist in both schemas
- AND raises ValueError with clear message for missing keys

#### Scenario: NULL key detection with filtered scanners
- WHEN processing merge data
- THEN function uses filtered scanners to detect NULL values in key columns
- AND raises ValueError before processing if NULL keys found
- AND uses scanner filtering to avoid full dataset materialization for NULL checking

### Requirement: Canonical Strategy Semantics

All merge strategies SHALL follow canonical semantics defined in backend-neutral core.

#### Scenario: UPSERT strategy behavior
- WHEN strategy="upsert" and key exists in target
- THEN existing target row is updated with source row data
- AND statistics.count updated incremented

#### Scenario: UPSERT strategy new keys
- WHEN strategy="upsert" and key does not exist in target
- THEN source row is inserted as new row
- AND statistics.count inserted incremented

#### Scenario: INSERT strategy behavior
- WHEN strategy="insert" and key exists in target
- THEN source row is ignored (no update)
- AND statistics.count updated unchanged

#### Scenario: INSERT strategy new keys
- WHEN strategy="insert" and key does not exist in target
- THEN source row is inserted as new row
- AND statistics.count inserted incremented

#### Scenario: UPDATE strategy behavior
- WHEN strategy="update" and key exists in target
- THEN existing target row is updated with source row data
- AND statistics.count updated incremented

#### Scenario: UPDATE strategy new keys
- WHEN strategy="update" and key does not exist in target
- THEN source row is ignored (no insert)
- AND statistics.count updated unchanged

#### Scenario: FULL_MERGE strategy behavior
- WHEN strategy="full_merge"
- THEN perform insert for new keys, update for existing keys, and delete for target-only keys
- AND all three statistics (inserted, updated, deleted) can be non-zero

#### Scenario: FULL_MERGE empty source
- WHEN strategy="full_merge" and source is empty
- THEN all target rows are deleted
- AND statistics.count deleted equals original target row count
- AND statistics.count inserted and updated are zero

#### Scenario: DEDUPLICATE strategy behavior
- WHEN strategy="deduplicate"
- THEN deduplicate source data first (keep highest values from dedup_order_by)
- AND then apply UPSERT semantics to deduplicated source
- AND statistics reflect operations on deduplicated data

### Requirement: Streaming Scanner Requirements

merge_parquet_dataset_pyarrow SHALL use filtered scanners as primary mechanism.

#### Scenario: Key-based filtered scanning
- WHEN matching source keys to target dataset
- THEN build filter expressions around key column values
- AND use scanner.filter() to limit target data to relevant rows
- AND process only target rows that could match source keys

#### Scenario: Batch streaming processing
- WHEN processing target dataset
- THEN use scanner.to_batches() with batch_rows parameter
- AND process each batch independently in memory
- AND build output incrementally without full dataset materialization

#### Scenario: Avoid full dataset materialization
- WHEN processing merge operations
- THEN avoid calling dataset.to_table() on full target dataset
- AND use filtered scanners for all target data access
- AND maintain memory usage bounded by batch size

### Requirement: Per-Group Processing

merge operations SHALL process data in streaming batches.

#### Scenario: Batch row processing
- WHEN processing target batches via scanner.to_batches()
- THEN process each batch to identify insert/update/delete actions
- AND build output table for each batch
- AND accumulate statistics across batches

#### Scenario: Batch size control
- WHEN batch_rows parameter is specified
- THEN limit each batch to at most batch_rows rows
- AND adjust memory usage accordingly
- AND ensure deterministic processing regardless of batch size

#### Scenario: Source row handling
- WHEN processing all target batches
- THEN append remaining source rows that should be inserted
- AND handle cases where source keys had no target matches
- AND ensure final dataset includes all appropriate source rows

### Requirement: Enhanced Schema Compatibility

merge operations SHALL use shared schema compatibility helpers.

#### Scenario: Schema unification
- WHEN source and target schemas have compatible types
- THEN use unify_schemas() from shared core for type promotion
- AND apply consistent type casting across both backends
- AND preserve existing casting behavior for compatible schemas

#### Scenario: Type promotion
- WHEN schemas have different but compatible types (e.g., int32 vs int64)
- THEN promote to widest compatible type
- AND maintain data integrity during merge
- AND apply consistent promotion rules

#### Scenario: Schema incompatibility
- WHEN source and target schemas have incompatible types
- THEN raise ValueError with clear schema mismatch message
- AND identify incompatible column names or types
- AND suggest resolution approaches when possible

### Requirement: Canonical Statistics Structure

merge_parquet_dataset_pyarrow SHALL return canonical statistics structure.

#### Scenario: Statistics format
- WHEN merge operation completes successfully
- THEN return dict with keys: "inserted", "updated", "deleted", "total"
- AND all values are non-negative integers
- AND "total" equals final target dataset row count

#### Scenario: Statistics calculation
- WHEN calculating merge statistics
- THEN count actual rows inserted, updated, deleted during operation
- AND accumulate statistics incrementally during batch processing
- AND ensure statistics sum to expected totals

#### Scenario: Empty source statistics
- WHEN source table is empty
- THEN return {"inserted": 0, "updated": 0, "deleted": target_row_count, "total": 0}
- IF strategy="full_merge" and target exists
- AND target is removed completely

### Requirement: Edge Case Alignment

Edge case behavior SHALL match shared canonical definitions.

#### Scenario: Empty target + UPSERT/INSERT
- WHEN target dataset does not exist and strategy is "upsert" or "insert"
- THEN all source rows are inserted into new dataset
- AND statistics.deleted = 0
- AND statistics.total = source row count

#### Scenario: Empty target + UPDATE
- WHEN target dataset does not exist and strategy is "update"
- THEN zero rows are updated
- AND statistics.updated = 0
- AND no dataset is created (consistent no-op behavior)

#### Scenario: Filtered scanner NULL key detection
- WHEN checking for NULL keys in target dataset
- THEN use scanner.filter() with IS NULL conditions on key columns
- AND detect NULL values without full dataset materialization
- AND raise appropriate error before processing begins

#### Scenario: Deduplication ordering
- WHEN strategy="deduplicate" and dedup_order_by is specified
- THEN keep records with highest values in dedup_order_by columns
- AND apply sorting before deduplication
- AND maintain consistent ordering with DuckDB backend