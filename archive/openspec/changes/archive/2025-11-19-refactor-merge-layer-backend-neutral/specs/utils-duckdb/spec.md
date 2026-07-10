# Merge Dataset Operations

## ADDED Requirements

### Requirement: Shared Merge Core Integration

DuckDBParquetHandler.merge_parquet_dataset SHALL use shared validation and statistics from fsspeckit.core.merge.

#### Scenario: Backend-neutral validation usage
- WHEN calling merge_parquet_dataset
- THEN function calls normalize_keys() for key column normalization
- AND function calls validate_keys_and_schema() for shared validation
- AND function calls compute_merge_stats() for canonical statistics

### Requirement: Canonical Key Validation

merge_parquet_dataset SHALL perform canonical key validation using backend-neutral helpers.

#### Scenario: Key normalization
- WHEN user provides key_columns as string or list
- THEN function calls normalize_keys() to convert to list format
- AND returns normalized list for internal use

#### Scenario: Key existence validation
- WHEN source or target schemas are available
- THEN function validates all key columns exist in both schemas
- AND raises ValueError with clear message for missing keys

#### Scenario: NULL key detection
- WHEN processing merge data
- THEN function checks for NULL values in key columns
- AND raises ValueError before processing if NULL keys found
- AND error message identifies which key columns contain NULL values

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

### Requirement: Streaming Execution Requirements

merge_parquet_dataset SHALL process target datasets in streaming fashion.

#### Scenario: Avoid full dataset materialization
- WHEN processing target dataset for merge
- THEN use parquet_scan with SQL filters instead of read_parquet(target_path)
- AND process data in SQL queries without Python materialization
- AND memory usage remains bounded relative to batch size

#### Scenario: Key-based filtering
- WHEN matching source keys to target dataset
- THEN construct SQL WHERE clauses using key column values
- AND only read target rows with matching keys
- AND minimize I/O by reading only relevant data

#### Scenario: Batch processing
- WHEN processing large target datasets
- THEN execute merge in batches if target exceeds reasonable size
- AND maintain atomicity across batch boundaries
- AND process each batch independently

### Requirement: Canonical Statistics Structure

merge_parquet_dataset SHALL return canonical statistics structure.

#### Scenario: Statistics format
- WHEN merge operation completes successfully
- THEN return dict with keys: "inserted", "updated", "deleted", "total"
- AND all values are non-negative integers
- AND "total" equals final target dataset row count

#### Scenario: Statistics calculation
- WHEN calculating merge statistics
- THEN count actual rows inserted, updated, deleted during operation
- AND avoid heuristic estimates or approximations
- AND ensure statistics sum to expected totals

#### Scenario: Empty source statistics
- WHEN source table is empty
- THEN return {"inserted": 0, "updated": 0, "deleted": target_row_count, "total": 0}
- IF strategy="full_merge" and target exists
- AND target is removed completely

### Requirement: Atomicity and Error Handling

merge operations SHALL maintain dataset atomicity.

#### Scenario: Atomic write operation
- WHEN writing merged dataset
- THEN write to temporary directory first
- AND swap with original directory only after successful write
- AND preserve original dataset if merge fails mid-operation

#### Scenario: Empty target handling
- WHEN target dataset does not exist
- THEN create new dataset with source data
- AND behavior consistent across all merge strategies
- AND provide clear status in returned statistics

#### Scenario: Schema incompatibility
- WHEN source and target schemas are incompatible
- THEN raise ValueError with clear schema mismatch message
- AND identify incompatible column names or types
- AND suggest resolution approaches when possible

### Requirement: Edge Case Alignment

Edge case behavior SHALL match shared canonical definitions.

#### Scenario: Empty target + UPSERT/INSERT
- WHEN target dataset is empty and strategy is "upsert" or "insert"
- THEN all source rows are inserted
- AND statistics.deleted = 0
- AND statistics.total = source row count

#### Scenario: Empty target + UPDATE
- WHEN target dataset is empty and strategy is "update"
- THEN zero rows are updated
- AND statistics.updated = 0
- AND statistics.total = 0 (no change)

#### Scenario: Schema compatibility
- WHEN source and target schemas have compatible types
- THEN use unify_schemas() for type promotion
- AND apply consistent type casting
- AND preserve data integrity during merge

#### Scenario: Invalid strategy
- WHEN strategy parameter is not in valid strategies list
- THEN raise ValueError with list of valid strategies
- AND include current invalid value in error message