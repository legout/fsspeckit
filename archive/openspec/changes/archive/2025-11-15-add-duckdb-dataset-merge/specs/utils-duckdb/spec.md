# Capability: DuckDB Parquet Handler

## ADDED Requirements

### Requirement: Merge Parquet Dataset with UPSERT Strategy

The system SHALL provide a `merge_parquet_dataset` method that merges source data into target dataset using UPSERT strategy (insert new records, update existing records based on key columns).

#### Scenario: UPSERT with single key column

- **WHEN** user calls `handler.merge_parquet_dataset(source, target, key_columns=["id"], strategy="upsert")`
- **AND** source contains records with ids [1, 2, 3] (id=1,2 exist in target, id=3 is new)
- **THEN** method updates existing records (id=1,2) with new values from source
- **AND** inserts new record (id=3) into target
- **AND** preserves other target records not in source

#### Scenario: UPSERT with composite key

- **WHEN** user merges with composite key `key_columns=["customer_id", "order_date"]`
- **AND** source has records matching on both columns
- **THEN** method updates records where both key columns match
- **AND** inserts records where key combination doesn't exist
- **AND** treats partial key matches as different records

#### Scenario: UPSERT from PyArrow table source

- **WHEN** source is PyArrow table with new/updated records
- **THEN** method performs UPSERT using table data
- **AND** returns merge statistics with inserted and updated counts

#### Scenario: UPSERT from parquet path source

- **WHEN** source is path to parquet dataset (e.g., "/staging/updates/")
- **THEN** method reads source dataset and performs UPSERT
- **AND** handles source dataset with multiple files correctly

### Requirement: Merge Parquet Dataset with INSERT Strategy

The system SHALL support INSERT strategy that adds only new records from source, ignoring records that already exist in target.

#### Scenario: INSERT only new records

- **WHEN** user calls `merge_parquet_dataset(source, target, key_columns=["id"], strategy="insert")`
- **AND** source contains ids [1, 2, 3] where id=1,2 exist in target
- **THEN** method inserts only id=3 (new record)
- **AND** preserves existing target records unchanged
- **AND** ignores source records with matching keys

#### Scenario: INSERT with no new records

- **WHEN** all source records exist in target (based on key columns)
- **THEN** method completes without error
- **AND** target dataset remains unchanged
- **AND** returns statistics showing zero inserted records

#### Scenario: INSERT all records as new

- **WHEN** no source records exist in target
- **THEN** method inserts all source records
- **AND** returns statistics showing all records inserted

### Requirement: Merge Parquet Dataset with UPDATE Strategy

The system SHALL support UPDATE strategy that updates only existing records, ignoring new records from source.

#### Scenario: UPDATE only existing records

- **WHEN** user calls `merge_parquet_dataset(source, target, key_columns=["id"], strategy="update")`
- **AND** source contains ids [1, 2, 3] where id=1,2 exist in target
- **THEN** method updates existing records (id=1,2) with source values
- **AND** ignores new record (id=3)
- **AND** preserves other target records unchanged

#### Scenario: UPDATE with no matching records

- **WHEN** no source records exist in target (all are new)
- **THEN** method completes without error
- **AND** target dataset remains unchanged
- **AND** returns statistics showing zero updated records

#### Scenario: UPDATE all matching records

- **WHEN** all source records exist in target
- **THEN** method updates all source records in target
- **AND** returns statistics showing all records updated

### Requirement: Merge Parquet Dataset with FULL_MERGE Strategy

The system SHALL support FULL_MERGE strategy that inserts new records, updates existing records, and deletes records missing from source (synchronization).

#### Scenario: FULL_MERGE with inserts, updates, and deletes

- **WHEN** user calls `merge_parquet_dataset(source, target, key_columns=["id"], strategy="full_merge")`
- **AND** source has ids [2, 3, 4]
- **AND** target has ids [1, 2, 3]
- **THEN** method updates id=2,3 with source values
- **AND** inserts new id=4
- **AND** deletes id=1 (missing from source)

#### Scenario: FULL_MERGE replaces entire dataset

- **WHEN** target has records not in source
- **THEN** method removes all target records not in source
- **AND** final dataset matches source exactly
- **AND** returns statistics showing inserted, updated, and deleted counts

#### Scenario: FULL_MERGE with empty source

- **WHEN** source dataset is empty
- **THEN** method deletes all records from target
- **AND** target becomes empty dataset
- **AND** returns statistics showing all records deleted

### Requirement: Merge Parquet Dataset with DEDUPLICATE Strategy

The system SHALL support DEDUPLICATE strategy that removes duplicates from source before performing UPSERT merge.

#### Scenario: DEDUPLICATE with duplicate source records

- **WHEN** user calls `merge_parquet_dataset(source, target, key_columns=["id"], strategy="deduplicate")`
- **AND** source has duplicate ids [1, 1, 2] with different values
- **THEN** method removes duplicates from source first
- **AND** keeps last occurrence of each duplicate (by default)
- **AND** performs UPSERT with deduplicated source

#### Scenario: DEDUPLICATE with custom sort order

- **WHEN** user specifies `dedup_order_by=["timestamp"]` parameter
- **AND** source has duplicates with different timestamps
- **THEN** method keeps record with highest timestamp value
- **AND** removes other duplicates
- **AND** performs UPSERT with deduplicated result

#### Scenario: DEDUPLICATE with no duplicates

- **WHEN** source has no duplicate keys
- **THEN** method performs standard UPSERT (no deduplication needed)
- **AND** behavior identical to UPSERT strategy

### Requirement: Merge Statistics Reporting

The system SHALL return merge statistics indicating number of records inserted, updated, deleted, and total records in merged dataset.

#### Scenario: Return statistics for UPSERT

- **WHEN** merge completes successfully
- **THEN** method returns dictionary with statistics
- **AND** includes "inserted" count (new records added)
- **AND** includes "updated" count (existing records modified)
- **AND** includes "deleted" count (0 for UPSERT)
- **AND** includes "total" count (final dataset size)

#### Scenario: Statistics reflect actual operations

- **WHEN** 5 records inserted, 10 updated, 3 deleted
- **THEN** statistics show {"inserted": 5, "updated": 10, "deleted": 3, "total": <count>}
- **AND** total equals previous_total + inserted - deleted

#### Scenario: Statistics for INSERT strategy

- **WHEN** using INSERT strategy
- **THEN** statistics show only inserted count
- **AND** updated count is 0
- **AND** deleted count is 0

### Requirement: Key Column Validation

The system SHALL validate that key columns exist in both source and target datasets and contain no NULL values.

#### Scenario: Missing key column in source

- **WHEN** key_columns includes column not in source dataset
- **THEN** method raises ValueError with clear error message
- **AND** indicates which column is missing
- **AND** no merge operation is performed

#### Scenario: Missing key column in target

- **WHEN** key_columns includes column not in target dataset
- **THEN** method raises ValueError with clear error message
- **AND** indicates which column is missing
- **AND** no merge operation is performed

#### Scenario: NULL values in key columns

- **WHEN** source or target has NULL values in key columns
- **THEN** method raises ValueError indicating NULL keys not allowed
- **AND** suggests filtering or filling NULL values first
- **AND** no merge operation is performed

#### Scenario: Valid key columns

- **WHEN** all key columns exist in both source and target
- **AND** no NULL values in key columns
- **THEN** method proceeds with merge operation
- **AND** uses specified columns for record matching

### Requirement: Schema Compatibility Validation

The system SHALL validate that source and target datasets have compatible schemas before merging.

#### Scenario: Matching schemas

- **WHEN** source and target have identical column names and types
- **THEN** method proceeds with merge
- **AND** no schema errors are raised

#### Scenario: Schema mismatch - missing column

- **WHEN** source has column not in target (or vice versa)
- **THEN** method raises ValueError indicating schema mismatch
- **AND** lists columns that don't match
- **AND** no merge operation is performed

#### Scenario: Schema mismatch - type incompatibility

- **WHEN** source and target have same column names but different types
- **THEN** method raises TypeError indicating incompatible types
- **AND** shows column name and type difference
- **AND** no merge operation is performed

### Requirement: Merge with Compression

The system SHALL support configurable compression for merged dataset output.

#### Scenario: Merge with custom compression

- **WHEN** user specifies `compression="gzip"` parameter
- **THEN** method writes merged dataset with gzip compression
- **AND** all output files use specified compression

#### Scenario: Merge with default compression

- **WHEN** user doesn't specify compression parameter
- **THEN** method uses default snappy compression
- **AND** merged dataset written with snappy compression

### Requirement: Merge Error Handling

The system SHALL provide clear error messages for invalid merge operations and handle errors gracefully.

#### Scenario: Invalid strategy value

- **WHEN** user provides strategy not in valid set
- **THEN** method raises ValueError listing valid strategies
- **AND** no merge operation is performed

#### Scenario: Empty target dataset

- **WHEN** target dataset path doesn't exist or is empty
- **AND** strategy is UPSERT or INSERT
- **THEN** method creates new dataset with source data
- **AND** treats as initial data load

#### Scenario: Empty target with UPDATE strategy

- **WHEN** target dataset is empty
- **AND** strategy is UPDATE
- **THEN** method completes with no changes
- **AND** returns statistics showing zero updates

#### Scenario: Merge failure mid-operation

- **WHEN** merge operation fails during processing
- **THEN** method raises exception with clear error message
- **AND** target dataset remains in original state (atomic operation)
- **AND** no partial merge is left behind

### Requirement: Merge Performance Optimization

The system SHALL optimize merge operations for performance using DuckDB's analytical query engine.

#### Scenario: Efficient key matching

- **WHEN** merging datasets with key columns
- **THEN** method uses DuckDB joins for key matching
- **AND** leverages DuckDB query optimization
- **AND** completes merge in reasonable time for large datasets

#### Scenario: Memory-efficient merge

- **WHEN** merging large datasets
- **THEN** method uses DuckDB's larger-than-memory capabilities
- **AND** doesn't require entire dataset in Python memory
- **AND** handles datasets larger than available RAM

### Requirement: Merge Documentation and Examples

The system SHALL provide comprehensive documentation and examples for all merge strategies.

#### Scenario: Docstring completeness

- **WHEN** user accesses method documentation
- **THEN** docstring includes all parameters
- **AND** explains each merge strategy with examples
- **AND** shows expected behavior for each strategy

#### Scenario: Example availability

- **WHEN** user looks for merge examples
- **THEN** example scripts demonstrate each strategy
- **AND** show common use cases (CDC, incremental loads, deduplication)
- **AND** include performance best practices

## MODIFIED Requirements

None - all existing requirements remain unchanged. This change adds new merge functionality without modifying existing behavior.
