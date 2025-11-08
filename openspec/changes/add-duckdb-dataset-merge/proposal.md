# Change Proposal: Add Dataset Merge Capabilities to DuckDB Handler

## Why

Parquet datasets often need to be updated with new data while preserving existing records. Current capabilities only support full replacement (overwrite) or simple appending. Real-world data workflows require intelligent merging:

- **Data updates**: Update existing records with new values based on a primary key
- **Incremental loads**: Add only new records, skip duplicates
- **Change data capture (CDC)**: Apply inserts, updates, and deletes from source systems
- **Data quality**: Remove duplicate records before merging
- **Slowly changing dimensions**: Track changes to dimensional data over time

Without merge capabilities, users must:
1. Read entire dataset into memory
2. Perform merge logic in application code
3. Overwrite entire dataset with merged result

This is inefficient, memory-intensive, and error-prone for large datasets.

## What Changes

- Add `merge_parquet_dataset` method to `DuckDBParquetHandler` class
- Support five merge strategies:
  1. **UPSERT**: Insert new records, update existing (based on key columns)
  2. **INSERT**: Insert only new records, ignore existing
  3. **UPDATE**: Update only existing records, ignore new
  4. **FULL_MERGE**: Insert new, update existing, delete missing
  5. **DEDUPLICATE**: Remove duplicates from source before merging
- Support key column specification for identifying matching records
- Implement efficient merge using DuckDB SQL operations
- Support merge with source as PyArrow table or path to parquet dataset
- Add validation for key columns and merge strategies
- Implement optimized merge that minimizes data rewriting

## Impact

- Affected specs: `utils-duckdb` (modify existing capability)
- Affected code:
  - Modified: `src/fsspeckit/utils/duckdb.py` (add merge_parquet_dataset method)
  - Tests: `tests/test_utils/test_duckdb.py` (add merge tests)
  - Examples: `examples/duckdb/` (add merge examples)
- Dependencies: No new dependencies (uses DuckDB SQL)
- No breaking changes (additive feature)
- Extends existing `DuckDBParquetHandler` functionality
- Performance benefit: In-database merge vs. application-level merge
