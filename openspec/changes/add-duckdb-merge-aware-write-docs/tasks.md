## 1. API Documentation Updates

- [x] 1.1 Update `docs/api/fsspeckit.datasets.md` to include DuckDB merge-aware write parameters
- [x] 1.2 Add API documentation for DuckDB convenience helpers:
  - [x] 1.2.1 `DuckDBDatasetIO.insert_dataset` function documentation
  - [x] 1.2.2 `DuckDBDatasetIO.upsert_dataset` function documentation  
  - [x] 1.2.3 `DuckDBDatasetIO.update_dataset` function documentation
  - [x] 1.2.4 `DuckDBDatasetIO.deduplicate_dataset` function documentation
  - [x] 1.2.5 `DuckDBParquetHandler` re-export documentation
- [x] 1.3 Document `write_parquet_dataset` `strategy` and `key_columns` parameters
- [x] 1.4 Ensure parameter types, defaults, and requirements are accurately documented
- [x] 1.5 Document return value differences for merge vs standard writes

## 2. How-to Guide Updates

- [x] 2.1 Add "DuckDB Merge-Aware Dataset Writes" section to `docs/how-to/read-and-write-datasets.md`
- [x] 2.2 Document basic DuckDB merge-aware write syntax with examples for each strategy
- [x] 2.3 Add DuckDB convenience helper examples showing equivalent functionality
- [x] 2.4 Include advanced examples (composite keys, compression interactions, error handling)
- [x] 2.5 Update `docs/how-to/index.md` to include new merge guide link

## 3. Unified Merge Guide

- [x] 3.1 Create or update `docs/how-to/merge-datasets.md` to cover both DuckDB and PyArrow
- [x] 3.2 Document DuckDB-specific merge strategy behavior and examples:
  - [x] 3.2.1 INSERT strategy use cases and examples
  - [x] 3.2.2 UPSERT strategy use cases and examples
  - [x] 3.2.3 UPDATE strategy use cases and examples
  - [x] 3.2.4 FULL_MERGE strategy use cases and examples
  - [x] 3.2.5 DEDUPLICATE strategy use cases and examples
- [x] 3.3 Add backend selection guidance (DuckDB vs PyArrow)
- [x] 3.4 Include performance characteristics and use case recommendations
- [x] 3.5 Add error handling and validation guidance specific to DuckDB

## 4. Example Integration

- [x] 4.1 Integrate existing `examples/duckdb/duckdb_merge_example.py` into documentation
- [x] 4.2 Integrate existing `examples/duckdb/duckdb_dataset_write_example.py` into documentation
- [x] 4.3 Add cross-references from how-to guides to existing examples
- [x] 4.4 Update `examples/README.md` to highlight merge examples
- [x] 4.5 Ensure all examples are accessible from documentation

## 5. Documentation Quality and Consistency

- [x] 5.1 Review all new DuckDB documentation for accuracy and completeness
- [x] 5.2 Ensure consistent terminology and style with existing docs
- [x] 5.3 Add cross-references between DuckDB and PyArrow merge documentation
- [x] 5.4 Validate all code examples in documentation
- [x] 5.5 Test documentation build process with new content
- [x] 5.6 Correct OpenSpec task status in `add-duckdb-merge-aware-write/tasks.md` to reflect actual completion state