## MODIFIED Requirements

### Requirement: Provide comprehensive merge strategy guidance

Users SHALL have access to comprehensive documentation explaining when and how to use the `merge()` method with each merge strategy.

#### Scenario: Strategy selection guidance
- **WHEN** a user needs to perform merge operations
- **THEN** documentation SHALL demonstrate the `merge()` method signature:
  `io.merge(data, path, strategy='insert'|'update'|'upsert', key_columns=...)`
- **AND** SHALL provide clear guidance on selecting the appropriate strategy
- **AND** SHALL explain incremental file-level rewrite behavior
- **AND** SHALL document the `MergeResult` return type with its fields

#### Scenario: Practical examples for all strategies
- **WHEN** a user wants to implement merge operations
- **THEN** documentation SHALL include working examples using `io.merge(data, path, strategy=..., key_columns=...)`
- **AND** SHALL show `MergeResult` field access patterns
- **AND** SHALL demonstrate common patterns like composite keys and partition handling

### Requirement: PyArrow Dataset Handler Class Documentation

The documentation SHALL include comprehensive coverage of PyArrow class-based dataset handlers (`PyarrowDatasetIO`, `PyarrowDatasetHandler`) with same level of detail as DuckDB handler documentation.

#### Scenario: Getting started tutorial includes correct PyArrow examples
- **WHEN** a user follows `docs/tutorials/getting-started.md`
- **THEN** the "Your First Dataset Operation" section SHALL show:
  - `io.write_dataset(data, path, mode='append')` for writes
  - `io.merge(data, path, strategy='upsert', key_columns=[...])` for merges
- **AND** SHALL demonstrate `WriteDatasetResult` and `MergeResult` usage

#### Scenario: How-to guides demonstrate current APIs
- **WHEN** a user reads `docs/how-to/read-and-write-datasets.md` or `docs/how-to/merge-datasets.md`
- **THEN** all examples SHALL use `write_dataset()` and `merge()` methods
- **AND** import statements SHALL show correct PyArrow class imports

### Requirement: Provide comprehensive DuckDB merge strategy guidance

Users SHALL have access to comprehensive documentation explaining when and how to use each DuckDB merge strategy.

#### Scenario: DuckDB strategy selection guidance
- **WHEN** a user needs to perform merge operations with DuckDB
- **THEN** documentation SHALL demonstrate `DuckDBDatasetIO.merge()` method
- **AND** SHALL include real-world use cases for each strategy
- **AND** SHALL explain DuckDB-specific behavior of each strategy with existing and new data

#### Scenario: DuckDB vs PyArrow backend selection
- **WHEN** a user chooses between DuckDB and PyArrow for merge operations
- **THEN** documentation SHALL provide guidance on backend selection
- **AND** SHALL include performance characteristics
- **AND** SHALL document feature differences
- **AND** SHALL recommend use cases for each backend

## ADDED Requirements

### Requirement: Dataset write documentation uses write_dataset API

The documentation SHALL demonstrate the `write_dataset()` method for append and overwrite operations.

#### Scenario: Write operations use write_dataset
- **WHEN** a user reads `docs/how-to/read-and-write-datasets.md`
- **THEN** append examples SHALL use `io.write_dataset(data, path, mode='append')`
- **AND** overwrite examples SHALL use `io.write_dataset(data, path, mode='overwrite')`
- **AND** examples SHALL demonstrate accessing `WriteDatasetResult` fields
- **AND** examples SHALL show per-file metadata from the result

#### Scenario: Clear separation of write vs merge
- **WHEN** a user reads dataset documentation
- **THEN** documentation SHALL clearly distinguish:
  - Write operations (append/overwrite): `write_dataset()`
  - Merge operations (insert/update/upsert): `merge()`
- **AND** SHALL explain when to use each approach

### Requirement: Document WriteDatasetResult type

The documentation SHALL include comprehensive coverage of the `WriteDatasetResult` type returned by write operations.

#### Scenario: WriteDatasetResult field documentation
- **WHEN** a user reads dataset write documentation
- **THEN** they SHALL find documentation for `WriteDatasetResult` with its fields:
  - `files`: list of `FileWriteMetadata` (path, row_count, size_bytes)
  - `total_rows`: total rows written
  - `mode`: the write mode used ('append' or 'overwrite')
  - `backend`: the backend used ('pyarrow' or 'duckdb')

#### Scenario: WriteDatasetResult usage examples
- **WHEN** a user implements write operations
- **THEN** documentation SHALL show how to:
  - Access result fields for logging and monitoring
  - Iterate over written files for downstream processing
  - Check operation success via result counts

### Requirement: Document MergeResult type

The documentation SHALL include comprehensive coverage of the `MergeResult` type returned by merge operations.

#### Scenario: MergeResult field documentation
- **WHEN** a user reads merge documentation
- **THEN** they SHALL find documentation for `MergeResult` with its fields:
  - `strategy`: the merge strategy used
  - `source_count`, `target_count_before`, `target_count_after`: row counts
  - `inserted`, `updated`, `deleted`: operation counts
  - `files`: list of `MergeFileMetadata`
  - `rewritten_files`, `inserted_files`, `preserved_files`: file path lists

#### Scenario: MergeResult usage examples
- **WHEN** a user implements merge operations
- **THEN** documentation SHALL show how to:
  - Access result fields for logging and monitoring
  - Verify operation success via inserted/updated counts
  - Identify which files were affected by the merge
