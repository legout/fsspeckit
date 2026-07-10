# Capability: PyArrow Parquet Dataset Merge

## ADDED Requirements

### Requirement: Merge Parquet Dataset with Strategies (PyArrow)

The system SHALL provide a `merge_parquet_dataset_pyarrow` helper that merges a
source table or parquet dataset into a target parquet dataset directory using
PyArrow only, with configurable merge strategies.

#### Scenario: UPSERT with single key column

- **WHEN** user calls  
  `merge_parquet_dataset_pyarrow(source, target_path, key_columns=["id"], strategy="upsert")`
- **AND** `source` contains ids [1, 2, 3] where id=1,2 exist in the target
  dataset
- **THEN** the helper rewrites the target dataset so that rows for id=1,2 are
  updated with source values, id=3 is inserted, and all other rows are
  preserved.

#### Scenario: INSERT / UPDATE / FULL_MERGE / DEDUPLICATE

- **WHEN** the caller selects `strategy="insert"|"update"|"full_merge"|"deduplicate"`
- **THEN** the behavior for inserts, updates, deletes, and deduplication SHALL
  match the semantics defined by the DuckDB merge capability:
  - INSERT: insert only rows not present in the target.
  - UPDATE: update only rows already present; ignore new ones.
  - FULL_MERGE: insert new, update matching rows, and delete rows missing from
    the source (full sync).
  - DEDUPLICATE: deduplicate the source on `key_columns` (keeping the preferred
    record according to `dedup_order_by`) and then perform an UPSERT-style
    merge.

### Requirement: Merge Statistics Reporting (PyArrow)

The helper SHALL return a statistics dictionary with the following keys:
`"inserted"`, `"updated"`, `"deleted"`, and `"total"` representing the number
of inserted, updated, deleted, and final total rows in the merged dataset.

#### Scenario: Stats reflect merge operations

- **WHEN** a merge inserts 5 new rows, updates 10 existing rows, and deletes 3
  rows
- **THEN** the returned stats SHALL be
  `{"inserted": 5, "updated": 10, "deleted": 3, "total": <final_count>}`
- **AND** `total` SHALL equal `previous_total + inserted - deleted`.

### Requirement: Key Column and Schema Validation (PyArrow)

The helper SHALL validate that the requested key columns exist and are safe to
use for joining, and that the source and target schemas are compatible.

#### Scenario: Missing key columns or NULL keys

- **WHEN** any column in `key_columns` is missing from the source or target
  schemas
- **OR** contains NULL values in either source or target
- **THEN** the helper SHALL raise `ValueError` with an informative message
  indicating the problematic key(s)
- **AND** no merge output SHALL be written.

#### Scenario: Schema incompatibility

- **WHEN** source and target columns with the same name have incompatible types
  according to `utils-pyarrow` schema compatibility rules
- **THEN** the helper SHALL raise a descriptive error (e.g. `TypeError`) instead
  of performing a merge.

### Requirement: Filtered, Batch-Oriented Merge Execution

The helper SHALL avoid full in-memory materialization of large datasets and MUST
use PyArrow dataset scanners and compute filters to restrict what is loaded into
memory.

#### Scenario: Key-filtered scanning on large target

- **WHEN** the target dataset has many files (e.g. tens of millions of rows)
- **AND** `merge_parquet_dataset_pyarrow` is invoked
- **THEN** the implementation MUST:
  - Process the source in batches.
  - For each batch, construct a filter on the target such as
    `pc.field("id").is_in(batch_keys)` using `pyarrow.compute`.
  - Use `pyarrow.dataset.Scanner` or equivalent to read only rows matching that
    filter from the target.
  - Avoid calling unfiltered `dataset.to_table()` on the entire target dataset.

#### Scenario: Partition-limited merge

- **WHEN** the source is partitioned (e.g. `/staging/updates/date=2025-11-15`)
- **THEN** the implementation SHOULD take advantage of partition information
  (where available) to further restrict the target scan to relevant partitions
  before applying the key-based filter.

