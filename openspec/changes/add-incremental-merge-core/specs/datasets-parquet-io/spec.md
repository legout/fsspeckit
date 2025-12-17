# datasets-parquet-io Specification (Delta)

## ADDED Requirements

### Requirement: Incremental Merge Planning Uses Partition and Statistics Pruning
Merge planning SHALL reduce candidate files using partition information and parquet metadata statistics where available.

#### Scenario: Partition pruning reduces candidate files
- **GIVEN** a hive-partitioned dataset under `path` (e.g. `day=2025-01-01/`)
- **AND** the source contains only rows for a single partition value
- **WHEN** the system plans a merge for `key_columns=["id"]`
- **THEN** the system SHALL consider only files within the matching partition(s) as candidates
- **AND** SHALL treat files with unknown partition values conservatively (not pruned)

#### Scenario: Statistics pruning excludes non-overlapping files
- **GIVEN** parquet metadata contains exact min/max statistics for `key_columns`
- **AND** a parquet file’s key range does not overlap the source key range
- **WHEN** the system plans a merge
- **THEN** that parquet file SHALL be excluded from rewrite candidates

### Requirement: Conservative Pruning Preserves Correctness
When file membership cannot be proven via partition info or metadata statistics, the system SHALL behave conservatively.

#### Scenario: Missing or inexact stats treated as unknown membership
- **WHEN** a parquet file has missing or inexact key statistics
- **THEN** the system SHALL treat that file as a candidate (not pruned)

### Requirement: Full Row Replacement Semantics
For merge operations, matching keys SHALL be updated via full-row replacement.

#### Scenario: Update replaces full rows
- **GIVEN** a target dataset row with key `id=1` and columns `{id, a, b}`
- **AND** the source provides a row for `id=1` with columns `{id, a, b}`
- **WHEN** the system merges using `key_columns=["id"]`
- **THEN** the resulting row for `id=1` SHALL equal the source row for all columns

### Requirement: Partition Columns Cannot Change (Initial Implementation)
For partitioned datasets, merge operations SHALL reject updates that would move existing keys between partitions.

#### Scenario: Reject partition movement on update
- **GIVEN** a dataset is partitioned by `["day"]`
- **AND** the target contains key `id=1` in partition `day=2025-01-01`
- **AND** the source provides key `id=1` with `day=2025-01-02`
- **WHEN** the system merges using `key_columns=["id"]` and `partition_by=["day"]`
- **THEN** the method SHALL raise `ValueError` indicating partition columns cannot change for existing keys

