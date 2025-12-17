# datasets-parquet-io Specification (Delta)

## ADDED Requirements

### Requirement: `merge` Supports Insert, Update, Upsert (PyArrow)
The PyArrow dataset IO SHALL provide `merge(..., strategy=...)` with `strategy in {"insert","update","upsert"}`.

#### Scenario: Insert appends only new keys
- **GIVEN** a target dataset exists with key `id=1`
- **WHEN** user calls `merge(source, path, strategy="insert", key_columns=["id"])` with keys `id=1` and `id=2`
- **THEN** the system SHALL write parquet file(s) containing only rows for `id=2`
- **AND** SHALL NOT rewrite existing parquet files

#### Scenario: Update rewrites only affected files
- **GIVEN** a target dataset exists with many parquet files
- **WHEN** user calls `merge(source, path, strategy="update", key_columns=["id"])`
- **THEN** the system SHALL rewrite only parquet files that actually contain keys present in `source`
- **AND** SHALL preserve all other parquet files unchanged
- **AND** SHALL NOT write inserts for keys not present in the target

#### Scenario: Upsert rewrites affected files and appends inserts
- **GIVEN** a target dataset exists with many parquet files
- **WHEN** user calls `merge(source, path, strategy="upsert", key_columns=["id"])`
- **THEN** the system SHALL rewrite only parquet files that actually contain keys present in `source`
- **AND** SHALL write additional new parquet file(s) for keys not present in the target
- **AND** SHALL preserve all other parquet files unchanged

### Requirement: Merge Returns File Metadata (PyArrow)
`merge` SHALL return metadata for files it rewrites or writes.

#### Scenario: Upsert returns metadata for rewritten and inserted files
- **WHEN** user calls `merge(source, path, strategy="upsert", key_columns=["id"])`
- **THEN** the result SHALL include metadata for each rewritten file
- **AND** SHALL include metadata for each newly written insert file

