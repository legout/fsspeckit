## MODIFIED Requirements

### Requirement: `merge` Supports Insert, Update, Upsert (PyArrow)
The PyArrow dataset IO SHALL provide `merge(..., strategy=...)` with `strategy in {"insert","update","upsert"}`.

#### Scenario: Update rewrites only affected files
- **GIVEN** a target dataset exists with many parquet files
- **WHEN** user calls `merge(source, path, strategy="update", key_columns=["id"])`
- **THEN** the system SHALL rewrite only parquet files that actually contain keys present in `source`
- **AND** SHALL preserve all other parquet files unchanged
- **AND** SHALL NOT write inserts for keys not present in the target

## REMOVED Requirements

### Requirement: Single Canonical Incremental Merge Path (DuckDB)
**Reason**: This requirement is specific to DuckDB and already covered in `datasets-duckdb`.
**Migration**: None.
