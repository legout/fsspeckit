# datasets-parquet-io Specification (Delta)

## ADDED Requirements

### Requirement: `merge` Supports Insert, Update, Upsert (DuckDB)
The DuckDB dataset IO SHALL provide `merge(..., strategy=...)` with `strategy in {"insert","update","upsert"}`.

#### Scenario: Insert appends only new keys
- **GIVEN** a target dataset exists with key `id=1`
- **WHEN** user calls `merge(source, path, strategy="insert", key_columns=["id"])` with keys `id=1` and `id=2`
- **THEN** the system SHALL write parquet file(s) containing only rows for `id=2`
- **AND** SHALL NOT rewrite existing parquet files

#### Scenario: Update preserves unaffected rows
- **GIVEN** a target dataset exists with rows for keys `id=1` and `id=2`
- **AND** the source provides only key `id=2`
- **WHEN** user calls `merge(source, path, strategy="update", key_columns=["id"])`
- **THEN** the resulting dataset SHALL still contain the row for `id=1`
- **AND** SHALL update the row for `id=2`

#### Scenario: Composite keys work for upsert
- **GIVEN** a target dataset exists with keys `(id=1, category="A")` and `(id=2, category="B")`
- **WHEN** user calls `merge(source, path, strategy="upsert", key_columns=["id","category"])`
- **THEN** the system SHALL treat the composite tuple as the key
- **AND** SHALL update matching tuples and insert only non-matching tuples

#### Scenario: Conservative pruning + key-scan confirmation
- **GIVEN** a target dataset exists with many parquet files
- **AND** the source keys touch only a subset of those files
- **WHEN** user calls `merge(source, path, strategy="update", key_columns=[...])`
- **THEN** the system SHALL prune candidate files conservatively (partition parsing + parquet stats when available)
- **AND** SHALL confirm affected files by scanning only `key_columns`
- **AND** SHALL rewrite only confirmed affected files

### Requirement: Merge Returns File Metadata (DuckDB)
`merge` SHALL return metadata for files it rewrites or writes.

#### Scenario: Upsert returns metadata for rewritten and inserted files
- **WHEN** user calls `merge(source, path, strategy="upsert", key_columns=["id"])`
- **THEN** the result SHALL include metadata for each rewritten file
- **AND** SHALL include metadata for each newly written insert file

#### Scenario: Parquet metadata is the default metadata source
- **WHEN** the system needs per-file row counts and size for `MergeFileMetadata`
- **THEN** it SHOULD prefer parquet metadata (`duckdb.parquet_metadata(...)` or `pyarrow.parquet.read_metadata`)
- **AND** MAY use `COPY ... RETURN_STATS` as an optimization but MUST NOT depend on it for correctness

### Requirement: Single Canonical Incremental Merge Path (DuckDB)
The DuckDB backend SHALL implement incremental merge semantics in one place and SHALL reuse it across APIs.

#### Scenario: `write_parquet_dataset(rewrite_mode="incremental")` delegates to `merge`
- **GIVEN** a DuckDB handler supports both `merge(...)` and `write_parquet_dataset(..., rewrite_mode="incremental")`
- **WHEN** user calls `write_parquet_dataset(source, path, strategy="update"|"upsert", rewrite_mode="incremental")`
- **THEN** the handler SHOULD delegate to the same incremental merge implementation used by `merge(...)`
