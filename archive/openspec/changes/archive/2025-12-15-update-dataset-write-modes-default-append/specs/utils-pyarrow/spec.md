## ADDED Requirements

### Requirement: Parquet Dataset Writes Support Append and Overwrite (PyArrow)
The system SHALL provide `write_parquet_dataset(..., mode=...)` for the PyArrow dataset handler with safe append and overwrite behaviors.

#### Scenario: Default append creates additional files
- **GIVEN** a dataset directory already contains parquet files
- **WHEN** user calls `io.write_parquet_dataset(table, path)` twice without specifying `mode`
- **THEN** the second call SHALL create additional parquet file(s) (no filename collisions)
- **AND** existing parquet files SHALL be preserved
- **AND** reading the dataset SHALL return combined rows

#### Scenario: Overwrite deletes parquet files only
- **GIVEN** a dataset directory contains parquet files and non-parquet files (e.g. `README.txt`)
- **WHEN** user calls `io.write_parquet_dataset(table, path, mode="overwrite")`
- **THEN** the method SHALL delete only existing parquet files under `path`
- **AND** SHALL preserve non-parquet files
- **AND** SHALL write the new dataset contents to fresh parquet file(s)

### Requirement: Mode and Strategy Compatibility (PyArrow)
The system SHALL validate `mode` and reject incompatible combinations with merge `strategy`.

#### Scenario: Reject append with rewrite strategies
- **WHEN** user calls `io.write_parquet_dataset(table, path, mode="append", strategy="upsert")`
- **OR** uses `strategy="update"|"full_merge"|"deduplicate"`
- **THEN** the method SHALL raise `ValueError` indicating that `mode="append"` is not supported for the chosen strategy

#### Scenario: Insert + append writes only new keys
- **GIVEN** a target dataset exists with key `id=1`
- **AND** user provides a source table with keys `id=1` and `id=2`
- **WHEN** user calls `io.write_parquet_dataset(source, path, strategy="insert", key_columns=["id"], mode="append")`
- **THEN** the system SHALL write parquet file(s) containing only rows for `id=2`
- **AND** SHALL NOT delete or rewrite existing parquet files

