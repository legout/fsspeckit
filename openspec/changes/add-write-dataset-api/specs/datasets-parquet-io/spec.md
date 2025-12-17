# datasets-parquet-io Specification (Delta)

## ADDED Requirements

### Requirement: `write_dataset` Supports Append and Overwrite
The system SHALL provide `write_dataset(..., mode=...)` for parquet dataset writes with `mode in {"append","overwrite"}`.

#### Scenario: Append writes new parquet files only
- **GIVEN** a dataset directory already contains parquet files
- **WHEN** user calls `write_dataset(table, path, mode="append")`
- **THEN** the system SHALL write additional parquet file(s) under `path`
- **AND** SHALL NOT delete or rewrite existing parquet files

#### Scenario: Overwrite replaces parquet data files
- **GIVEN** a dataset directory contains parquet files and non-parquet files (e.g. `README.txt`)
- **WHEN** user calls `write_dataset(table, path, mode="overwrite")`
- **THEN** the system SHALL remove existing parquet *data* files under `path`
- **AND** MAY preserve non-parquet files
- **AND** SHALL write the new dataset contents to fresh parquet file(s)

### Requirement: Return File Metadata for Newly Written Files
`write_dataset` SHALL return metadata entries for each parquet file it writes.

#### Scenario: Append returns written file metadata
- **WHEN** user calls `write_dataset(table, path, mode="append")`
- **THEN** the result SHALL include metadata entries for each newly written file
- **AND** each entry SHALL include at least file path and row count

