## ADDED Requirements

### Requirement: PyArrow merge honors partition_columns on insert writes
When `partition_columns` are provided to PyArrow merge helpers, inserted rows SHALL be written using hive partitioning so that new files are placed under `col=value/` directories consistent with the target dataset layout.

#### Scenario: PyArrow upsert writes to partition directories
- **GIVEN** a dataset partitioned by `day` with existing files under `day=2025-01-01/`
- **AND** the source contains new rows with `day=2025-01-02`
- **WHEN** the user calls `io.merge(..., strategy="upsert", partition_columns=["day"])`
- **THEN** the inserted parquet files SHALL be written under `day=2025-01-02/`
