## ADDED Requirements

### Requirement: Partitioned merge inserts preserve hive layout
When `partition_columns` are provided for a merge, the system SHALL write inserted rows using the dataset's hive partition layout so that new parquet files land under `col=value/` directories derived from the source data.

#### Scenario: Insert writes to partition directories
- **GIVEN** a dataset partitioned by `day` with existing files under `day=2025-01-01/`
- **AND** the source contains new rows with `day=2025-01-02`
- **WHEN** the system merges using `strategy="insert"` or `strategy="upsert"` and `partition_columns=["day"]`
- **THEN** the newly written parquet files SHALL be placed under `day=2025-01-02/`

### Requirement: Hive partitioned merges keep file schemas consistent
For hive-partitioned datasets, merge rewrites SHALL NOT inject partition columns into Parquet file schemas by default; partition values SHALL be inferred from the file path to keep schemas consistent across the dataset.

#### Scenario: Rewritten files do not add partition columns
- **GIVEN** a hive-partitioned dataset where existing parquet files do not include the partition column in their schema
- **WHEN** the system rewrites affected files during `strategy="update"` or `strategy="upsert"`
- **THEN** the rewritten files SHALL preserve the original schema and SHALL NOT add the partition column as a data field
