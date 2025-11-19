## MODIFIED Requirements

### Requirement: Write Parquet Dataset with Unique Filenames

The system SHALL provide a `write_parquet_dataset` method that writes PyArrow tables to parquet dataset directories with automatically generated unique filenames using a UUID-based mechanism.

#### Scenario: Write dataset with default UUID filenames

- **WHEN** user calls `handler.write_parquet_dataset(table, "/path/to/dataset/")`
- **THEN** method creates dataset directory if it doesn't exist
- **AND** writes parquet file with UUID-based filename (e.g., "part-a1b2c3d4.parquet")
- **AND** file contains all data from the table

#### Scenario: Write large table split into multiple files

- **WHEN** user calls `handler.write_parquet_dataset(table, path, max_rows_per_file=1000)`
- **AND** table has more than 1000 rows
- **THEN** method splits table into multiple files with ~1000 rows each
- **AND** each file has unique UUID-based filename
- **AND** reading the dataset returns all original data

#### Scenario: Write with custom basename template

- **WHEN** user calls `handler.write_parquet_dataset(table, path, basename_template="data_{}.parquet")`
- **THEN** method generates filenames using the template with UUID-based unique identifiers
- **AND** files are named like "data_a1b2c3d4.parquet", "data_e5f6g7h8.parquet", etc.
- **AND** each filename contains a unique UUID token ensuring no collisions

#### Scenario: Write empty table to dataset

- **WHEN** user calls `handler.write_parquet_dataset(empty_table, path)`
- **THEN** method creates at least one file with the schema
- **AND** file contains zero rows but preserves column structure
- **AND** filename follows UUID-based unique naming pattern

### Requirement: Unique Filename Generation

The system SHALL generate unique filenames using UUID-based identifiers that avoid collisions across multiple writes without requiring sequential numbering or timestamp-based ordering.

#### Scenario: UUID-based filename uniqueness

- **WHEN** method generates filenames using default UUID strategy
- **THEN** filenames are globally unique using short UUID tokens
- **AND** format is "part-{uuid}.parquet" where uuid is an 8-character UUID fragment
- **AND** no collision management or sequential state is required

#### Scenario: Custom template with UUID insertion

- **WHEN** method uses basename_template with {} placeholder
- **THEN** {} is replaced with a unique UUID token
- **AND** template structure is preserved (e.g., "data_{}.parquet" → "data_a1b2c3d4.parquet")
- **AND** each call generates different UUID tokens

#### Scenario: Filename collision prevention

- **WHEN** multiple concurrent writes to same dataset occur
- **THEN** UUID-based generation ensures no collisions
- **AND** each write produces unique filenames without coordination
- **AND** no sequential state or timestamp ordering is required