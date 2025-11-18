## MODIFIED Requirements

### Requirement: Error Handling

The system SHALL provide clear error messages for common failure scenarios with specific exception types that allow callers to distinguish between different error conditions.

#### Scenario: Invalid path error

- **WHEN** user provides non-existent path to read_parquet
- **THEN** method raises FileNotFoundError with clear message indicating file or directory not found
- **AND** error message includes the problematic path

#### Scenario: Invalid storage options error

- **WHEN** user provides storage options with missing credentials for remote storage
- **THEN** method raises clear exception indicating authentication failure
- **AND** error preserves original authentication error details

#### Scenario: SQL execution error

- **WHEN** SQL query has syntax error or references invalid columns
- **THEN** execute_sql raises exception with DuckDB error message
- **AND** original DuckDB error type and message are preserved

### Requirement: Dataset Write Validation

The system SHALL validate inputs and provide clear error messages for invalid dataset write operations with specific exception types.

#### Scenario: Invalid mode error

- **WHEN** user provides invalid mode value (not "overwrite" or "append")
- **THEN** method raises ValueError with clear message listing valid modes
- **AND** error message includes the invalid value that was provided

#### Scenario: Invalid max_rows_per_file error

- **WHEN** user provides max_rows_per_file <= 0
- **THEN** method raises ValueError indicating minimum value must be > 0
- **AND** error message includes the invalid value that was provided

#### Scenario: Path is file not directory error

- **WHEN** user provides path to existing file (not directory) for dataset operations
- **THEN** method raises ValueError or NotADirectoryError with clear message indicating path must be directory
- **AND** error message includes the problematic path

#### Scenario: Remote storage write permission error

- **WHEN** user attempts to write to remote storage without write permissions
- **THEN** method raises exception with clear authentication/permission error message
- **AND** original permission error details are preserved

### Requirement: Maintenance Validation and Safety

The system SHALL validate inputs and support dry-run safety for all maintenance operations with consistent error handling.

#### Scenario: Invalid thresholds

- **WHEN** user provides `target_mb_per_file <= 0` or `target_rows_per_file <= 0`
- **THEN** method raises ValueError with clear message indicating minimum valid values
- **AND** error message includes the invalid threshold values

#### Scenario: Dry run returns plan only

- **WHEN** dry_run=True is passed
- **THEN** no files are written or deleted
- **AND** plan includes proposed output file structure

#### Scenario: Non-existent dataset path

- **WHEN** user calls maintenance on path that does not exist
- **THEN** method raises FileNotFoundError with clear message indicating dataset path not found
- **AND** error message includes the problematic path

#### Scenario: No parquet files found

- **WHEN** maintenance operation finds no parquet files matching criteria (including partition filters)
- **THEN** method raises FileNotFoundError with clear message indicating no parquet files found
- **AND** error message specifies the search criteria that yielded no results