## ADDED Requirements

### Requirement: Datetime column detection works across supported DataFrame types

The system SHALL support detecting timestamp/date columns in all officially supported DataFrame/table types (e.g., Polars, PyArrow, pandas) used in utilities.

#### Scenario: Timestamp detection in pandas DataFrame
- **WHEN** a caller passes a pandas DataFrame with one or more datetime-like columns to `get_timestamp_column`
- **THEN** the function SHALL return the names of the datetime/date columns
- **AND** SHALL not raise due to missing Polars/Arrow methods.

### Requirement: Timedelta string helpers are robust to unknown units

The system SHALL provide helpers for mapping between human-readable timedelta strings and backend-specific representations without failing on unknown units.

#### Scenario: Unknown timedelta unit with Polars target
- **WHEN** a caller passes a timedelta string with an unrecognised unit (e.g. `"1invalid"`) to `get_timedelta_str(..., to="polars")`
- **THEN** the function SHALL NOT raise a `KeyError`  
- **AND** SHALL return a reasonable `"value unit"` representation that preserves the original input semantics.

#### Scenario: Unknown timedelta unit with DuckDB target
- **WHEN** a caller passes a timedelta string with an unrecognised unit to `get_timedelta_str(..., to="duckdb")`
- **THEN** the function SHALL NOT raise due to missing mappings  
- **AND** SHALL return a `"value unit"` representation consistent with DuckDB’s expectations when possible, or a best-effort fallback otherwise.

### Requirement: Partition parsing provides a dict interface by default

The system SHALL provide a simple, dictionary-based interface for extracting partition key/value pairs from typical dataset paths.

#### Scenario: Hive-style path without explicit partitioning argument
- **WHEN** a caller passes a path such as `"data/year=2023/month=12/file.parquet"` to `get_partitions_from_path` with `partitioning` left as `None`
- **THEN** the function SHALL return a dictionary like `{"year": "2023", "month": "12"}`.

#### Scenario: Windows-style and relative paths are handled
- **WHEN** a caller passes Windows-style or relative paths with Hive-style segments to `get_partitions_from_path`
- **THEN** the function SHALL normalise the path representation
- **AND** SHALL correctly extract the partition key/value pairs into a dictionary.
