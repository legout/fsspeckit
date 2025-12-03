# utils-dtype Specification

## Purpose
TBD - created by archiving change add-opt-dtype-sampling. Update Purpose after archive.
## Requirements
### Requirement: Sample-based dtype inference for `opt_dtype`
The `opt_dtype` helpers for Polars and PyArrow SHALL accept controls for sampling (`sample_size` and `sample_method`) so that the regex-based dtype inference inspects a bounded number of values instead of every row in very large tables.

#### Scenario: Sample-limited inference still detects integers
- **WHEN** a string column contains only integers but has millions of rows
- **AND** the caller sets `sample_size=128` and `sample_method="first"`
- **THEN** the optimizer only inspects the first 128 cleaned values for the regex match
- **AND** it still casts the entire column to integers because the full series is still valid

#### Scenario: Random sampling is supported
- **WHEN** the caller sets `sample_method="random"` and `sample_size=256`
- **AND** the cleaned column contains enough numeric-looking samples to satisfy the regex rule in that random subset
- **THEN** the inference flow proceeds exactly like the first-sample case and returns the inferred dtype

### Requirement: Safety guard for sampled inference
Inference based on the sample SHALL NOT silently corrupt data when the remainder of the column cannot be cast to the guessed type; instead it SHALL fall back to leaving the column as-is (or retain its original dtype) and not disrupt other columns.

#### Scenario: Sample hints integer but rest of column is invalid
- **WHEN** the sampled subset looks like an integer column but the full column contains non-digit text
- **THEN** the optimizer discovers that casting the eager data would fail or produce extra nulls
- **AND** it leaves the column in its original string form so the DataFrame remains valid

### Requirement: Sample-Driven Schema Inference for `opt_dtype`
`opt_dtype` (Polars and PyArrow) SHALL derive its optimized schema solely from the user-specified sample (`sample_size`/`sample_method`) so that large tables can be profiled without scanning every row.

#### Scenario: First-n sample defines the schema
- **WHEN** a string column is sampled with `sample_size=128` and `sample_method="first"` while the rest of the column contains non-numeric noise
- **THEN** the optimizer infers the numeric type from the sampled 128 values
- **AND** uses that inferred dtype to cast the entire column, leaving the final schema numeric even though the non-sampled tails might not match

### Requirement: Random sampling respects sample schema
`opt_dtype` SHALL honor `sample_method="random"` by randomly selecting `sample_size` entries for schema inference while keeping the optimized dtype consistent across the whole column.

#### Scenario: Random sample derives integer schema
- **WHEN** `sample_method="random"` and `sample_size=256` select only digits out of a mixed column
- **THEN** `opt_dtype` infers an integer schema just once from that sample
- **AND** applies the same schema during the final casting step instead of rescanning all remaining rows

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

### Requirement: Partition parsing has a single canonical implementation

Partition parsing utilities used across the project SHALL be implemented in a single canonical helper that is reused by backend-specific helpers.

#### Scenario: Backend helpers rely on the canonical partition parser
- **WHEN** a backend-specific helper (DuckDB or PyArrow) needs to extract partition information from a path
- **THEN** it SHALL call the canonical helper (e.g. `common.partitions.get_partitions_from_path`)
- **AND** SHALL not maintain its own separate parsing logic.

