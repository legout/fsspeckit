## ADDED Requirements

### Requirement: Parquet helpers return consistent PyArrow tables

The system SHALL ensure that helpers which read Parquet data and expose a PyArrow interface:

- Always return `pyarrow.Table` objects when declared to do so.
- Use native PyArrow types for any additional columns that they add (for example, `file_path`).

#### Scenario: Parquet `include_file_path` uses a PyArrow string array
- **WHEN** a caller reads Parquet data via a helper that returns a `pyarrow.Table`
- **AND** passes `include_file_path=True`
- **THEN** the resulting table SHALL include a `file_path` column
- **AND** that column SHALL be a PyArrow string array whose length matches the number of rows in the table.

#### Scenario: Threading parameter does not change semantics
- **WHEN** a caller reads JSON or CSV data via helpers that support a `use_threads` parameter
- **AND** calls the helper with the same paths and arguments but different values for `use_threads`
- **THEN** the resulting data SHALL be semantically equivalent (same records)
- **AND** the `use_threads` parameter SHALL only affect the execution strategy (parallel vs sequential), not the content.
