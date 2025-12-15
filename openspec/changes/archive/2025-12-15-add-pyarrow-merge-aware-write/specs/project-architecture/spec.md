## ADDED Requirements

### Requirement: PyArrow dataset writes expose merge-aware UX

The PyArrow dataset write API SHALL offer merge-aware entry points so that common strategies do not require separate staging steps.

#### Scenario: One-step merge via write helper
- **WHEN** a caller writes in-memory data to a parquet dataset using the PyArrow path
- **AND** specifies a merge strategy and key columns where applicable
- **THEN** the library SHALL perform the merge in a single call
- **AND** SHALL expose shortcut methods whose names mirror the supported strategies for parity with DuckDB.
