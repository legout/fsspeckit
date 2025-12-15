## ADDED Requirements

### Requirement: PyArrow dataset handler parity with DuckDB

The project SHALL provide a class-based PyArrow dataset handler that aligns with the DuckDB handler for a consistent user experience.

#### Scenario: Class-based PyArrow dataset API
- **WHEN** a user prefers a class-based API for PyArrow parquet datasets
- **THEN** they SHALL instantiate `PyarrowDatasetIO` or `PyarrowDatasetHandler` to access read/write/merge/maintenance helpers
- **AND** method names SHALL mirror the DuckDB handler where supported.

#### Scenario: Lazy optional dependency imports for PyArrow handler
- **WHEN** importing the PyArrow handler without PyArrow installed
- **THEN** the import SHALL succeed
- **AND** an `ImportError` SHALL only be raised when PyArrow-dependent methods are invoked, with guidance on the required extras.
