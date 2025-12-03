## ADDED Requirements

### Requirement: DuckDB helpers use specific exception types

DuckDB-based dataset helpers SHALL surface specific DuckDB exception types for
common failure scenarios and SHALL preserve the original DuckDB error type and
message when propagating failures.

#### Scenario: Invalid SQL uses InvalidInputException
- **WHEN** a helper executes DuckDB SQL with invalid syntax or references
  missing columns or tables
- **THEN** it SHALL raise or propagate `duckdb.InvalidInputException`
  (or a more specific DuckDB exception) rather than a generic `Exception`
- **AND** the error message SHALL include the operation name and a summary of
  the SQL problem.

#### Scenario: Operational failures use OperationalException
- **WHEN** a helper encounters database operation failures (e.g. constraint
  violations, transaction errors)
- **THEN** it SHALL raise or propagate `duckdb.OperationalException`
- **AND** the original DuckDB message SHALL be preserved in the raised error.

#### Scenario: Catalog and I/O issues use dedicated types
- **WHEN** a helper fails because of catalog issues (missing table/view) or
  file I/O problems for parquet data
- **THEN** it SHALL raise or propagate `duckdb.CatalogException` or
  `duckdb.IOException` respectively
- **AND** the error message SHALL include the affected table name or path.

### Requirement: DuckDB cleanup helpers are granular and logged

DuckDB-related cleanup helpers (including table unregistration) SHALL handle
each resource individually and SHALL log failures with sufficient context while
still attempting to clean up remaining resources.

#### Scenario: Table unregistration logs failures but continues
- **WHEN** a cleanup helper unregisters multiple DuckDB tables or views
- **THEN** it SHALL attempt to unregister each table individually
- **AND** failures for one table SHALL be logged with table identifier and
  underlying exception
- **AND** failures SHALL NOT prevent attempts to clean up remaining tables.

#### Scenario: Catch-all handlers log and re-raise
- **WHEN** a DuckDB helper needs a catch-all exception handler
- **THEN** the handler SHALL log the unexpected exception with context
  (operation, table/path) using the project logger
- **AND** it SHALL re-raise the exception instead of silently swallowing it.

