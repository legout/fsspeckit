## ADDED Requirements

### Requirement: PyArrow helpers use specific exception types

PyArrow dataset helpers and utilities SHALL surface specific PyArrow exception
types for expected failure modes and SHALL preserve the original exception type
and message when re-raising.

#### Scenario: Invalid schema uses ArrowInvalid
- **WHEN** a merge, compaction, or optimization helper encounters an invalid
  schema or data condition (e.g. incompatible column types or missing fields)
- **THEN** it SHALL raise or propagate a `pyarrow.ArrowInvalid` (or a more
  specific PyArrow error) instead of a generic `Exception`
- **AND** the error message SHALL include operation context (helper name, path,
  and a short description of the schema issue).

#### Scenario: I/O failures use ArrowIOError
- **WHEN** a helper fails to read or write parquet data due to file system or
  network issues
- **THEN** it SHALL raise or propagate a `pyarrow.ArrowIOError`
- **AND** the message SHALL include the affected path or dataset identifier
  and a brief description of the I/O failure.

#### Scenario: Type and indexing errors use typed exceptions
- **WHEN** a helper performs type conversions or positional access that fails
- **THEN** it SHALL raise `pyarrow.ArrowTypeError`, `pyarrow.ArrowKeyError`,
  or `pyarrow.ArrowIndexError` as appropriate
- **AND** callers MAY rely on these specific types to handle failures in
  a targeted way.

### Requirement: PyArrow error handling is logged and non-silencing

PyArrow helpers SHALL avoid silently swallowing unexpected exceptions and SHALL
log error details using the project logging utilities before propagating the
error.

#### Scenario: Unexpected errors are logged and re-raised
- **WHEN** an unexpected exception is raised inside a PyArrow helper
- **THEN** the helper SHALL log the error (including operation name and
  relevant path or dataset identifiers) using a module-level logger
- **AND** it SHALL re-raise the exception (or a more specific wrapper) rather
  than returning partial results or silently continuing.

#### Scenario: Cleanup helpers log individual failures
- **WHEN** a cleanup helper is used to release PyArrow-related resources
  (datasets, scanners, temporary files)
- **THEN** it SHALL handle each resource individually so that a failure to
  clean up one resource does not prevent attempts on others
- **AND** each cleanup failure SHALL be logged with context instead of being
  ignored.
