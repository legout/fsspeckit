# core-maintenance Specification

## Purpose
TBD - created by archiving change fix-maintenance-error-handling. Update Purpose after archive.
## Requirements
### Requirement: Maintenance helpers use specific exception types

Backend-neutral maintenance helpers in `fsspeckit.core.maintenance` SHALL use
specific exception types for expected failures and SHALL avoid catching and
re-raising generic `Exception` for all error scenarios.

#### Scenario: Invalid thresholds raise ValueError
- **WHEN** a maintenance helper receives invalid configuration (e.g.
  non-positive thresholds, unsupported options)
- **THEN** it SHALL raise `ValueError` describing the invalid parameter
- **AND** it SHALL NOT start any read/write operations in this case.

#### Scenario: Missing or inaccessible paths use FileNotFoundError or PermissionError
- **WHEN** a maintenance helper cannot access the target dataset path
- **THEN** it SHALL raise `FileNotFoundError` if the path does not exist
- **OR** `PermissionError` for permission problems
- **AND** the error message SHALL include the problematic path.

### Requirement: Maintenance helpers log per-path failures and continue where safe

Maintenance helpers SHALL log failures with context per dataset or path and
SHALL attempt to continue processing independent work items where it is safe
to do so.

#### Scenario: Partial failure logging
- **WHEN** a maintenance helper processes multiple paths or partitions and some
  operations fail
- **THEN** it SHALL log each failure with the specific path and error details
- **AND** it SHALL continue processing other paths when this does not risk
  data corruption.

