## MODIFIED Requirements

### Requirement: Dataset write/read parameters are validated

DuckDB-based dataset helpers SHALL validate user-provided paths and configuration values before constructing SQL or performing dataset operations.

#### Scenario: Invalid dataset path is rejected early
- **WHEN** a caller provides an obviously invalid or unsafe dataset path (e.g. containing control characters or attempting to escape a base directory)
- **THEN** the DuckDB helpers SHALL raise a `ValueError` before constructing or executing any SQL
- **AND** the error message SHALL clearly indicate that the path is invalid.

#### Scenario: Unsupported compression codec is rejected
- **WHEN** a caller passes a compression codec that is not in the supported/whitelisted set
- **THEN** the helper SHALL raise a `ValueError` before passing the codec into DuckDB
- **AND** the error message SHALL list the supported codecs.

