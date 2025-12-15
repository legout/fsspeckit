# core-ext Specification

## Purpose
TBD - created by archiving change fix-core-io-error-handling. Update Purpose after archive.
## Requirements
### Requirement: Core I/O helpers use specific exception types

Core I/O helpers in `fsspeckit.core.ext` SHALL use specific exception types for common failure modes and SHALL avoid
collapsing all errors into generic `RuntimeError`, while also respecting optional dependency rules.

#### Scenario: Optional dependency failures surface as ImportError with guidance
- **WHEN** a caller invokes `read_files`, `write_file`, or `write_files` for a format that requires an optional dependency
- **AND** the corresponding package (e.g. Polars, Pandas, or PyArrow) is not installed
- **THEN** the helper SHALL raise `ImportError` from the shared optional-dependency helpers with a clear installation hint
- **AND** SHALL NOT raise `NameError` or other unexpected exceptions due to missing imports.

#### Scenario: Universal I/O helpers avoid NameError and undefined variables
- **WHEN** `fsspeckit.core.ext_io` is imported and `read_files`, `write_file`, or `write_files` are called
- **THEN** references to Polars, Pandas, and PyArrow in those helpers are resolved via `fsspeckit.common.optional`
- **AND** no helper SHALL reference a runtime variable that was only imported under `TYPE_CHECKING`.

### Requirement: Core I/O helpers log context and do not use print

Core I/O helpers SHALL use the project logging utilities for warnings and
errors and SHALL log enough context to debug failures without relying on
`print()` statements.

#### Scenario: Logged errors with file context
- **WHEN** a helper logs a recoverable or fatal error
- **THEN** the log entry SHALL include the operation name, the file path or
  URI, and the underlying exception message
- **AND** logs SHALL be emitted through a module-level logger obtained via the
  centralized logging utilities.

#### Scenario: Unexpected errors are logged and re-raised
- **WHEN** an unexpected exception occurs inside a core I/O helper
- **THEN** the helper SHALL log the error with context and re-raise it
  (possibly wrapped) instead of swallowing it or returning partial results.

### Requirement: Document PyArrow merge-aware write parameters

`write_pyarrow_dataset` API documentation SHALL include new merge strategy parameters and their behavior.

#### Scenario: Strategy parameter documentation
- **WHEN** a user views `write_pyarrow_dataset` API documentation
- **THEN** the documentation SHALL include the `strategy` parameter with all valid options ('insert', 'upsert', 'update', 'full_merge', 'deduplicate')
- **AND** SHALL describe when each strategy should be used
- **AND** SHALL document any requirements (like key_columns for relational strategies)

#### Scenario: Key columns parameter documentation
- **WHEN** a user views `write_pyarrow_dataset` API documentation
- **THEN** the documentation SHALL include the `key_columns` parameter
- **AND** SHALL explain its purpose for merge operations
- **AND** SHALL document which strategies require it
- **AND** SHALL show examples of single and composite key usage

### Requirement: Document convenience helper functions

All new convenience helper functions SHALL be documented with clear usage examples.

#### Scenario: Helper function documentation
- **WHEN** a user searches for merge functionality
- **THEN** the documentation SHALL include `insert_dataset`, `upsert_dataset`, `update_dataset`, and `deduplicate_dataset`
- **AND** each function SHALL have clear parameter documentation
- **AND** each function SHALL include practical usage examples
- **AND** each function SHALL document key column requirements

### Requirement: GitLab filesystem SHALL implement proper resource cleanup

GitLabFileSystem SHALL provide proper cleanup of HTTP session resources to prevent resource leaks in long-running applications.

#### Scenario: Session cleanup on explicit close
- **WHEN** caller invokes `close()` method on GitLabFileSystem instance
- **THEN** filesystem SHALL cleanup underlying requests.Session
- **AND** subsequent API calls SHALL raise appropriate errors indicating closed state

#### Scenario: Session cleanup on garbage collection
- **WHEN** GitLabFileSystem instance is garbage collected without explicit close
- **THEN** filesystem SHALL cleanup underlying requests.Session in `__del__` method
- **AND** cleanup SHALL be logged at debug level for monitoring

#### Scenario: Multiple instances maintain separate sessions
- **WHEN** multiple GitLabFileSystem instances are created
- **THEN** each instance SHALL maintain its own independent session
- **AND** cleanup of one instance SHALL NOT affect others

### Requirement: GitLab filesystem SHALL implement pagination limits

GitLabFileSystem SHALL implement maximum page limits to prevent infinite loops when processing malformed API responses.

#### Scenario: Pagination limit enforcement
- **WHEN** `ls()` method encounters more than `max_pages` (default 1000) pages
- **THEN** filesystem SHALL stop pagination and return collected results
- **AND** SHALL log warning about pagination limit reached
- **AND** SHALL include metadata indicating incomplete results

#### Scenario: Configurable pagination limits
- **WHEN** caller specifies `max_pages` parameter during filesystem creation
- **THEN** filesystem SHALL use provided limit instead of default
- **AND** SHALL validate that limit is positive integer
- **AND** SHALL raise ValueError for invalid limits

#### Scenario: Malformed pagination header handling
- **WHEN** GitLab API returns non-numeric or invalid `X-Next-Page` header
- **THEN** filesystem SHALL log warning about malformed header
- **AND** SHALL stop pagination gracefully
- **AND** SHALL return results collected up to that point

### Requirement: GitLab filesystem SHALL validate input parameters

GitLabFileSystem SHALL validate all configuration parameters to prevent runtime errors and provide clear error messages.

#### Scenario: Timeout parameter validation
- **WHEN** caller provides timeout parameter during filesystem creation
- **THEN** filesystem SHALL validate timeout is positive number
- **AND** SHALL enforce maximum timeout (3600 seconds)
- **AND** SHALL raise ValueError with descriptive message for invalid values

#### Scenario: Maximum pages parameter validation
- **WHEN** caller provides max_pages parameter during filesystem creation
- **THEN** filesystem SHALL validate max_pages is positive integer
- **AND** SHALL enforce reasonable maximum (10000 pages)
- **AND** SHALL raise ValueError with descriptive message for invalid values

#### Scenario: Invalid configuration error messages
- **WHEN** filesystem detects invalid configuration parameters
- **THEN** error message SHALL clearly indicate which parameter is invalid
- **AND** SHALL provide guidance on valid range or format
- **AND** SHALL suggest corrective action when possible

### Requirement: PyArrow dataset writes support merge strategies

`write_pyarrow_dataset` SHALL accept optional merge strategy arguments and apply them when provided.

#### Scenario: Strategy-aware write
- **WHEN** a caller passes `strategy` (one of `insert`, `upsert`, `update`, `full_merge`, `deduplicate`) and `key_columns` (when required)
- **THEN** `write_pyarrow_dataset` SHALL apply the corresponding merge semantics instead of a plain append/overwrite
- **AND** behaviour without `strategy` remains unchanged.

#### Scenario: Convenience helpers
- **WHEN** a caller invokes filesystem helpers `insert_dataset`, `upsert_dataset`, `update_dataset`, or `deduplicate_dataset`
- **THEN** these helpers SHALL delegate to `write_pyarrow_dataset` with the appropriate `strategy`
- **AND** SHALL validate that required `key_columns` are provided for key-based strategies.

