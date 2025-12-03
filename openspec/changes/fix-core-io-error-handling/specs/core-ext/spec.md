## ADDED Requirements

### Requirement: Core I/O helpers use specific exception types

Core I/O helpers in `fsspeckit.core.ext` SHALL use specific exception types
for common failure modes and SHALL avoid collapsing all errors into generic
`RuntimeError`.

#### Scenario: File access errors use typed exceptions
- **WHEN** a helper fails to open, read, or write a file
- **THEN** it SHALL raise `FileNotFoundError`, `PermissionError`, or `OSError`
  as appropriate instead of a generic `RuntimeError`
- **AND** the error message SHALL include the operation (read, write, metadata)
  and the affected path or URI.

#### Scenario: Parameter validation uses ValueError
- **WHEN** a helper detects invalid arguments (e.g. unsupported mode, missing
  path, or incompatible options)
- **THEN** it SHALL raise `ValueError` with a clear description of the problem
- **AND** callers MAY rely on this type for validation failures.

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
