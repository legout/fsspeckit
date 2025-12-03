## ADDED Requirements

### Requirement: Error handling follows consistent, non-silencing patterns

Core code paths SHALL avoid silently swallowing unexpected exceptions and SHALL use consistent, explicit error handling patterns.

#### Scenario: No bare `except Exception` in core flows
- **WHEN** core modules (`core`, `datasets`, `storage_options`) are inspected
- **THEN** they SHALL NOT contain bare `except Exception:` blocks that simply `pass` or suppress errors without logging
- **AND** any broad exception handlers SHALL either:
  - Narrow the types caught to expected exceptions, or
  - Log the error using the project’s logging facilities and re-raise.

#### Scenario: Resource cleanup is granular and observable
- **WHEN** cleanup logic (e.g. unregistering DuckDB tables, closing resources) is executed
- **THEN** each resource SHALL be handled individually so that failures are visible in logs
- **AND** a failure to clean up one resource SHALL NOT silently mask issues or prevent attempts to clean up others.

### Requirement: Logging is used instead of `print` for error reporting

Core modules SHALL rely on the logging utilities provided by `fsspeckit.common.logging` for error reporting and diagnostics.

#### Scenario: Errors are logged, not printed
- **WHEN** a non-trivial error is handled in core code
- **THEN** the error details SHALL be recorded using a logger obtained via `get_logger`
- **AND** `print(...)` SHALL NOT be used to signal errors or warnings in these paths.

