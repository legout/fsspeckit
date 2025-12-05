## ADDED Requirements

### Requirement: Storage options use specific exception types

Storage option classes under `fsspeckit.storage_options` SHALL use specific
exception types for configuration and credential problems instead of generic
exceptions.

#### Scenario: Invalid configuration raises ValueError
- **WHEN** a storage options class detects invalid or inconsistent arguments
  (e.g. unsupported protocol, conflicting parameters)
- **THEN** it SHALL raise `ValueError` with a clear explanation of the
  configuration problem.

#### Scenario: Missing credentials raise informative errors
- **WHEN** a helper attempts to build a filesystem or store from incomplete
  credentials or environment variables
- **THEN** it SHALL raise a specific exception (e.g. `ValueError` or
  `FileNotFoundError` for missing config files)
- **AND** the message SHALL describe what is missing and how to fix it.

### Requirement: Storage options avoid silencing configuration errors

Storage option helpers SHALL NOT silently ignore configuration or credential
errors and SHALL avoid broad `except Exception:` blocks that swallow failures.

#### Scenario: Catch-all handlers log and re-raise
- **WHEN** a storage options helper needs a catch-all exception handler
- **THEN** it SHALL log the error (including which configuration path failed)
  using the project logger
- **AND** it SHALL re-raise the exception instead of returning a partially
  configured object.

