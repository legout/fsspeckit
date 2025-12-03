## 1. Implementation

- [ ] 1.1 Review storage option classes for error handling and logging:
  - [ ] 1.1.1 `src/fsspeckit/storage_options/base.py`
  - [ ] 1.1.2 `src/fsspeckit/storage_options/cloud.py`
  - [ ] 1.1.3 `src/fsspeckit/storage_options/core.py`
  - [ ] 1.1.4 `src/fsspeckit/storage_options/git.py`
- [ ] 1.2 Ensure invalid configuration and environment-driven failures raise
  specific exceptions with clear messages.
- [ ] 1.3 Avoid adding broad `except Exception:` blocks; where unavoidable,
  log and re-raise instead of swallowing errors.

## 2. Testing

- [ ] 2.1 Add or extend tests that:
  - [ ] 2.1.1 Cover invalid configuration and missing environment variables.
  - [ ] 2.1.2 Assert that specific exception types and messages are emitted.

## 3. Validation

- [ ] 3.1 Run `openspec validate fix-storage-options-error-handling --strict`
  and fix any spec issues.

*** Add File: openspec/changes/fix-storage-options-error-handling/specs/storage-options/spec.md
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

