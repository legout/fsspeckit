# common-modules Specification

## Purpose
TBD - created by archiving change fix-common-modules-error-handling. Update Purpose after archive.
## Requirements
### Requirement: Common utilities SHALL use specific exception types

Common utility modules under `fsspeckit.common` (including `schema` and `misc`) SHALL use specific exception types for validation and conversion failures instead of catching and re-raising generic `Exception`. Common utilities MUST NOT introduce new broad `except Exception:` blocks that silence errors.

#### Scenario: Schema validation errors use ValueError
- **WHEN** schema helpers detect invalid user schema, missing fields, or
  incompatible types
- **THEN** they SHALL raise `ValueError` with a clear explanation of the
  problem
- **AND** they SHALL NOT wrap these failures in generic `Exception` or
  silently ignore them.

#### Scenario: Type and attribute errors use typed exceptions
- **WHEN** utilities fail due to missing attributes, keys, or incompatible
  types
- **THEN** they SHALL raise `TypeError`, `KeyError`, or `AttributeError` as
  appropriate
- **AND** callers MAY rely on these specific types for error handling.

### Requirement: Common utilities log unexpected errors instead of swallowing them

Common utilities SHALL avoid silencing unexpected exceptions and SHALL log
them with enough context before propagating the error.

#### Scenario: Callback failures are logged and re-raised
- **WHEN** a utility invokes user-provided callbacks or functions that raise
  unexpected exceptions
- **THEN** the utility SHALL log the exception (including which callback
  failed and relevant arguments) using the project logger
- **AND** it SHALL re-raise the exception instead of swallowing it.

