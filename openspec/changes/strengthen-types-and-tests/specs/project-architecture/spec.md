## MODIFIED Requirements

### Requirement: Core modules are type-checked

Core modules SHALL be subject to static type checking as part of the development and CI process.

#### Scenario: Type-checking for key modules
- **WHEN** changes are made to key modules such as `core.filesystem`, `core.ext`, `datasets.pyarrow`, or `datasets.duckdb`
- **THEN** a type-checking step (e.g. mypy) SHALL be run
- **AND** new or changed functions SHALL have appropriate type annotations.

### Requirement: Refactors maintain or improve test coverage

Structural refactors (such as splitting large modules into submodules) SHALL maintain or improve effective test coverage.

#### Scenario: New submodules introduced
- **WHEN** a large module is refactored into smaller submodules
- **THEN** tests SHALL be added or updated so that behaviour is still covered at both integration and, where appropriate, unit level
- **AND** the refactor SHALL not reduce coverage for existing behaviours.

