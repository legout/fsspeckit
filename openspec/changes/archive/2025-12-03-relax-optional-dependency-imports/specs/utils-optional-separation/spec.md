## ADDED Requirements

### Requirement: Optional dependency utilities are fully lazy

Utility modules requiring optional dependencies SHALL avoid importing those dependencies at module import time and instead rely on centralised lazy-import helpers.

#### Scenario: Importing common utilities without optional deps
- **WHEN** a user imports `fsspeckit.common` or other base utility modules
- **AND** Polars, PyArrow, DuckDB, sqlglot, or other optional packages are not installed
- **THEN** the import SHALL succeed without raising `ImportError`
- **AND** functions that depend on these packages SHALL raise targeted errors only when called.

#### Scenario: Optional utilities use shared import helpers
- **WHEN** a function that depends on an optional package is invoked
- **THEN** it SHALL obtain that package via the shared helpers in `fsspeckit.common.optional`
- **AND** if the package is not available, the resulting error message SHALL mention the corresponding extras group.
