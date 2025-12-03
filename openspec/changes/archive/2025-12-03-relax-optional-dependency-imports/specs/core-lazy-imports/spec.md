## ADDED Requirements

### Requirement: Core helpers treat joblib as optional

The system SHALL treat joblib as an optional dependency that is only required for parallel execution paths, not for importing core modules.

#### Scenario: Import core modules without joblib
- **WHEN** a user imports core utilities (e.g., `fsspeckit.common.misc`, `fsspeckit.core.ext`) in an environment without joblib installed
- **THEN** the import SHALL succeed
- **AND** functions that require joblib for parallel execution SHALL raise a clear `ImportError` only when parallel execution is requested.

#### Scenario: `run_parallel` uses lazy joblib import
- **WHEN** a caller uses `run_parallel` to execute work in parallel
- **AND** joblib is installed
- **THEN** `run_parallel` SHALL import joblib lazily and execute tasks in parallel
- **AND** behaviour SHALL remain compatible with existing tests and documented semantics.

- **WHEN** joblib is not installed
- **AND** a caller requests parallel execution
- **THEN** `run_parallel` SHALL raise an `ImportError`
- **AND** the error message SHALL indicate how to install the appropriate extra.
