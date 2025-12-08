## MODIFIED Requirements

### Requirement: Core helpers treat joblib as optional

The system SHALL treat joblib as an optional dependency that is only required for parallel execution paths, not for
importing core modules.

#### Scenario: Serial execution does not require joblib
- **WHEN** a caller uses CSV or Parquet helpers with `use_threads=False`
- **THEN** those helpers SHALL execute without requiring joblib to be installed
- **AND** importing the relevant modules SHALL succeed even if joblib is absent.

#### Scenario: Parallel execution errors clearly when joblib is missing
- **WHEN** a caller requests parallel execution (for example, by setting `use_threads=True` on CSV/Parquet helpers or using `run_parallel`)
- **AND** joblib is not installed
- **THEN** the helper SHALL raise an `ImportError` that explains joblib is required for parallel execution
- **AND** the error message SHALL mention the appropriate extras group to install.

