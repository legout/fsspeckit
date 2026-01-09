## MODIFIED Requirements
### Requirement: Installation Docs Match Published Extras

The installation documentation SHALL describe optional extras and supported
Python versions that match `pyproject.toml`.

#### Scenario: Extras match pyproject configuration

- **WHEN** a user reads the "Optional Cloud Provider Support" section in
  `docs/installation.md`
- **THEN** the documented extras include `aws`, `gcp`, and `azure`
- **AND** example commands match the extras in `pyproject.toml` (for example
  `pip install "fsspeckit[aws,gcp,azure]"`).

#### Scenario: Python version requirement is accurate

- **WHEN** a user checks the prerequisites for installing `fsspeckit`
- **THEN** the docs state Python 3.11+ to match `requires-python` in
  `pyproject.toml`.

#### Scenario: DuckDB version requirement is accurate

- **WHEN** a user checks the DuckDB version prerequisites for `fsspeckit`
- **THEN** the documentation states `duckdb>=1.4.0` to match the actual
  dependency requirement in `pyproject.toml` for the `datasets` and `sql` extras
- **AND** the version reflects the need for native MERGE statement support
  introduced in DuckDB 1.4.0.
