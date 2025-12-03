## ADDED Requirements

### Requirement: Optional dependencies are declared consistently

The project SHALL declare optional dependency groups in a way that:

- Is valid TOML under the `[project.optional-dependencies]` table.
- Uses extras names that correspond to documented installation commands (e.g. `pip install fsspeckit[datasets]`).

#### Scenario: Installing extras for datasets and SQL
- **WHEN** a caller runs `pip install fsspeckit[datasets]` or `pip install fsspeckit[sql]`
- **THEN** the declared extras SHALL resolve cleanly
- **AND** the resulting environment SHALL contain the packages required for the documented dataset and SQL features.

### Requirement: Version information is safely initialised

The project SHALL expose a `__version__` attribute on the top-level package that is robust to common development and deployment workflows.

#### Scenario: Version resolution in an installed environment
- **WHEN** the package is installed in a standard environment
- **AND** a caller imports `fsspeckit` and accesses `fsspeckit.__version__`
- **THEN** the version SHALL reflect the installed distribution version.

#### Scenario: Version resolution in a source-only development environment
- **WHEN** a developer imports `fsspeckit` from a working copy that has not been installed
- **THEN** the import SHALL NOT fail solely because `importlib.metadata` cannot find an installed distribution
- **AND** `fsspeckit.__version__` SHALL be set to a safe default string that clearly indicates a non-installed/development state.
