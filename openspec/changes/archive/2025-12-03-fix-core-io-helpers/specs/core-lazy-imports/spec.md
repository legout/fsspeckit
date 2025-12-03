## ADDED Requirements

### Requirement: Core filesystem extensions honour lazy imports

The system SHALL use the shared optional-dependency layer for all core filesystem extensions that rely on optional libraries (e.g., `pyarrow`, `orjson`), ensuring:

- No hard failure at module-import time when optional dependencies are missing.
- Clear, guided error messages when optional functionality is invoked without the required extra installed.

#### Scenario: Parquet helpers use lazy PyArrow imports
- **WHEN** a caller invokes a Parquet helper attached to `AbstractFileSystem`
- **AND** `pyarrow` is installed
- **THEN** the helper SHALL import `pyarrow` via the shared optional-dependency mechanism
- **AND** SHALL successfully return a `pyarrow.Table` object.

#### Scenario: Parquet helpers error cleanly without PyArrow
- **WHEN** a caller invokes a Parquet helper attached to `AbstractFileSystem`
- **AND** `pyarrow` is not installed
- **THEN** the helper SHALL raise an `ImportError`
- **AND** the error message SHALL include guidance about the required extra (for example, `pip install fsspeckit[datasets]`).

#### Scenario: JSON writer uses lazy `orjson` imports
- **WHEN** a caller invokes `write_json` on a filesystem
- **AND** `orjson` is installed
- **THEN** the helper SHALL import `orjson` via the shared optional-dependency mechanism
- **AND** SHALL successfully serialise the data.

#### Scenario: JSON writer errors cleanly without `orjson`
- **WHEN** a caller invokes `write_json` on a filesystem
- **AND** `orjson` is not installed
- **THEN** the helper SHALL raise an `ImportError`
- **AND** the error message SHALL include guidance about the required extra.
