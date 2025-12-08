## MODIFIED Requirements

### Requirement: Core filesystem extensions honour lazy imports

The system SHALL use the shared optional-dependency layer for all core filesystem extensions that rely on optional
libraries (e.g., `pyarrow`, `orjson`, Polars, Pandas), ensuring:

- No hard failure at module-import time when optional dependencies are missing.
- Clear, guided error messages when optional functionality is invoked without the required extra installed.

#### Scenario: Universal I/O helpers import dependencies lazily
- **WHEN** a caller invokes `read_files`, `write_file`, or `write_files` on a filesystem for a given format
- **AND** the required optional library for that format is installed
- **THEN** the helper SHALL obtain the library via `fsspeckit.common.optional` helpers
- **AND** SHALL not import those libraries at module-import time.

#### Scenario: Universal I/O helpers error cleanly without optional libraries
- **WHEN** a caller invokes `read_files`, `write_file`, or `write_files` for a format that requires an optional library
- **AND** that library is not installed
- **THEN** the helper SHALL raise an `ImportError` whose message includes the appropriate extras group (for example, `pip install fsspeckit[datasets]`)
- **AND** importing `fsspeckit.core.ext_io` SHALL still succeed without that library present.

