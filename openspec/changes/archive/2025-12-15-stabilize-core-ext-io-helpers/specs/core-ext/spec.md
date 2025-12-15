## MODIFIED Requirements

### Requirement: Core I/O helpers use specific exception types

Core I/O helpers in `fsspeckit.core.ext` SHALL use specific exception types for common failure modes and SHALL avoid
collapsing all errors into generic `RuntimeError`, while also respecting optional dependency rules.

#### Scenario: Optional dependency failures surface as ImportError with guidance
- **WHEN** a caller invokes `read_files`, `write_file`, or `write_files` for a format that requires an optional dependency
- **AND** the corresponding package (e.g. Polars, Pandas, or PyArrow) is not installed
- **THEN** the helper SHALL raise `ImportError` from the shared optional-dependency helpers with a clear installation hint
- **AND** SHALL NOT raise `NameError` or other unexpected exceptions due to missing imports.

#### Scenario: Universal I/O helpers avoid NameError and undefined variables
- **WHEN** `fsspeckit.core.ext_io` is imported and `read_files`, `write_file`, or `write_files` are called
- **THEN** references to Polars, Pandas, and PyArrow in those helpers are resolved via `fsspeckit.common.optional`
- **AND** no helper SHALL reference a runtime variable that was only imported under `TYPE_CHECKING`.

