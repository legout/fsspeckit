## ADDED Requirements

### Requirement: Universal I/O helpers respect optional separation

Universal I/O helpers SHALL honour the separation between base utilities and optional dependency utilities, so that
base imports remain safe even when optional packages are missing.

#### Scenario: Importing universal I/O helpers does not require optional dependencies
- **WHEN** a user imports `fsspeckit.core.ext_io` or constructs a filesystem that exposes `read_files` / `write_files`
- **AND** optional dependencies such as Polars, Pandas, or PyArrow are not installed
- **THEN** the import SHALL succeed without raising `ImportError`
- **AND** optional dependencies SHALL only be required when the relevant format-specific path is invoked.

