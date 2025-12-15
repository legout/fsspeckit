# utils-optional-separation Specification

## Purpose
TBD - created by archiving change refactor-optional-dependencies. Update Purpose after archive.
## Requirements
### Requirement: Optional dependency utility modules
Utility modules requiring optional dependencies SHALL be separated into dedicated modules that can be imported independently without affecting core functionality.

#### Scenario: Core utilities import without optional dependencies
- **WHEN** a user imports fsspeckit.common with only base dependencies
- **AND** optional utilities are in separate modules
- **THEN** core utilities import successfully
- **AND** no ImportError occurs for missing optional dependencies

#### Scenario: Optional utilities are available when needed
- **WHEN** a user has optional dependencies installed
- **AND** imports specialized utility modules
- **THEN** those modules provide enhanced functionality
- **AND** integrate seamlessly with core utilities

### MODIFIED Requirement: Common module structure
The common module structure SHALL separate base utilities from optional dependency utilities to enable selective importing.

#### Scenario: Base utilities available without optional deps
- **WHEN** importing from fsspeckit.common
- **AND** only base dependencies are installed
- **THEN** base utilities (misc, datetime without polars/pyarrow) are available
- **AND** optional utilities are not imported

#### Scenario: Optional utilities import on demand
- **WHEN** optional dependencies are available
- **AND** user imports optional utility modules
- **THEN** enhanced functionality is accessible
- **AND** integrates with base utilities

### REMOVED Requirement: Unconditional optional dependency imports
Utility modules SHALL NOT unconditionally import optional dependencies at module level.

#### Scenario: Module imports without triggering optional deps
- **WHEN** importing fsspeckit.common.types
- **AND** polars/pandas/pyarrow are not installed
- **THEN** import succeeds without ImportError
- **AND** functions raise errors only when called

#### Scenario: Optional imports are lazy
- **WHEN** utility module is imported
- **AND** contains functions using optional dependencies
- **THEN** optional dependencies are not imported until function call
- **AND** availability is checked before use

### Requirement: Backward compatibility for imports
Existing import paths SHALL remain functional while supporting the new modular structure.

#### Scenario: Existing imports continue working
- **WHEN** code imports from fsspeckit.common.types
- **AND** the module has been refactored
- **THEN** existing import statements work
- **AND** behavior is unchanged when dependencies are available

#### Scenario: Gradual migration path
- **WHEN** users want to use new modular imports
- **AND** existing code uses old import paths
- **THEN** both import patterns work
- **AND** users can migrate incrementally

### Requirement: Clear module documentation
Each utility module SHALL clearly document its dependency requirements in docstrings.

#### Scenario: Module documentation shows requirements
- **WHEN** viewing module docstring
- **AND** module requires optional dependencies
- **THEN** required packages are listed
- **AND** installation instructions are provided

#### Scenario: Function documentation indicates optional deps
- **WHEN** reading function docstrings
- **AND** function requires optional dependencies
- **THEN** dependency requirements are documented
- **AND** error conditions are explained

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

### Requirement: Universal I/O helpers respect optional separation

Universal I/O helpers SHALL honour the separation between base utilities and optional dependency utilities, so that
base imports remain safe even when optional packages are missing.

#### Scenario: Importing universal I/O helpers does not require optional dependencies
- **WHEN** a user imports `fsspeckit.core.ext_io` or constructs a filesystem that exposes `read_files` / `write_files`
- **AND** optional dependencies such as Polars, Pandas, or PyArrow are not installed
- **THEN** the import SHALL succeed without raising `ImportError`
- **AND** optional dependencies SHALL only be required when the relevant format-specific path is invoked.

