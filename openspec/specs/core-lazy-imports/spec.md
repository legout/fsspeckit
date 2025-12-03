# core-lazy-imports Specification

## Purpose
TBD - created by archiving change refactor-optional-dependencies. Update Purpose after archive.
## Requirements
### Requirement: Optional dependency availability flags
Core modules SHALL define availability flags for optional dependencies using importlib.util.find_spec() to check if packages are installed without importing them.

#### Scenario: Core module imports without optional dependencies
- **WHEN** a user imports fsspeckit with only base dependencies installed
- **AND** the module uses optional dependencies like polars or pyarrow
- **THEN** the module imports successfully without ImportError
- **AND** availability flags (e.g., _POLARS_AVAILABLE) are set to False

#### Scenario: Availability flags detect installed packages
- **WHEN** optional dependencies are installed in the environment
- **AND** a core module is imported
- **THEN** availability flags are correctly set to True
- **AND** the corresponding packages can be imported when needed

### Requirement: Lazy import helper functions
Core modules SHALL provide helper functions that import optional dependencies on first use with clear error messages when dependencies are missing.

#### Scenario: Helper function imports available dependency
- **WHEN** a function calls _import_polars() and polars is installed
- **THEN** the function returns the polars module
- **AND** subsequent calls use cached import for performance

#### Scenario: Helper function provides helpful error for missing dependency
- **WHEN** a function calls _import_polars() and polars is not installed
- **THEN** the function raises ImportError with installation instructions
- **AND** the error message specifies the correct extra to install (e.g., fsspeckit[datasets])

### Requirement: Function-level conditional imports
Functions requiring optional dependencies SHALL import those dependencies within the function body rather than at module level.

#### Scenario: Function imports dependency only when called
- **WHEN** a module containing optional dependency functions is imported
- **AND** those functions are not called
- **THEN** no optional dependencies are imported
- **AND** import time is minimized

#### Scenario: Function validates dependency before use
- **WHEN** a function requiring polars is called
- **THEN** it checks polars availability before attempting to use it
- **AND** raises a clear ImportError if polars is not available
- **AND** proceeds normally if polars is available

### MODIFIED Requirement: Type annotations with TYPE_CHECKING
Type annotations for optional dependencies SHALL use TYPE_CHECKING imports to avoid runtime imports while maintaining type safety.

#### Scenario: Type checking works without runtime imports
- **WHEN** mypy analyzes code with optional dependency types
- **AND** TYPE_CHECKING is used for those imports
- **THEN** type checking works correctly
- **AND** no runtime imports occur for those dependencies

#### Scenario: Runtime imports are avoided
- **WHEN** the module is imported at runtime
- **AND** TYPE_CHECKING imports are present
- **THEN** optional dependencies are not imported
- **AND** only availability checks are performed

### Requirement: Consistent error messaging
All ImportError messages for missing optional dependencies SHALL follow a consistent format specifying the required package and installation command.

#### Scenario: Error message guides installation
- **WHEN** a user tries to use a function requiring polars without it installed
- **THEN** the ImportError mentions polars is required
- **AND** provides the exact pip install command
- **AND** suggests the appropriate extra (e.g., fsspeckit[datasets])

#### Scenario: Error messages are consistent across modules
- **WHEN** different modules raise ImportError for missing dependencies
- **THEN** all messages follow the same format
- **AND** provide equivalent installation guidance

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

