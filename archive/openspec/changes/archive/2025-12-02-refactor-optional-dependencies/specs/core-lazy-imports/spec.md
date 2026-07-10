# core-lazy-imports Specification

## Purpose
Implement lazy loading patterns for core optional dependencies (polars, pandas, pyarrow, duckdb) to enable core fsspeckit functionality without requiring all optional dependencies.

## ADDED Requirements

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