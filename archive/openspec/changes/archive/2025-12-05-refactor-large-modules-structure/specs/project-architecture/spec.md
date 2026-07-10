## MODIFIED Requirements

### Requirement: Large modules are decomposed into focused submodules

The system SHALL avoid monolithic modules with many unrelated responsibilities and instead organise functionality into focused submodules under each domain package.

#### Scenario: IO helpers split by format and responsibility
- **WHEN** inspecting the `fsspeckit.core` package
- **THEN** JSON, CSV, and Parquet helpers SHALL reside in clearly named submodules (or sections) according to their format
- **AND** the wiring layer that attaches these helpers to `AbstractFileSystem` SHALL be clearly separated from the helper implementations.

**Implementation:**

The `fsspeckit.core.ext` module has been decomposed into:

- `fsspeckit.core.ext_json.py` - JSON/JSONL file I/O helpers
- `fsspeckit.core.ext_csv.py` - CSV file I/O helpers
- `fsspeckit.core.ext_parquet.py` - Parquet file I/O helpers
- `fsspeckit.core.ext_dataset.py` - PyArrow dataset creation helpers
- `fsspeckit.core.ext_io.py` - Universal I/O interfaces
- `fsspeckit.core.ext_register.py` - Registration layer for AbstractFileSystem

The original `fsspeckit.core.ext.py` is now a thin re-export module that maintains backward compatibility.

#### Scenario: Dataset modules separate schema logic from dataset operations
- **WHEN** inspecting `fsspeckit.datasets.pyarrow` and `fsspeckit.datasets.duckdb` implementations
- **THEN** schema/type inference and unification logic SHALL be factored into dedicated helpers
- **AND** dataset merge/maintenance operations SHALL delegate to the shared core and reuse those helpers instead of embedding all logic in the top-level dataset modules.

**Implementation:**

The `fsspeckit.datasets.pyarrow` module has been decomposed into:

- `fsspeckit.datasets.pyarrow_schema.py` - Schema unification, type inference, and optimization
- `fsspeckit.datasets.pyarrow_dataset.py` - Dataset merge and maintenance operations

The original `fsspeckit.datasets.pyarrow` is now a thin re-export module.

The `fsspeckit.datasets.duckdb` module has been decomposed into:

- `fsspeckit.datasets.duckdb_connection.py` - Connection management and filesystem registration
- `fsspeckit.datasets.duckdb_dataset.py` - Dataset I/O and maintenance operations

The original `fsspeckit.datasets.duckdb` is now a thin re-export module with a backward-compatibility wrapper.

### Requirement: Public entrypoints are stable, internal structure is modular

The system SHALL preserve stable public import paths at the package level while allowing internal module structure to evolve towards smaller, focused units.

#### Scenario: Existing imports remain valid after refactor
- **WHEN** user code imports public helpers from `fsspeckit`, `fsspeckit.core`, or `fsspeckit.datasets`
- **THEN** those imports SHALL continue to work after the refactor
- **AND** any internal restructuring SHALL be reflected through re-exports or thin entrypoint modules so that external code does not need to change.

**Implementation:**

All public APIs are maintained through:
- `fsspeckit.core.__init__.py` - Re-exports filesystem functionality
- `fsspeckit.datasets.__init__.py` - Re-exports dataset functionality
- `fsspeckit.__init__.py` - Re-exports public entrypoints

Backward compatibility is preserved through thin re-export modules:
- `fsspeckit.core.ext.py`
- `fsspeckit.datasets.pyarrow.py`
- `fsspeckit.datasets.duckdb.py`

## NEW Module Structure

### fsspeckit.core

The core module has been refactored into focused submodules:

- `base.py` - Base functionality exports
- `filesystem.py` - Factory functions for filesystem instances
- `filesystem_paths.py` - Path manipulation and protocol detection utilities
- `filesystem_cache.py` - Cache mapper and monitored cache filesystem
- `ext.py` - Re-export module (backward compatibility)
- `ext_json.py` - JSON I/O helpers
- `ext_csv.py` - CSV I/O helpers
- `ext_parquet.py` - Parquet I/O helpers
- `ext_dataset.py` - Dataset creation helpers
- `ext_io.py` - Universal I/O interfaces
- `ext_register.py` - Registration layer for AbstractFileSystem
- `merge.py` - Merge strategies and statistics
- `maintenance.py` - Dataset maintenance operations

### fsspeckit.datasets

The datasets module has been refactored into focused submodules:

- `pyarrow.py` - Re-export module (backward compatibility)
- `pyarrow_schema.py` - Schema utilities
- `pyarrow_dataset.py` - PyArrow dataset operations
- `duckdb.py` - Re-export module (backward compatibility)
- `duckdb_connection.py` - DuckDB connection management
- `duckdb_dataset.py` - DuckDB dataset I/O operations

## Benefits

1. **Improved Maintainability**: Each module has a single, clear responsibility
2. **Better Code Organization**: Related functionality is grouped together
3. **Easier Testing**: Smaller modules are easier to test in isolation
4. **Enhanced Developer Experience**: Clear module boundaries make the codebase easier to navigate
5. **Backward Compatibility**: Public API remains stable through re-export modules

