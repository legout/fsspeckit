## Why

DuckDB operations in `fsspeckit` currently use broad `except Exception:` blocks that mask specific errors and make debugging difficult. Common issues include:

- Generic exception catching hides DuckDB-specific errors (InvalidInputException, OperationalException, etc.)
- Resource cleanup failures are silently ignored
- Error context is lost, making troubleshooting harder
- Some exceptions are re-raised as generic `Exception` instead of preserving the original type

## What Changes

**Scope**: `src/fsspeckit/datasets/duckdb.py` and `src/fsspeckit/datasets/_duckdb_helpers.py`

- Replace generic `except Exception:` with specific DuckDB exception types:
  - `duckdb.InvalidInputException` for bad SQL/parameters
  - `duckdb.OperationalException` for database operation failures  
  - `duckdb.CatalogException` for table/view issues
  - `duckdb.IOException` for file I/O problems
  - `duckdb.OutOfMemoryException` for memory issues
- Preserve original exception types when re-raising
- Add context-specific error messages that include operation details
- Ensure cleanup helpers log failures but continue cleanup process
- Import DuckDB exception types properly with fallback handling

## Impact

- **Behaviour**: More precise error messages, better debugging experience
- **Backwards Compatibility**: Exception types will be more specific (breaking change for callers catching generic `Exception`)
- **Performance**: Minimal impact, slightly better exception handling performance
- **Maintainability**: Easier to identify and fix DuckDB-specific issues

## Code affected

- `src/fsspeckit/datasets/duckdb.py` (~20 exception blocks)
- `src/fsspeckit/datasets/_duckdb_helpers.py` (~2 exception blocks)

## Dependencies

- Requires DuckDB to be available for exception type imports
- Needs proper import fallback for when DuckDB is not installed