## 1. Implementation

- [ ] 1.1 Audit DuckDB modules for exception handling patterns:
  - [ ] 1.1.1 Scan `src/fsspeckit/datasets/duckdb.py` for `except Exception:` blocks (~20 instances)
  - [ ] 1.1.2 Scan `src/fsspeckit/datasets/_duckdb_helpers.py` for exception blocks
  - [ ] 1.1.3 Document current exception types and error handling patterns

- [ ] 1.2 Implement DuckDB-specific exception handling:
  - [ ] 1.2.1 Add proper DuckDB exception imports with fallback handling
  - [ ] 1.2.2 Replace generic `except Exception:` with specific DuckDB exception types:
    - `duckdb.InvalidInputException` for bad SQL/parameters
    - `duckdb.OperationalException` for database operation failures  
    - `duckdb.CatalogException` for table/view issues
    - `duckdb.IOException` for file I/O problems
    - `duckdb.OutOfMemoryException` for memory issues
  - [ ] 1.2.3 Preserve original exception types when re-raising
  - [ ] 1.2.4 Add context-specific error messages with operation details

- [ ] 1.3 Enhance cleanup helpers:
  - [ ] 1.3.1 Improve `_unregister_duckdb_table_safely` with specific exception handling
  - [ ] 1.3.2 Ensure cleanup failures are logged but don't interrupt cleanup process
  - [ ] 1.3.3 Add proper logging using `fsspeckit.common.logging.get_logger`

## 2. Testing

- [ ] 2.1 Add unit tests for DuckDB exception handling:
  - [ ] 2.1.1 Test specific DuckDB exception types are caught correctly
  - [ ] 2.1.2 Test exception messages contain proper context
  - [ ] 2.1.3 Test original exception types are preserved when re-raising
  - [ ] 2.1.4 Test cleanup helpers handle failures gracefully

- [ ] 2.2 Integration tests:
  - [ ] 2.2.1 Test error scenarios in DuckDB operations with invalid SQL
  - [ ] 2.2.2 Test file I/O error scenarios with DuckDB
  - [ ] 2.2.3 Test table registration/unregistration error scenarios

## 3. Documentation

- [ ] 3.1 Update DuckDB module docstrings with error handling information
- [ ] 3.2 Add examples of proper DuckDB error handling patterns
- [ ] 3.3 Document breaking changes for callers catching generic `Exception`