# Change: Fix Critical Issues in Refactor Duplicate Backend Code

## Why
The `refactor-duplicate-backend-code` implementation contains critical bugs that prevent safe merging:
- Unreachable dead code in `plan_source_processing` and `_create_empty_source_result` functions
- Syntax error in DuckDB dataset class with incorrect indentation
- Performance issue with O(n) key matching instead of O(1) set lookup
- Potential SQL injection vulnerability in DuckDB methods
- Missing inheritance relationship as specified in the design document

These issues must be resolved before the refactoring can be safely merged to prevent runtime errors, security vulnerabilities, and performance degradation.

## What Changes
- **Remove unreachable dead code** in `src/fsspeckit/core/merge.py` (lines 588-618 and 822-852)
- **Fix syntax error** in `src/fsspeckit/datasets/duckdb/dataset.py` (incorrect indentation in `write_dataset`)
- **Fix performance issue** in `select_rows_by_keys_common` to use set-based key lookup
- **Add SQL injection protection** by validating file paths before SQL interpolation
- **Complete DuckDB inheritance** from `BaseDatasetHandler` as per design document
- **Remove redundant logic** in multi-column key handling
- **Standardize import patterns** for PyArrow imports
- **Improve exception handling** with proper logging instead of broad `except Exception`

## Impact
- **Affected specs**: datasets-duckdb, datasets-pyarrow, core-merge
- **Affected code**:
  - `src/fsspeckit/core/merge.py` (dead code removal, performance fix)
  - `src/fsspeckit/datasets/duckdb/dataset.py` (syntax fix, security fix, inheritance)
  - `src/fsspeckit/datasets/base.py` (already exists, just needs DuckDB to use it)
- **Breaking changes**: None - all fixes maintain existing API
- **Dependencies**: None - purely bug fixes without new dependencies