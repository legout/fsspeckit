# Change: Fix Error Handling and Path Normalization Issues

## Why
The datasets module has several critical reliability issues:

1. **Silent failures**: Broad `except Exception` handlers that return incorrect data without logging
2. **Path normalization failures**: `os.path.abspath()` only works for local filesystems, not cloud storage
3. **Inconsistent error handling**: Mix of different exception types and error messages
4. **Missing error context**: Users get generic errors without actionable information

## What Changes
- Replace broad exception handlers with specific error handling and proper logging
- Fix path normalization to work with both local and cloud filesystems
- Standardize error handling patterns across all dataset operations
- Add proper error context and user-friendly error messages
- Implement consistent exception types and hierarchy

## Impact
- **Affected specs**: datasets-pyarrow, datasets-duckdb, common-error-handling
- **Affected code**: 
  - `src/fsspeckit/datasets/pyarrow/io.py` (lines 294-308)
  - `src/fsspeckit/datasets/pyarrow/io.py` (lines 78-83)
  - Multiple files with error handling issues
- **Reliability impact**: Prevents silent failures and provides better debugging information
- **Breaking changes**: None (improved error messages only)
