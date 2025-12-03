---
brief: Fix PyArrow Error Handling
date: 2025-12-03
# End date (estimated): 2025-12-04
status: draft
---

# Fix PyArrow Error Handling

## Why

PyArrow operations in `fsspeckit` currently use broad exception handling that masks specific errors and makes debugging difficult. Common issues include:

- Generic `except Exception:` blocks hide PyArrow-specific errors (ArrowInvalid, ArrowNotImplementedError, ArrowIOError, etc.)
- Dataset and filesystem operation failures are silently ignored or poorly reported
- Error context is lost when dealing with Parquet files, datasets, and compute operations
- Some exceptions are re-raised as generic `Exception` instead of preserving the original type

## What Changes

**Scope**: `src/fsspeckit/datasets/pyarrow.py` and related PyArrow utilities

- Replace generic `except Exception:` with specific PyArrow exception types:
  - `pyarrow.ArrowInvalid` for invalid data/schema operations
  - `pyarrow.ArrowNotImplementedError` for unsupported operations
  - `pyarrow.ArrowIOError` for file I/O problems
  - `pyarrow.ArrowTypeError` for type-related errors
  - `pyarrow.ArrowKeyError` for missing keys/fields
  - `pyarrow.ArrowIndexError` for out-of-bounds access
- Preserve original exception types when re-raising
- Add context-specific error messages that include operation details (file paths, schema info)
- Ensure dataset cleanup operations log failures but continue cleanup process
- Import PyArrow exception types properly with fallback handling for when PyArrow is not installed

## Impact

- **Behaviour**: More precise error messages for PyArrow operations, better debugging experience
- **Backwards Compatibility**: Exception types will be more specific (breaking change for callers catching generic `Exception`)
- **Performance**: Minimal impact, slightly better exception handling performance
- **Maintainability**: Easier to identify and fix PyArrow-specific issues

## Code affected

- `src/fsspeckit/datasets/pyarrow.py` (~15 exception blocks)
- `src/fsspeckit/utils/pyarrow.py` (~8 exception blocks)

## Dependencies

- Requires PyArrow to be available for exception type imports
- Needs proper import fallback for when PyArrow is not installed

## Implementation Requirements

1. Replace all `except Exception:` blocks in PyArrow modules with specific exception types
2. Add proper import fallbacks for PyArrow exception types
3. Enhance error messages with operation context (file paths, schema info, operation details)
4. Ensure cleanup helpers log failures individually but continue cleanup process
5. Ensure all unexpected errors are logged and re-raised
6. Replace `print()` calls with logger calls obtained via `fsspeckit.common.logging.get_logger`