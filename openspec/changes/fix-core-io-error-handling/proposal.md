## Why

Core IO operations in fsspeckit currently use broad exception handling that masks specific errors and makes debugging difficult. Common issues include:

- Generic `except Exception:` blocks that catch everything including system errors
- Resource cleanup failures are silently ignored
- Error context is lost when exceptions are re-raised as generic types
- Some operations use `print()` instead of proper logging

## What Changes

**Scope**: `src/fsspeckit/core/ext.py` and related core IO operations

- Replace generic exception handling with specific exception types:
  - `FileNotFoundError` for file access issues
  - `PermissionError` for permission problems
  - `OSError` for system-level I/O issues
  - `ValueError` for data validation failures
  - `TypeError` for type conversion issues
- `RuntimeError` for runtime failures
- `ImportError` for module import issues

- Add proper import fallbacks for exception types when dependencies are optional
- Ensure cleanup operations log failures individually while continuing cleanup process
- Route all error logging through `fsspeckit.common.logging.get_logger`
- Add context-rich error messages with operation details

## Impact

- **Behaviour**: More precise error messages, better debugging experience
- **Backwards Compatibility**: Exception types will be more specific (breaking change for callers catching generic `Exception`)
- **Performance**: Minimal impact, slightly better exception handling performance
- **Maintainability**: Easier to identify and fix IO-specific issues

## Code affected

- `src/fsspeckit/core/ext.py` (~25 exception blocks)
- Related core IO helper functions

## Dependencies

- Requires proper import fallbacks for exception types when dependencies are optional
- Needs proper import fallbacks for when dependencies are not installed

## Implementation Requirements

1. Replace all `except Exception:` blocks in core IO modules with specific exception types
2. Add proper import fallbacks for exception types
3. Enhance error messages with operation context (file paths, operation details)
4. Ensure cleanup helpers log failures individually but continue cleanup process
5. Ensure all unexpected errors are logged and re-raised
6. Replace `print()` calls with logger calls obtained via `fsspeckit.common.logging.get_logger`