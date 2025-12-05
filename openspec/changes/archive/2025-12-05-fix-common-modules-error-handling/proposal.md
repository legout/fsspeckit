---
brief: Fix Common Modules Error Handling
date: 2025-12-03
# End date (estimated): 2025-12-04
status: draft
---

# Fix Common Modules Error Handling

## Why

Common utility modules in `fsspeckit` currently use broad `except Exception:` blocks that mask specific errors and make debugging difficult. Common issues include:

- Generic exception catching hides specific errors in schema validation and type conversion
- Resource cleanup failures are silently ignored
- Error context is lost when exceptions are re-raised as generic types
- Some operations use `print()` instead of proper logging

## What Changes

**Scope**: `src/fsspeckit/common/misc.py` and `src/fsspeckit/common/schema.py`

- Replace generic `except Exception:` with specific exception types:
  - `ValueError` for data validation failures
  - `TypeError` for type conversion issues
  - `KeyError` for missing keys/fields
  - `AttributeError` for missing attributes
  - `RuntimeError` for runtime failures
  - `ImportError` for module import issues
- Preserve original exception types when re-raising
- Add context-specific error messages that include operation details
- Ensure cleanup operations log failures individually while continuing cleanup process
- Route all error logging through `fsspeckit.common.logging.get_logger`

## Impact

- **Behaviour**: More precise error messages, better debugging experience
- **Backwards Compatibility**: Exception types will be more specific (breaking change for callers catching generic `Exception`)
- **Performance**: Minimal impact, slightly better exception handling performance
- **Maintainability**: Easier to identify and fix common utility issues

## Code affected

- `src/fsspeckit/common/misc.py` (~3 exception blocks)
- `src/fsspeckit/common/schema.py` (~3 exception blocks)

## Dependencies

- Requires proper import fallbacks for exception types when dependencies are optional
- Needs proper import fallbacks for when dependencies are not installed

## Implementation Requirements

1. Replace all `except Exception:` blocks in common modules with specific exception types
2. Add proper import fallbacks for exception types
3. Enhance error messages with operation context (schema details, operation details)
4. Ensure cleanup helpers log failures individually but continue cleanup process
5. Ensure all unexpected errors are logged and re-raised
6. Replace `print()` calls with logger calls obtained via `fsspeckit.common.logging.get_logger`