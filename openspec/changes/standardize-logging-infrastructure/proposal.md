---
brief: Standardize Logging Infrastructure
date: 2025-12-03
# End date (estimated): 2025-12-04
status: draft
---

# Standardize Logging Infrastructure

## Why

The fsspeckit codebase currently has inconsistent logging patterns that make debugging and maintenance difficult:

- Mix of `print()` statements and logger usage across modules
- No central configuration for logging behavior
- Missing structured information in log messages
- Inconsistent log levels and error reporting patterns
- Exceptions not properly logged with stack traces and context

## What Changes

**Scope**: Create centralized logging infrastructure and update error handling modules to use consistent logging

### 1. Centralized Logging Configuration

Create `src/fsspeckit/common/logging_config.py` with:

```python
import logging
import sys
from typing import Optional

def setup_logging(
    level: str = "INFO",
    format_string: Optional[str] = None,
    include_timestamp: bool = True
) -> None:
    """Configure logging for the entire fsspeckit package."""
    
def get_logger(name: str) -> logging.Logger:
    """Get a properly configured logger for a module."""
```

### 2. Standardized Logger Usage Patterns

- **Module-level Logger**: `logger = get_logger(__name__)`
- **Log Level Guidelines**: DEBUG for debugging, INFO for operations, WARNING for recoverable issues, ERROR for serious errors
- **Exception Logging**: Use `exc_info=True` for proper stack traces
- **Structured Messages**: Use lazy string formatting with contextual information

### 3. Replace Print Statements

Update all modules to use proper logging instead of `print()` statements

## Impact

- **Behaviour**: More consistent and informative logging across the codebase
- **Debuggability**: Better context for troubleshooting with structured log messages
- **Maintainability**: Single source of truth for logging configuration
- **Performance**: Proper lazy string formatting and optional debug guards

## Code affected

- **New Files**: `src/fsspeckit/common/logging_config.py`
- **Updated Files**: All modules with `print()` statements or inconsistent logging:
  - `src/fsspeckit/core/ext.py`
  - `src/fsspeckit/core/filesystem.py`
  - `src/fsspeckit/datasets/duckdb.py`
  - `src/fsspeckit/datasets/pyarrow.py`
  - `src/fsspeckit/utils/` modules
  - `src/fsspeckit/__init__.py`

## Dependencies

- Python standard library `logging` module
- No external dependencies required

## Implementation Requirements

1. Create centralized logging configuration module
2. Update `__init__.py` to configure logging on import
3. Replace all `print()` statements with appropriate logger calls
4. Add exception logging with `exc_info=True` where needed
5. Standardize log message formats with contextual information
6. Add performance guards for expensive debug operations