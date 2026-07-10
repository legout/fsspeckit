# Implementation Guide

## Getting Started

### 1. Create the Logging Configuration Module
First, create the central logging configuration file:

```python
# src/fsspeckit/common/logging_config.py
import logging
import os
import sys
from typing import Optional

# Global registry to prevent duplicate configuration
_configured = False

def setup_logging(
    level: str = "INFO",
    format_string: Optional[str] = None,
    include_timestamp: bool = True,
    enable_console: bool = True,
    enable_file: bool = False,
    file_path: Optional[str] = None
) -> None:
    """
    Configure logging for the fsspeckit package.
    
    This should be called once at application startup.
    """
    global _configured
    
    if _configured:
        return
        
    # Parse level from environment if not provided
    if not level:
        level = os.getenv('FSSPECKIT_LOG_LEVEL', 'INFO')
    
    # Set default format
    if not format_string:
        timestamp_part = "%(asctime)s - " if include_timestamp else ""
        format_string = f"{timestamp_part}%(name)s - %(levelname)s - %(message)s"
    
    # Configure root logger
    root_logger = logging.getLogger('fsspeckit')
    root_logger.setLevel(getattr(logging, level.upper()))
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Console handler
    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter(format_string))
        root_logger.addHandler(console_handler)
    
    # File handler
    if enable_file and file_path:
        file_handler = logging.FileHandler(file_path)
        file_handler.setFormatter(logging.Formatter(format_string))
        root_logger.addHandler(file_handler)
    
    _configured = True

def get_logger(name: str) -> logging.Logger:
    """
    Get a properly configured logger for a module.
    
    Args:
        name: Module name (typically __name__)
        
    Returns:
        Configured logger instance
    """
    # Auto-configure if not already done
    if not _configured:
        setup_logging()
    
    return logging.getLogger(f"fsspeckit.{name}")
```

### 2. Update Package Initialization
Modify the main package init file to configure logging:

```python
# src/fsspeckit/__init__.py
from .common.logging_config import setup_logging

# Configure logging when package is imported
setup_logging()
```

### 3. Update Individual Modules
Replace print statements and add proper error handling:

**Before:**
```python
def read_file(file_path):
    try:
        with open(file_path, 'rb') as f:
            return f.read()
    except Exception as e:
        raise RuntimeError(f"Failed to read file: {file_path}")
```

**After:**
```python
from ..common.logging_config import get_logger

logger = get_logger(__name__)

def read_file(file_path):
    """Read file contents with proper error handling and logging."""
    try:
        with open(file_path, 'rb') as f:
            result = f.read()
        
        logger.info("Successfully read %d bytes from %s", len(result), file_path)
        return result
        
    except FileNotFoundError as e:
        logger.error("File not found: %s", file_path)
        raise FileNotFoundError(f"File not found: {file_path}") from e
        
    except PermissionError as e:
        logger.error("Permission denied: %s", file_path)
        raise PermissionError(f"Permission denied: {file_path}") from e
        
    except OSError as e:
        logger.error("OS error reading %s: %s", file_path, e)
        raise OSError(f"OS error reading {file_path}: {e}") from e
        
    except Exception as e:
        logger.error("Unexpected error reading %s: %s", file_path, e, exc_info=True)
        raise RuntimeError(f"Unexpected error reading {file_path}") from e
```

### 4. Update Exception Cleanup Patterns
For cleanup operations where individual errors shouldn't stop the process:

```python
def cleanup_resources(resources):
    """Clean up multiple resources, logging failures individually."""
    for i, resource in enumerate(resources):
        try:
            resource.close()
            logger.debug("Successfully closed resource %d: %s", i, resource.name)
        except Exception as e:
            logger.error("Failed to close resource %d (%s): %s", i, resource.name, e, exc_info=True)
            # Continue with other resources
```

### 5. Environment Variable Configuration
Users can configure logging via environment variables:

```bash
export FSSPECKIT_LOG_LEVEL=DEBUG
export FSSPECKIT_LOG_TIMESTAMP=false
export FSSPECKIT_LOG_FILE=/tmp/fsspeckit.log
```

### 6. Performance Considerations
For performance-critical code, use guards around expensive debug operations:

```python
def process_dataset(dataset):
    logger.info("Starting dataset processing")
    
    # Guard expensive debug computation
    if logger.isEnabledFor(logging.DEBUG):
        stats = dataset.get_statistics()
        logger.debug("Dataset stats: rows=%d, size=%sMB", stats.rows, stats.size_mb)
    
    # ... processing code
```

## Migration Checklist

### Phase 1: Infrastructure
- [ ] Create `src/fsspeckit/common/logging_config.py`
- [ ] Update `src/fsspeckit/__init__.py` to call `setup_logging()`
- [ ] Test basic logging functionality

### Phase 2: Core Modules
- [ ] Update `src/fsspeckit/core/ext.py`
- [ ] Update `src/fsspeckit/core/filesystem.py`
- [ ] Update `src/fsspeckit/core/maintenance.py`
- [ ] Update `src/fsspeckit/core/merge.py`

### Phase 3: Utility Modules
- [ ] Update all modules in `src/fsspeckit/utils/`
- [ ] Update modules in `src/fsspeckit/datasets/`
- [ ] Update modules in `src/fsspeckit/storage_options/`

### Phase 4: Validation
- [ ] Remove all `print()` statements
- [ ] Verify exception handling consistency
- [ ] Test environment variable configuration
- [ ] Verify performance impact

## Testing the Implementation

### Unit Tests
```python
def test_logging_configuration():
    from fsspeckit.common.logging_config import setup_logging, get_logger
    import logging
    
    # Test configuration
    setup_logging(level="DEBUG")
    logger = get_logger("test")
    
    assert logger.level == logging.DEBUG
    assert logger.name == "fsspeckit.test"

def test_exception_logging():
    from fsspeckit.common.logging_config import get_logger
    import io
    import logging
    
    # Capture log output
    log_capture = io.StringIO()
    handler = logging.StreamHandler(log_capture)
    
    logger = get_logger("test")
    logger.addHandler(handler)
    logger.setLevel(logging.ERROR)
    
    try:
        raise ValueError("Test error")
    except Exception as e:
        logger.error("Operation failed: %s", e, exc_info=True)
    
    log_output = log_capture.getvalue()
    assert "Operation failed" in log_output
    assert "ValueError" in log_output
    assert "Test error" in log_output
```

## Success Criteria

1. ✅ All `print()` statements replaced with logger calls
2. ✅ Specific exception types used instead of generic `RuntimeError`
3. ✅ Consistent error message format with context
4. ✅ Logging is configurable via environment variables
5. ✅ Performance impact is minimal
6. ✅ All existing tests pass
7. ✅ New logging tests added

## Troubleshooting

### Common Issues

1. **Duplicate log messages**: Ensure `setup_logging()` is only called once
2. **Missing log output**: Check log level configuration
3. **Performance impact**: Use guards around expensive debug operations
4. **Import issues**: Verify relative imports in submodules

### Debugging Logging Issues

```python
# Check logger configuration
logger = get_logger(__name__)
print(f"Logger level: {logger.level}")
print(f"Logger handlers: {logger.handlers}")
print(f"Logger parent: {logger.parent}")
print(f"Is enabled for DEBUG: {logger.isEnabledFor(logging.DEBUG)}")
```

This implementation guide provides a complete, production-ready approach to standardizing logging infrastructure across the fsspeckit codebase.