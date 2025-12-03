# Tasks: Fix Core IO Error Handling

## Core Implementation Tasks

### 1. Update Exception Hierarchy in `src/fsspeckit/core/ext.py`
- [ ] Replace generic `RuntimeError` with specific exception types:
  - Use `FileNotFoundError` for missing files
  - Use `PermissionError` for access issues  
  - Use `OSError` for system-level I/O errors
  - Use `TimeoutError` for operations that timeout
  - Use `ValueError` for invalid parameters
- [ ] Preserve original exceptions with `from e` clause
- [ ] Update all public functions to use consistent error handling

### 2. Enhance Error Messages
- [ ] Add file path URI to error context
- [ ] Include operation type (read, write, metadata, list)  
- [ ] Add backend storage system information
- [ ] Include relevant parameters in error messages

### 3. Replace Print Statements with Logging
- [ ] Import `logging` module at top of file
- [ ] Create module-level logger: `logger = logging.getLogger(__name__)`
- [ ] Replace `print()` calls with appropriate logger calls:
  - `logger.warning()` for recoverable issues
  - `logger.error()` for errors
  - Use `exc_info=True` for exception logging
- [ ] Add debug logging for successful operations

### 4. Update Function Documentation  
- [ ] Document exception types raised by each function
- [ ] Update docstrings to reflect specific error conditions
- [ ] Add examples of error handling patterns

### 5. Add Helper Functions
- [ ] Create standardized error handling decorator or utility function
- [ ] Add error context collection utility
- [ ] Implement consistent error formatting

## Testing Tasks

### 1. Update Existing Tests
- [ ] Update test expectations to match new exception types
- [ ] Replace `RuntimeError` expectations with specific exceptions
- [ ] Test error message content and context

### 2. Add New Error Scenario Tests  
- [ ] Test FileNotFoundError scenarios
- [ ] Test PermissionError scenarios
- [ ] Test OSError scenarios
- [ ] Test timeout error handling
- [ ] Test invalid parameter errors

### 3. Logging Verification Tests
- [ ] Verify logger calls replace print statements
- [ ] Test log message formats and levels
- [ ] Test exception logging with `exc_info=True`

## Example Implementation Pattern
```python
import logging
logger = logging.getLogger(__name__)

def safe_file_operation(file_path, operation="read"):
    """Standardized error handling pattern for file operations"""
    context = {
        "file": str(file_path), 
        "operation": operation
    }
    
    try:
        # Perform operation
        result = perform_operation(file_path)
        logger.debug("Successfully %s %s", operation, context["file"])
        return result
    except FileNotFoundError as e:
        logger.error("File not found during %s: %s", operation, context["file"])
        raise FileNotFoundError(f"File not found: {context['file']}") from e
    except PermissionError as e:
        logger.error("Permission denied during %s: %s", operation, context["file"])
        raise PermissionError(f"Permission denied: {context['file']}") from e
    # ... other exception types
```

## Files to Modify
1. `src/fsspeckit/core/ext.py` - Main implementation
2. Tests in `tests/` directory - Update expectations
3. Documentation - Update API docs as needed

## Review Checklist
- [ ] All exception types are specific and appropriate
- [ ] Error messages include full context  
- [ ] No print statements remain
- [ ] Original exceptions are preserved with `from e`
- [ ] All tests pass with new exception types
- [ ] Logging is working properly