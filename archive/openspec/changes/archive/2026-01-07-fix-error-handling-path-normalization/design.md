# Design: Fix Error Handling & Path Normalization

## Context
The datasets module has several critical reliability issues:
- **Silent failures**: Broad `except Exception` handlers return incorrect data without logging
- **Path normalization failures**: `os.path.abspath()` only works for local filesystems
- **Inconsistent error handling**: Mix of different exception types and messages
- **Missing error context**: Users get generic errors without actionable information

## Goals / Non-Goals

### Goals
- Eliminate silent failures with proper error handling and logging
- Fix path normalization to work with both local and cloud filesystems
- Standardize error handling patterns across all dataset operations
- Provide actionable, contextual error messages
- Maintain backward compatibility

### Non-Goals
- Changing the public API or user interface
- Adding new functionality or features
- Performance optimizations (separate concern)
- Modifying backend-specific logic beyond error handling

## Error Handling Strategy

### Decision 1: Specific Exception Hierarchy
**Decision**: Implement a consistent exception hierarchy for dataset operations
**Rationale**: Enables proper error handling and provides clear error categorization

**Exception Hierarchy**:
```
fsspeckit.datasets.exceptions
├── DatasetError (base)
│   ├── DatasetOperationError (operation-specific)
│   ├── DatasetValidationError (input validation)
│   ├── DatasetFileError (file I/O issues)
│   ├── DatasetPathError (path-related issues)
│   └── DatasetMergeError (merge operation issues)
```

**Implementation Pattern**:
```python
class DatasetError(Exception):
    """Base exception for all dataset operations."""
    def __init__(self, message, operation=None, details=None):
        super().__init__(message)
        self.operation = operation
        self.details = details or {}

class DatasetOperationError(DatasetError):
    """Raised when dataset operations fail."""
    pass

class DatasetPathError(DatasetError):
    """Raised when path operations fail."""
    pass
```

### Decision 2: Contextual Error Messages
**Decision**: Include contextual information in all error messages
**Rationale**: Helps users understand what went wrong and how to fix it

**Error Message Template**:
```
{operation} failed: {reason}
Path: {path}
Details: {additional_context}
Suggested action: {remediation}
```

**Example**:
```
Dataset merge failed: Key column 'user_id' not found in source data
Path: /data/dataset/
Details: {'available_columns': ['id', 'name', 'timestamp']}
Suggested action: Check that key_columns matches actual column names in source data
```

### Decision 3: Proper Logging Strategy
**Decision**: Use structured logging with appropriate levels
**Rationale**: Enables debugging and monitoring while avoiding log spam

**Logging Strategy**:
```python
import structlog

logger = structlog.get_logger(__name__)

# Error cases - always log
logger.error("operation_failed", 
             operation="merge",
             error=str(e),
             path=path,
             exc_info=True)

# Warning cases - log when appropriate
logger.warning("operation_completed_with_warnings",
               operation="compact",
               warnings=warnings)

# Info cases - log for important operations
logger.info("operation_completed",
            operation="merge",
            duration=duration,
            rows_processed=rows)
```

## Path Normalization Strategy

### Decision 1: Filesystem-Aware Path Normalization
**Decision**: Use filesystem-specific path normalization instead of `os.path.abspath()`
**Rationale**: `os.path.abspath()` only works for local filesystems

**Normalization Strategy**:
```python
def normalize_path(path: str, filesystem: AbstractFileSystem) -> str:
    """Normalize path based on filesystem type."""
    if isinstance(filesystem, LocalFileSystem):
        # Local filesystem - use os.path operations
        return os.path.abspath(path)
    elif hasattr(filesystem, 'protocol'):
        # Remote filesystem - preserve protocol and structure
        if '://' in path:
            # Already has protocol
            return path
        else:
            # Add protocol based on filesystem
            return f"{filesystem.protocol}://{path}"
    else:
        # Fallback - return as-is
        return path
```

### Decision 2: Path Validation Enhancement
**Decision**: Enhance path validation to catch more issues early
**Rationale**: Prevents confusing errors later in the operation

**Validation Strategy**:
```python
def validate_dataset_path(path: str, filesystem: AbstractFileSystem, operation: str):
    """Comprehensive path validation for dataset operations."""
    
    # Check path exists for read operations
    if operation in ['read', 'merge']:
        if not filesystem.exists(path):
            raise DatasetPathError(
                f"Dataset path does not exist: {path}",
                operation=operation,
                details={'path': path}
            )
    
    # Check parent directory exists for write operations
    if operation in ['write', 'merge']:
        parent = filesystem._parent(path)
        if parent and not filesystem.exists(parent):
            raise DatasetPathError(
                f"Parent directory does not exist: {parent}",
                operation=operation,
                details={'path': path, 'parent': parent}
            )
    
    # Validate path format
    if '://' in path:
        # Remote path - validate protocol
        protocol = path.split('://')[0]
        if protocol not in ['s3', 'gs', 'az', 'file', 'github', 'gitlab']:
            raise DatasetPathError(
                f"Unsupported protocol: {protocol}",
                operation=operation,
                details={'path': path, 'protocol': protocol}
            )
```

## Implementation Plan

### Phase 1: Exception Hierarchy (Week 1)
1. **Create exception classes**
   - Define base `DatasetError` class
   - Create specific exception types
   - Add proper inheritance structure

2. **Update imports**
   - Export exceptions from main `datasets` module
   - Update internal imports to use new exceptions

### Phase 2: Error Handling Updates (Week 2)
1. **Replace broad exception handlers**
   - Identify all `except Exception:` patterns
   - Replace with specific exception handling
   - Add proper logging and context

2. **Standardize error messages**
   - Update all error messages to use template
   - Add contextual information
   - Include suggested remediation steps

### Phase 3: Path Normalization Fixes (Week 3)
1. **Create filesystem-aware normalization**
   - Implement `normalize_path()` function
   - Handle local and remote filesystems
   - Add comprehensive path validation

2. **Update path operations**
   - Replace `os.path.abspath()` calls
   - Update path normalization in all backends
   - Add path validation where needed

### Phase 4: Testing and Validation (Week 4)
1. **Comprehensive testing**
   - Test error handling with various failure scenarios
   - Validate path normalization with different filesystems
   - Ensure no silent failures occur

2. **Integration testing**
   - Test error propagation across backends
   - Validate error message consistency
   - Test backward compatibility

## Risks / Trade-offs

### Risk 1: Breaking Changes
**Risk**: Changes to error handling may break existing error handling code
**Mitigation**:
- Maintain backward compatibility for exception types
- Provide clear migration guide
- Add deprecation warnings for old patterns

### Risk 2: Performance Impact
**Risk**: Enhanced validation and logging may impact performance
**Mitigation**:
- Make validation optional for performance-critical paths
- Use efficient logging strategies
- Profile and optimize hot paths

### Risk 3: Increased Complexity
**Risk**: More complex error handling may be harder to maintain
**Mitigation**:
- Clear documentation and examples
- Consistent patterns across codebase
- Automated testing for error scenarios

## Success Metrics

### Reliability Metrics
- **Silent failures**: 0 silent failures in test suite
- **Error coverage**: >95% of error scenarios properly handled
- **Logging coverage**: 100% of errors properly logged

### Usability Metrics
- **Error clarity**: All error messages include actionable information
- **Debuggability**: Error messages include sufficient context
- **Recovery guidance**: Error messages suggest remediation steps

### Compatibility Metrics
- **Backward compatibility**: 100% API compatibility maintained
- **Exception handling**: Existing try/catch blocks continue to work
- **Performance**: No significant performance regression

## Open Questions

### Question 1: Exception vs Return Value
**Question**: Should validation errors raise exceptions or return error values?
**Current Decision**: Raise exceptions for consistency and to avoid error handling complexity

### Question 2: Logging Granularity
**Question**: How detailed should error logging be?
**Current Decision**: Log all errors with context, warnings selectively, info for major operations

### Question 3: Path Normalization Scope
**Question**: Should path normalization be applied universally or only in specific operations?
**Current Decision**: Apply in all dataset operations for consistency

### Question 4: Error Recovery
**Question**: Should we attempt error recovery or just fail fast?
**Current Decision**: Fail fast with clear error messages; recovery is application-specific
