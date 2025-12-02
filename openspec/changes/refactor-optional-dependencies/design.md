# Design Document: Optional Dependencies Refactoring

## Architecture Overview

This refactoring implements a lazy loading strategy for optional dependencies across the fsspeckit codebase. The design follows these principles:

1. **Graceful Degradation**: Core functionality works without optional dependencies
2. **Clear Error Messages**: Users get helpful guidance when optional features are needed
3. **Performance**: No unnecessary imports or dependency checks
4. **Maintainability**: Consistent patterns across all modules

## Current Problems

1. **Unconditional Imports**: Modules like `common/types.py` import polars/pandas/pyarrow at module level
2. **ImportError Cascade**: Basic fsspeckit functionality fails when optional deps are missing
3. **Inconsistent Patterns**: Some modules use conditional imports, others don't
4. **Poor Error Messages**: Generic ImportError without installation guidance

## Solution Architecture

### 1. Availability Flags Pattern

```python
# At module level
import importlib.util

_POLARS_AVAILABLE = importlib.util.find_spec("polars") is not None
_PANDAS_AVAILABLE = importlib.util.find_spec("pandas") is not None
_PYARROW_AVAILABLE = importlib.util.find_spec("pyarrow") is not None
```

### 2. Lazy Import Functions

```python
def _import_polars():
    """Import polars with proper error handling."""
    if not _POLARS_AVAILABLE:
        raise ImportError(
            "polars is required for this function. "
            "Install with: pip install fsspeckit[datasets]"
        )
    import polars as pl
    return pl
```

### 3. Function-Level Imports

```python
def dict_to_dataframe(data, unique=False):
    """Convert dict to DataFrame using polars."""
    pl = _import_polars()
    # Implementation using pl
```

### 4. Type Handling with TYPE_CHECKING

```python
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl
    import pandas as pd
    import pyarrow as pa
```

## Implementation Strategy

### Phase 1: Core Infrastructure
- Create `common/optional.py` with utility functions
- Define availability flags and import helpers
- Implement consistent error messaging

### Phase 2: Common Modules
- Refactor `common/types.py` (highest impact)
- Refactor `common/polars.py` and `common/datetime.py`
- Update imports in `common/__init__.py`

### Phase 3: Dataset Modules
- Refactor `datasets/pyarrow.py`
- Refactor `datasets/duckdb.py`
- Update dataset imports

### Phase 4: Core and SQL Modules
- Refactor `core/ext.py` and `core/merge.py`
- Refactor `sql/filters/__init__.py`
- Update core imports

## Trade-offs

### Benefits
- **Better User Experience**: No unexpected ImportErrors
- **Faster Imports**: Only load what's needed
- **Clear Dependencies**: Explicit optional dependency requirements
- **Flexible Installation**: Users can install only what they need

### Costs
- **Code Complexity**: Additional indirection and helper functions
- **Runtime Overhead**: Import checks at function call time
- **Maintenance**: More complex import patterns to maintain

## Migration Path

1. **Backward Compatibility**: All existing APIs remain unchanged
2. **Incremental Rollout**: Module by module refactoring
3. **Testing**: Comprehensive tests for both scenarios (with/without deps)
4. **Documentation**: Updated installation guides and error messages

## Validation Strategy

1. **Unit Tests**: Test each function with and without dependencies
2. **Integration Tests**: Test full workflows with minimal installations
3. **Import Tests**: Verify clean imports with only base dependencies
4. **Error Message Tests**: Ensure helpful error messages

## Future Considerations

1. **Plugin Architecture**: This pattern enables future plugin systems
2. **Dynamic Loading**: Could extend to runtime dependency discovery
3. **Performance Monitoring**: Track import performance impact
4. **Dependency Graph**: Could auto-generate dependency requirements