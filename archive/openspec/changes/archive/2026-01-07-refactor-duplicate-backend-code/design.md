# Design: Refactor Duplicate Backend Code

## Context
The datasets module has massive code duplication between PyArrow and DuckDB backends, violating DRY principle and making maintenance extremely difficult. Current analysis shows:
- Nearly identical merge logic implementations in both backends
- Duplicate compaction and optimization operations
- Inconsistent error handling patterns
- Same validation logic duplicated across backends
- Maintenance burden: any bug fix requires changes in multiple places

## Goals / Non-Goals

### Goals
- Eliminate code duplication between PyArrow and DuckDB backends
- Create maintainable architecture with shared utilities
- Ensure both backends behave identically for common operations
- Improve testability and debugging
- Enable easier addition of new backends in the future

### Non-Goals
- Changing the public API or user interface
- Performance optimizations (separate concern)
- Adding new functionality or features
- Modifying backend-specific optimizations
- Breaking backwards compatibility

## Architectural Decisions

### Decision 1: Abstract Base Class Pattern
**Decision**: Create `BaseDatasetHandler` abstract base class with common implementations
**Rationale**: Provides clear inheritance structure and ensures consistent interface

**Base Class Structure**:
```python
class BaseDatasetHandler(ABC):
    """Abstract base class for dataset handlers."""
    
    @abstractmethod
    def _backend_specific_operation(self, *args, **kwargs):
        """Backend-specific operation that subclasses must implement."""
        pass
    
    def merge(self, data, path, strategy, key_columns, **kwargs):
        """Common merge logic using template method pattern."""
        # Common validation and preprocessing
        self._validate_merge_inputs(data, path, strategy, key_columns)
        
        # Backend-specific execution
        result = self._execute_merge_backend_specific(data, path, strategy, key_columns, **kwargs)
        
        # Common postprocessing
        return self._postprocess_merge_result(result)
```

### Decision 2: Shared Utilities in Core Module
**Decision**: Extract common logic into `fsspeckit.core.merge` and `fsspeckit.core.maintenance`
**Rationale**: Clear separation of concerns and enables reuse across backends

**Shared Modules Structure**:
```
fsspeckit/core/
├── merge/
│   ├── __init__.py
│   ├── validation.py      # Common validation logic
│   ├── strategies.py      # Merge strategy implementations
│   ├── planning.py        # Incremental rewrite planning
│   └── statistics.py      # Merge statistics calculation
├── maintenance/
│   ├── compaction.py      # Common compaction logic
│   ├── optimization.py    # Common optimization logic
│   └── deduplication.py   # Common deduplication logic
└── common/
    ├── error_handling.py  # Standardized error handling
    ├── filesystem_utils.py # Path normalization, etc.
```

### Decision 3: Template Method Pattern for Merge Operations
**Decision**: Use template method pattern where base class defines algorithm structure
**Rationale**: Allows backend-specific implementation while maintaining consistent flow

**Template Method Structure**:
```python
class BaseDatasetHandler:
    def merge(self, data, path, strategy, key_columns, **kwargs):
        """Template method for merge operations."""
        # Step 1: Common validation
        self._validate_inputs(data, path, strategy, key_columns)
        
        # Step 2: Backend-specific preparation
        prepared_data = self._prepare_data_backend_specific(data)
        
        # Step 3: Common merge execution
        result = self._execute_merge_common(prepared_data, path, strategy, key_columns, **kwargs)
        
        # Step 4: Backend-specific cleanup
        self._cleanup_backend_specific()
        
        return result
```

### Decision 4: Strategy Pattern for Backend-Specific Operations
**Decision**: Use strategy pattern for operations that differ significantly between backends
**Rationale**: Clean separation of backend-specific logic while maintaining common interface

**Strategy Implementation**:
```python
class MergeStrategy:
    """Strategy interface for merge operations."""
    
    @abstractmethod
    def execute(self, handler, data, path, key_columns, **kwargs):
        pass

class DuckDBMergeStrategy(MergeStrategy):
    """DuckDB-specific merge strategy."""
    def execute(self, handler, data, path, key_columns, **kwargs):
        # DuckDB-specific implementation
        pass

class PyArrowMergeStrategy(MergeStrategy):
    """PyArrow-specific merge strategy."""
    def execute(self, handler, data, path, key_columns, **kwargs):
        # PyArrow-specific implementation
        pass
```

## Implementation Plan

### Phase 1: Shared Utilities Extraction (Weeks 1-2)
1. **Create core.merge module**
   - Extract common validation logic
   - Move merge strategy implementations
   - Create planning utilities

2. **Create core.maintenance module**
   - Extract compaction logic
   - Extract optimization logic
   - Extract deduplication logic

3. **Update both backends** to use shared utilities

### Phase 2: Base Class Creation (Weeks 3-4)
1. **Design BaseDatasetHandler**
   - Define abstract methods for backend-specific operations
   - Implement common template methods
   - Ensure backward compatibility

2. **Refactor DuckDB backend**
   - Inherit from BaseDatasetHandler
   - Implement abstract methods
   - Use inherited common methods

3. **Refactor PyArrow backend**
   - Inherit from BaseDatasetHandler
   - Implement abstract methods
   - Use inherited common methods

### Phase 3: Testing and Validation (Weeks 5-6)
1. **Comprehensive testing**
   - Ensure both backends produce identical results
   - Test all merge strategies
   - Validate performance hasn't degraded

2. **Integration testing**
   - Test interaction between backends
   - Verify error handling consistency
   - Test edge cases and failure scenarios

## Risks / Trade-offs

### Risk 1: Increased Complexity
**Risk**: Adding abstraction layers may make code harder to understand initially
**Mitigation**:
- Clear documentation and examples
- Step-by-step migration plan
- Maintain detailed inline comments

### Risk 2: Performance Impact
**Risk**: Additional abstraction layers may impact performance
**Mitigation**:
- Benchmark before and after refactoring
- Optimize hot paths in shared utilities
- Use final methods where performance is critical

### Risk 3: Breaking Changes
**Risk**: Refactoring may accidentally change behavior
**Mitigation**:
- Comprehensive test suite
- Incremental refactoring with validation
- Feature flags for rollback capability

### Risk 4: Backward Compatibility
**Risk**: Changes to internal structure may affect existing code
**Mitigation**:
- Maintain exact same public API
- Document any internal changes clearly
- Provide migration guide if needed

## Migration Strategy

### Step 1: Shared Utilities First
1. Extract common code without changing backends
2. Update backends to use new utilities
3. Ensure identical behavior

### Step 2: Introduce Base Class
1. Create BaseDatasetHandler with common methods
2. Update backends to inherit (non-destructively)
3. Gradually move common code to base class

### Step 3: Remove Duplication
1. Remove duplicate code from backends
2. Use inherited/common implementations
3. Verify all tests still pass

### Step 4: Cleanup and Optimization
1. Remove unused code
2. Optimize shared utilities
3. Update documentation

## Success Metrics

### Code Quality Metrics
- **Code duplication**: <5% duplicate code between backends
- **Test coverage**: >95% for shared utilities
- **Cyclomatic complexity**: <10 for shared utilities

### Functionality Metrics
- **Correctness**: 100% identical results between backends
- **Performance**: No degradation in operation speed
- **Reliability**: Same or better error handling

### Maintainability Metrics
- **Bug fixes**: Single location for common bug fixes
- **Feature additions**: Shared implementation for common features
- **Development time**: Reduced time for cross-backend changes

## Open Questions

### Question 1: Extent of Shared Code
**Question**: How much logic should be shared vs kept backend-specific?
**Current Approach**: Share validation, planning, and algorithm structure; keep execution backend-specific

### Question 2: Testing Strategy
**Question**: Should we have separate tests for shared utilities vs integration tests?
**Current Approach**: Both - unit tests for shared utilities, integration tests for full workflows

### Question 3: Future Backend Support
**Question**: How should the architecture support future backends (e.g., Spark, DataFusion)?
**Current Approach**: Clear abstract base class and documented interface make it straightforward
