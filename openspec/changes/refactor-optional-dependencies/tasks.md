/# Tasks: refactor-optional-dependencies

## Progress Overview

**Overall Progress:** 9/18 tasks completed (50%)

**Phase Progress:**
- Phase 1: Core Infrastructure - 2/2 tasks completed ✅
- Phase 2: Common Modules Refactoring - 3/4 tasks completed
- Phase 3: Dataset Modules Refactoring - 3/3 tasks completed ✅
- Phase 4: Core and SQL Modules - 0/3 tasks completed  
- Phase 3: Dataset Modules Refactoring - 0/3 tasks completed
- Phase 4: Core and SQL Modules - 0/3 tasks completed
- Phase 5: Testing and Validation - 0/3 tasks completed
- Phase 6: Validation and Cleanup - 0/3 tasks completed

---

## Implementation Tasks

### Phase 1: Core Infrastructure

- [x] **Phase 1: Core Infrastructure**

1. [x] **Create optional dependency utilities**
   - [x] Create `src/fsspeckit/common/optional.py` with helper functions
   - [x] Implement availability flags for polars, pandas, pyarrow, duckdb
   - [x] Add lazy import functions with consistent error messaging
   - [x] Add TYPE_CHECKING imports for type annotations

2. [x] **Fix existing importlib usage**
   - [x] Fix `importlib.util` usage in `common/misc.py` (line 153, 486)
   - [x] Ensure consistent importlib patterns across codebase

### Phase 2: Common Modules Refactoring

- [ ] **Phase 2: Common Modules Refactoring**

3. [x] **Refactor common/types.py**
   - [x] Remove unconditional imports of polars, pandas, pyarrow
   - [x] Implement lazy loading for all functions
   - [x] Add availability checks and error handling
   - [x] Maintain backward compatibility for existing imports

4. [ ] **Refactor common/polars.py**
   - [ ] Add conditional imports for polars
   - [ ] Implement lazy loading pattern
   - [ ] Add error handling for missing polars

5. [x] **Refactor common/datetime.py**
   - [x] Remove unconditional polars and pyarrow imports
   - [x] Implement conditional loading for datetime utilities
   - [x] Add graceful fallbacks when dependencies missing

6. [x] **Update common/__init__.py**
   - [x] Review and update imports based on refactored modules
   - [x] Ensure conditional imports work properly

### Phase 3: Dataset Modules Refactoring

- [x] **Phase 3: Dataset Modules Refactoring**

7. [x] **Refactor datasets/pyarrow.py**
   - [x] Remove unconditional polars import
   - [x] Implement conditional loading for polars features
   - [x] Maintain core PyArrow functionality without polars

8. [x] **Refactor datasets/duckdb.py**
   - [x] Add conditional imports for duckdb and pyarrow
   - [x] Implement lazy loading with proper error handling
   - [x] Add feature detection for available capabilities

9. [x] **Update datasets/__init__.py**
   - [x] Implement conditional submodule imports
   - [x] Add feature detection capabilities
   - [x] Ensure graceful handling of missing dependencies

### Phase 4: Core and SQL Modules

- [ ] **Phase 4: Core and SQL Modules**

10. [ ] **Refactor core/ext.py**
     - [ ] Review and fix existing conditional import patterns
     - [ ] Ensure consistent error messaging
     - [ ] Fix any remaining unconditional imports

11. [ ] **Refactor core/merge.py**
     - [ ] Add conditional pyarrow import
     - [ ] Implement lazy loading pattern

12. [ ] **Refactor sql/filters/__init__.py**
     - [ ] Add conditional pyarrow imports
     - [ ] Implement lazy loading for filter functions

### Phase 5: Testing and Validation

- [ ] **Phase 5: Testing and Validation**

13. [ ] **Create comprehensive tests**
     - [ ] Test imports with only base dependencies
     - [ ] Test imports with all optional dependencies
     - [ ] Test error messages and guidance
     - [ ] Test functionality with partial dependencies

14. [ ] **Update existing tests**
     - [ ] Modify tests to handle conditional imports
     - [ ] Add tests for error conditions
     - [ ] Ensure backward compatibility

15. [ ] **Documentation updates**
     - [ ] Update installation guides
     - [ ] Document new import patterns
     - [ ] Add migration guide for existing users

### Phase 6: Validation and Cleanup

- [ ] **Phase 6: Validation and Cleanup**

16. [ ] **Import performance testing**
     - [ ] Measure import time improvements
     - [ ] Validate lazy loading performance
     - [ ] Ensure no regressions

17. [ ] **Final validation**
     - [ ] Test all import scenarios
     - [ ] Validate error messages
     - [ ] Ensure backward compatibility
     - [ ] Run full test suite

18. [ ] **Code cleanup**
     - [ ] Remove any remaining unconditional imports
     - [ ] Ensure consistent patterns across all modules
     - [ ] Final code review and polish

## Dependencies and Parallel Work

- **Tasks 1-2** can be done in parallel (core infrastructure)
- **Tasks 3-6** depend on task 1 (common modules)
- **Tasks 7-9** can be done in parallel after task 1 (dataset modules)
- **Tasks 10-12** can be done in parallel after task 1 (core/sql modules)
- **Tasks 13-15** depend on previous refactoring tasks
- **Tasks 16-18** are final validation and cleanup

## Validation Criteria

- All modules import successfully with only base dependencies
- Clear error messages when optional features are used without dependencies
- No performance regressions in import time
- Full functionality preserved when all dependencies are available
- Comprehensive test coverage for all scenarios
- Backward compatibility maintained for existing code
