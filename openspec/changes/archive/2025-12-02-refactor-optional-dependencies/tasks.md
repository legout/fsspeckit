/# Tasks: refactor-optional-dependencies

## Progress Overview

**Overall Progress:** 18/18 tasks completed (100%) ✅

**Phase Progress:**
- Phase 1: Core Infrastructure - 2/2 tasks completed ✅
- Phase 2: Common Modules Refactoring - 4/4 tasks completed ✅
- Phase 3: Dataset Modules Refactoring - 3/3 tasks completed ✅
- Phase 4: Core and SQL Modules - 3/3 tasks completed ✅
- Phase 5: Testing and Validation - 3/3 tasks completed ✅
- Phase 6: Validation and Cleanup - 3/3 tasks completed ✅

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

- [x] **Phase 2: Common Modules Refactoring**

3. [x] **Refactor common/types.py**
   - [x] Remove unconditional imports of polars, pandas, pyarrow
   - [x] Implement lazy loading for all functions
   - [x] Add availability checks and error handling
   - [x] Maintain backward compatibility for existing imports

4. [x] **Refactor common/polars.py**
   - [x] Add conditional imports for polars
   - [x] Implement lazy loading pattern
   - [x] Add error handling for missing polars

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

- [x] **Phase 4: Core and SQL Modules**

10. [x] **Refactor core/ext.py**
     - [x] Review and fix existing conditional import patterns
     - [x] Ensure consistent error messaging
     - [x] Fix any remaining unconditional imports

11. [x] **Refactor core/merge.py**
     - [x] Add conditional pyarrow import
     - [x] Implement lazy loading pattern

12. [x] **Refactor sql/filters/__init__.py**
     - [x] Add conditional pyarrow imports
     - [x] Implement lazy loading for filter functions

### Phase 5: Testing and Validation

- [x] **Phase 5: Testing and Validation**

13. [x] **Create comprehensive tests**
     - [x] Test imports with only base dependencies
     - [x] Test imports with all optional dependencies
     - [x] Test error messages and guidance
     - [x] Test functionality with partial dependencies
     - [x] Created `tests/test_optional_dependencies.py` with 28 passing tests

14. [x] **Update existing tests**
     - [x] Modify tests to handle conditional imports
     - [x] Add tests for error conditions
     - [x] Ensure backward compatibility
     - [x] Fixed import paths in 10 test files

15. [x] **Documentation updates**
     - [x] Update installation guides
     - [x] Document new import patterns
     - [x] Add migration guide for existing users
     - [x] Updated `docs/installation.md`, `README.md`, `docs/api-guide.md`, `MIGRATION_GUIDE.md`

### Phase 6: Validation and Cleanup

- [x] **Phase 6: Validation and Cleanup**

16. [x] **Run full test suite**
     - [x] Measure import time improvements
     - [x] Validate lazy loading performance
     - [x] Ensure no regressions
     - [x] Result: 334 passing tests, 55 pre-existing failures unrelated to refactoring

17. [x] **Final validation**
     - [x] Test all import scenarios
     - [x] Validate error messages
     - [x] Ensure backward compatibility
     - [x] Run full test suite
     - [x] Validated backward compatibility via `fsspeckit.utils`
     - [x] Updated `MIGRATION_GUIDE.md`

18. [x] **Code cleanup and validation script**
     - [x] Remove any remaining unconditional imports
     - [x] Ensure consistent patterns across all modules
     - [x] Final code review and polish
     - [x] Created `validate_refactoring.py` with 6 comprehensive tests (all passing)
     - [x] Created `REFACTORING_SUMMARY.md` documentation

## Dependencies and Parallel Work

- **Tasks 1-2** can be done in parallel (core infrastructure)
- **Tasks 3-6** depend on task 1 (common modules)
- **Tasks 7-9** can be done in parallel after task 1 (dataset modules)
- **Tasks 10-12** can be done in parallel after task 1 (core/sql modules)
- **Tasks 13-15** depend on previous refactoring tasks
- **Tasks 16-18** are final validation and cleanup

## Validation Criteria

- ✅ All modules import successfully with only base dependencies
- ✅ Clear error messages when optional features are used without dependencies
- ✅ No performance regressions in import time
- ✅ Full functionality preserved when all dependencies are available
- ✅ Comprehensive test coverage for all scenarios (28 new tests, 334 total passing)
- ✅ Backward compatibility maintained for existing code

## Final Status

**All tasks completed successfully! ✅**

- Created centralized lazy import system in `src/fsspeckit/common/optional.py`
- Refactored 14 modules to use lazy imports
- Created comprehensive test suite (28 new tests)
- Updated all documentation
- Created validation script (all 6 tests passing)
- Full backward compatibility maintained via `fsspeckit.utils` facade
- Test results: 334 passing, 55 pre-existing failures unrelated to refactoring

See `REFACTORING_SUMMARY.md` for complete details.
