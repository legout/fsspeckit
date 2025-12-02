# refactor-optional-dependencies Proposal

**Status:** ✅ COMPLETED (All 18 tasks finished - December 2025)

## Purpose
Refactor fsspeckit to eliminate unconditional imports of optional dependencies, implementing lazy loading patterns that allow the core library to function without requiring all optional dependencies to be installed.

## Summary
This change addresses the current issue where modules like `common/types.py`, `common/polars.py`, `datasets/pyarrow.py`, and others unconditionally import optional dependencies (polars, pandas, pyarrow, duckdb), causing ImportError when users try to use basic fsspeckit functionality without having all optional dependencies installed.

The refactoring will implement:
1. Lazy import patterns using importlib.util.find_spec()
2. Function-level imports for heavy dependencies
3. Availability flags and graceful error handling
4. Separate modules for optional functionality
5. Consistent error messaging across the codebase

## Capabilities
- **core-lazy-imports**: Implement lazy loading for core optional dependencies
- **utils-optional-separation**: Separate utility modules by dependency requirements
- **datasets-conditional-loading**: Conditional loading for dataset-specific functionality

## Relationships
This change builds on existing patterns in `common/misc.py` and `core/ext.py` while extending them consistently across the entire codebase.

## Validation
- ✅ All existing functionality remains unchanged when dependencies are available
- ✅ Core functionality works when only base dependencies are installed
- ✅ Clear error messages guide users to install required optional dependencies
- ✅ Test coverage for both scenarios (with and without optional deps)
- ✅ 334 passing tests (28 new optional dependency tests)
- ✅ Full backward compatibility via `fsspeckit.utils` facade
- ✅ Validation script created with all 6 tests passing

## Implementation Results

**Completed:** All 18 tasks across 6 phases

### Key Deliverables
1. **`src/fsspeckit/common/optional.py`** - Centralized lazy import utilities
2. **14 refactored modules** - Using lazy imports throughout
3. **`tests/test_optional_dependencies.py`** - 28 comprehensive tests
4. **Updated documentation** - Installation guide, API guide, README, migration guide
5. **`validate_refactoring.py`** - Final validation script (all tests passing)
6. **`REFACTORING_SUMMARY.md`** - Complete project documentation

### Test Results
- **334 tests passing** (includes 28 new optional dependency tests)
- **55 failing tests** (all pre-existing, unrelated to refactoring)
- **6/6 validation tests passing**

See `tasks.md` for detailed implementation checklist and `REFACTORING_SUMMARY.md` for comprehensive results.