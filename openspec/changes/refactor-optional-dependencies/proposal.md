# refactor-optional-dependencies Proposal

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
- All existing functionality remains unchanged when dependencies are available
- Core functionality works when only base dependencies are installed
- Clear error messages guide users to install required optional dependencies
- Test coverage for both scenarios (with and without optional deps)