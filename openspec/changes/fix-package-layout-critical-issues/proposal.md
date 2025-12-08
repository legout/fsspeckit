# Change: Fix Package Layout Critical Issues and Architectural Refinement

## Why

The `refactor-module-layout-packages` implementation has critical blocking issues that prevent the package from functioning:

1. **CRITICAL**: Circular import in `core.filesystem.__init__.py` prevents any imports from working
2. **HIGH**: Layering violation where `core.ext.parquet` imports from `datasets.pyarrow` 
3. **ARCHITECTURAL**: Significant code duplication between `common.schema` and `datasets.pyarrow.schema`
4. **MEDIUM**: Incomplete test migration to new domain packages
5. **LOW**: Missing migration guide documentation

These issues undermine the architectural benefits of the original refactor and must be addressed to restore functionality and achieve the intended clean separation of concerns.

## What Changes

### Phase 1: Critical Fixes (Unblock Functionality)

1. **Fix circular import**:
   - Correct `from . import ext` to `from .. import ext` in `core.filesystem.__init__.py:44`
   - This immediately restores all import functionality

2. **Resolve layering violation using Option A**:
   - Move shared PyArrow utilities from `datasets.pyarrow.schema` to `common.schema`
   - Functions to move: `cast_schema`, `opt_dtype`, `unify_schemas`, `convert_large_types_to_normal`
   - Update `datasets.pyarrow` to import from `common.schema`
   - Update `core.ext.parquet` to import from `common.schema`

### Phase 2: Architectural Refinement

3. **Consolidate schema utilities**:
   - Merge duplicate implementations between `common.schema` and `datasets.pyarrow.schema`
   - Keep the more comprehensive implementation from `common.schema` as canonical
   - Remove duplicate code from `datasets.pyarrow.schema`
   - Ensure all backends delegate to `common.schema` for consistency

4. **Complete test migration**:
   - Update remaining `fsspeckit.utils` imports in tests to use domain packages
   - Files to update: `test_basic.py`, `test_utils/test_duckdb.py`, `test_utils/test_utils_backwards_compat.py`
   - Preserve backwards compatibility tests to ensure utils façade still works
   - Add tests for new import paths and layering rules

5. **Add migration documentation**:
   - Create `docs/how-to/migrate-package-layout.md`
   - Document old→new import mappings with examples
   - Provide timeline and deprecation guidance
   - Update API reference documentation

### Phase 3: Architectural Enhancements

6. **Strengthen layering enforcement**:
   - Add static import analysis to CI (ruff rule or custom script)
   - Document layering rules in `CONTRIBUTING.md`
   - Add architectural decision record (ADR) for layering

7. **Refine package boundaries**:
   - Add package-level documentation explaining scope and boundaries
   - Ensure consistent error handling patterns across packages
   - Validate that all imports follow layering rules

## Impact

- **Critical**: Fixes blocking circular import, restoring all functionality
- **High**: Eliminates architectural layering violations, establishing clean separation
- **Medium**: Removes code duplication, improving maintainability and reducing bugs
- **Low**: Improves developer experience with clear migration path and documentation
- **Zero breaking changes**: All existing imports continue to work via utils façade
- **Performance**: Slight improvement due to reduced import overhead and code deduplication

## Affected Specs

- `project-architecture`: Strengthening layering rules and enforcement mechanisms
- `utils-pyarrow`: Moving schema utilities to common (behavior preserved, location changed)
- `utils-duckdb`: No changes (already uses common.schema correctly)
- `project-docs`: Adding migration guide and package boundary documentation

## Implementation Plan

### Phase 1: Critical Fixes (Priority: IMMEDIATE)
1. Fix circular import in `core.filesystem.__init__.py`
2. Move schema utilities from `datasets.pyarrow.schema` to `common.schema`
3. Update imports in `core.ext.parquet` and `datasets.pyarrow`
4. Verify basic imports work with simple test

### Phase 2: Cleanup and Migration (Priority: HIGH)
5. Remove duplicate code from `datasets.pyarrow.schema`
6. Update remaining test imports to use domain packages
7. Add migration documentation
8. Run full test suite to ensure no regressions

### Phase 3: Enhancement (Priority: MEDIUM)
9. Add import layering checks to CI configuration
10. Update package documentation with boundary explanations
11. Add architectural decision record for layering rules
12. Final validation and documentation updates

## Risk Mitigation

- **Functionality risk**: Test imports at each phase to ensure no regressions
- **Compatibility risk**: Maintain utils façade throughout, add backwards compatibility tests
- **Complexity risk**: Phase-based approach with clear validation points between phases
- **Documentation risk**: Update docs alongside code changes to prevent drift
- **Performance risk**: Benchmark import times before/after to ensure no degradation

## Success Criteria

1. All imports work without circular import errors
2. Core modules do not import from datasets or sql packages
3. No duplicate schema utility implementations remain
4. All tests pass with new import structure
5. Migration guide is complete and accurate
6. CI enforces layering rules automatically
7. Package boundaries are clearly documented

## Open Questions

1. Should we plan eventual deprecation of utils façade, or keep it indefinitely?
2. How strict should import layering checks be (warning vs error in CI)?
3. Should we add automated tests for layering rule compliance?