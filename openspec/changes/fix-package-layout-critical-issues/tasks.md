## 1. Critical Fixes (Phase 1)

- [ ] 1.1 Fix circular import in `src/fsspeckit/core/filesystem/__init__.py:44`
  - Change `from . import ext` to `from .. import ext`
  - Verify that filesystem imports work correctly
  - Test basic package import functionality

- [ ] 1.2 Move shared PyArrow utilities from `datasets.pyarrow.schema` to `common.schema`
  - Move functions: `cast_schema`, `opt_dtype`, `unify_schemas`, `convert_large_types_to_normal`
  - Preserve more comprehensive implementation from `common.schema` as canonical
  - Update function signatures and docstrings if needed

- [ ] 1.3 Update imports in `core.ext.parquet` to use `common.schema`
  - Replace imports from `fsspeckit.datasets.pyarrow` with `fsspeckit.common.schema`
  - Verify that all parquet I/O functions work correctly
  - Test that core layering rules are satisfied

- [ ] 1.4 Update imports in `datasets.pyarrow` to use `common.schema`
  - Update `datasets/pyarrow/__init__.py` to import from `common.schema`
  - Update `datasets/pyarrow/schema.py` to import from `common.schema`
  - Ensure re-exports work correctly for backwards compatibility

- [ ] 1.5 Validate Phase 1 completion
  - Test that `import fsspeckit` works without errors
  - Test that domain packages import correctly
  - Verify that utils façade still works
  - Run basic functionality tests

## 2. Code Cleanup and Migration (Phase 2)

- [ ] 2.1 Remove duplicate code from `datasets.pyarrow.schema`
  - Remove moved functions from `datasets/pyarrow/schema.py`
  - Remove duplicate regex patterns and constants
  - Clean up any unused imports
  - Ensure module still exports needed functions via re-exports

- [ ] 2.2 Update remaining test imports to use domain packages
  - Update `tests/test_basic.py` to use `fsspeckit.common.logging`
  - Update `tests/test_utils/test_duckdb.py` to use `fsspeckit.datasets`
  - Update `tests/test_utils/test_utils_backwards_compat.py` imports
  - Preserve backwards compatibility tests for utils façade

- [ ] 2.3 Create migration documentation
  - Create `docs/how-to/migrate-package-layout.md`
  - Document old→new import mappings with examples
  - Provide timeline and deprecation guidance
  - Include troubleshooting section for common issues

- [ ] 2.4 Update API reference documentation
  - Update `docs/api/fsspeckit.common.md` if needed
  - Update `docs/api/fsspeckit.datasets.md` with new structure
  - Update `docs/api/fsspeckit.sql.md` if needed
  - Ensure all new modules are properly documented

- [ ] 2.5 Validate Phase 2 completion
  - Run full test suite and ensure all tests pass
  - Test that both old and new import patterns work
  - Verify documentation builds correctly
  - Check that no regressions were introduced

## 3. Architectural Enhancements (Phase 3)

- [ ] 3.1 Add import layering checks to CI
  - Create script or ruff rule to check import layering compliance
  - Add check to CI configuration (GitHub Actions)
  - Configure to fail on layering violations
  - Add documentation for the check

- [ ] 3.2 Update package boundary documentation
  - Add package-level documentation explaining scope and boundaries
  - Update `CONTRIBUTING.md` with layering rules
  - Create architectural decision record (ADR) for layering
  - Document import patterns and best practices

- [ ] 3.3 Add tests for layering rule compliance
  - Create tests that verify core doesn't import from datasets/sql
  - Add tests for utils façade functionality
  - Test that domain packages have correct dependencies
  - Ensure backwards compatibility is maintained

- [ ] 3.4 Final validation and cleanup
  - Run comprehensive test suite including new tests
  - Validate that all imports follow layering rules
  - Check documentation for accuracy and completeness
  - Verify performance characteristics (import times, etc.)

## 4. Validation and Quality Assurance

- [ ] 4.1 Run openspec validation
  - Execute `openspec validate fix-package-layout-critical-issues --strict`
  - Fix any validation issues found
  - Ensure all requirements are satisfied

- [ ] 4.2 Test backwards compatibility
  - Test that all existing import patterns still work
  - Verify that utils façade re-exports correctly
  - Check that no public APIs were broken
  - Test with example code and documentation

- [ ] 4.3 Performance validation
  - Benchmark import times before and after changes
  - Check that no performance regressions were introduced
  - Validate that code deduplication improves memory usage
  - Test that layering checks don't slow down CI significantly

- [ ] 4.4 Documentation validation
  - Build documentation and check for warnings/errors
  - Verify that migration guide is accurate and helpful
  - Test that API reference includes all new modules
  - Ensure that all links and references work correctly