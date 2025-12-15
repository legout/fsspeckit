## Context

The `refactor-module-layout-packages` change successfully introduced domain-driven package structure but has critical implementation issues that prevent the package from functioning at all. The core problems are:

1. **Circular Import**: `core.filesystem.__init__.py` line 44 has `from . import ext` which should be `from .. import ext`
2. **Layering Violation**: `core.ext.parquet` imports from `datasets.pyarrow`, violating the architectural rule that core should not depend on higher-level packages
3. **Code Duplication**: Significant duplication exists between `common.schema` and `datasets.pyarrow.schema` for the same functions
4. **Incomplete Migration**: Some tests still use old `fsspeckit.utils` imports
5. **Missing Documentation**: No migration guide exists for users transitioning from old to new import structure

The circular import is particularly severe as it prevents any imports from working, effectively breaking the entire package.

## Goals / Non-Goals

- **Goals**:
  - Fix circular import to restore basic functionality
  - Eliminate layering violations to establish clean architectural boundaries
  - Remove code duplication to improve maintainability
  - Complete migration of tests to new domain packages
  - Provide clear migration documentation for users
  - Strengthen architectural enforcement to prevent future violations

- **Non-Goals**:
  - No behavioral changes to any public APIs
  - No removal of backwards compatibility (utils façade remains)
  - No changes to external dependency footprint
  - No breaking changes for existing users

## Decisions

- **Option A for Layering**: Move shared PyArrow utilities from `datasets.pyarrow.schema` to `common.schema`
  - This maintains clean layering where core can depend on common but not on datasets
  - Eliminates code duplication by establishing canonical implementations
  - Preserves all existing behavior while fixing architectural violations

- **Phased Implementation**: Address critical fixes first, then architectural improvements
  - Phase 1: Unblock functionality (circular import, layering fix)
  - Phase 2: Cleanup and migration (deduplication, test updates, docs)
  - Phase 3: Enhancement (CI enforcement, documentation refinement)

- **Preserve Utils Façade**: Keep `fsspeckit.utils` as backwards-compatible re-export layer
  - No breaking changes for existing users
  - Gradual migration path encouraged but not required
  - Maintain all existing import patterns

## Alternatives considered

- **Option B: Duplicate functions in core**: Keep utilities in datasets but duplicate minimal versions in core
  - Pros: Maintains conceptual ownership by datasets
  - Cons: Code duplication, maintenance overhead, potential divergence

- **Option C: Document layering exception**: Allow core→datasets import as documented exception
  - Pros: No code changes needed
  - Cons: Weakens architectural rules, creates confusing precedent

- **Hotfix-only approach**: Fix only circular import, leave other issues for later
  - Pros: Minimal risk, fastest unblock
  - Cons: Misses opportunity to fix architectural debt, requires additional work later

Option A with phased implementation provides the best balance of immediate unblocking and long-term architectural health.

## Risks / Trade-offs

- **Import churn risk**: Moving functions may affect internal imports
  - Mitigation: Careful import analysis and comprehensive testing
  - Trade-off: Accept short-term churn for long-term architectural clarity

- **Complexity risk**: Multi-phase approach increases coordination complexity
  - Mitigation: Clear phase boundaries and validation points
  - Trade-off: Accept coordination overhead for risk mitigation

- **Compatibility risk**: Moving functions might break some import patterns
  - Mitigation: Maintain utils façade, thorough backwards compatibility testing
  - Trade-off: Accept internal complexity for external stability

- **Documentation drift risk**: Code and docs might become inconsistent during migration
  - Mitigation: Update docs alongside code changes
  - Trade-off: Accept documentation maintenance overhead for accuracy

## Migration Plan

### Phase 1: Critical Fixes (Immediate)
1. Fix circular import in `core.filesystem.__init__.py`
2. Move schema utilities from `datasets.pyarrow.schema` to `common.schema`
3. Update imports in `core.ext.parquet` and `datasets.pyarrow`
4. Validate basic functionality works

### Phase 2: Cleanup and Migration (High Priority)
5. Remove duplicate code from `datasets.pyarrow.schema`
6. Update remaining test imports to use domain packages
7. Create migration documentation
8. Run comprehensive test suite

### Phase 3: Enhancement (Medium Priority)
9. Add import layering checks to CI
10. Update package documentation
11. Add architectural decision records
12. Final validation and documentation review

Each phase includes validation gates before proceeding to next phase.

## Open Questions

1. **Deprecation timeline**: Should we plan eventual deprecation of utils façade (e.g., in v2.0) or keep it indefinitely?
2. **CI enforcement strictness**: Should layering violations be warnings or errors in CI?
3. **Testing scope**: Should we add automated tests specifically for layering rule compliance?
4. **Documentation format**: Should migration guide be in MkDocs format or separate markdown file?