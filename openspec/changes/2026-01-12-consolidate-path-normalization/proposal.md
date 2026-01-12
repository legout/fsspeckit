# Proposal: Consolidate Path Normalization

## Metadata

- **ID**: `2026-01-12-consolidate-path-normalization`
- **Status**: Draft
- **Created**: 2026-01-12
- **Type**: Refactoring

## Problem Statement

fsspeckit currently has **three different implementations** of path normalization with inconsistent behaviors:

1. **`core/filesystem/paths._normalize_path()`** - Low-level string utility
   - Handles URL-like paths (`://`) by splitting protocol
   - Converts backslashes to forward slashes
   - Uses `posixpath.normpath`
   - No filesystem awareness

2. **`datasets/path_utils.normalize_path()`** - Filesystem-aware normalizer
   - For local filesystem: uses `os.path.abspath`
   - For remote filesystems: preserves/adds protocol prefix
   - Takes a filesystem instance parameter

3. **`datasets/pyarrow/io.PyarrowDatasetIO._normalize_path()`** - Method wrapper
   - Calls `path_utils.normalize_path()`
   - Then calls `validate_dataset_path()`
   - Takes an `operation` parameter for context-aware validation

**Issues with current state:**
- **Inconsistent behavior**: `posixpath.normpath` vs `os.path.abspath` produce different results
- **Confusing API**: Users must choose which function to use
- **Maintenance burden**: Three places to fix bugs or add features
- **Documentation overhead**: Need to document three different behaviors

## Proposed Solution

Create a **single, unified `normalize_path()` function** in `core/filesystem/paths.py` that:

1. Handles both string-only and filesystem-aware normalization
2. Supports optional validation via parameters
3. Maintains backward compatibility with existing callers
4. Provides clear, predictable behavior based on parameters

### New API Design

```python
def normalize_path(
    path: str,
    filesystem: AbstractFileSystem | None = None,
    validate: bool = False,
    operation: str | None = None
) -> str:
    """Normalize a filesystem path.

    Args:
        path: Path to normalize
        filesystem: Optional filesystem for filesystem-aware normalization
        validate: Whether to validate path after normalization
        operation: Operation type for validation ('read', 'write', 'merge', etc.)

    Returns:
        Normalized path string

    Raises:
        ValueError: If path is invalid when validate=True
        DatasetPathError: If path validation fails for dataset operations
    """
```

### Behavior Matrix

| filesystem | validate | Behavior |
|------------|----------|----------|
| None | False | String-only: convert slashes, handle `://` URLs, `posixpath.normpath` |
| None | True | String-only + basic validation |
| LocalFileSystem | False | `os.path.abspath` for local paths |
| LocalFileSystem | True | `os.path.abspath` + filesystem validation |
| Remote (S3/GCS/etc) | False | Add protocol prefix if missing |
| Remote (S3/GCS/etc) | True | Protocol handling + filesystem validation |

## Goals

### Primary Goals
1. **Single source of truth** for path normalization logic
2. **Consistent behavior** across all modules
3. **Clear API** with explicit parameters for different use cases
4. **No breaking changes** to public API (backward compatible)

### Success Criteria
- All existing tests pass without modification
- All three implementations consolidated into one
- Clear migration path for internal callers
- Documentation updated with examples
- No performance regression

## Non-Goals

- Changing existing path normalization behavior (unless fixing bugs)
- Breaking backward compatibility for external users
- Adding new path normalization features
- Changing validation logic in `validate_dataset_path()`

## Impact Assessment

### Breaking Changes
**None** - Internal refactoring only. Public API remains unchanged.

### Affected Modules
- `core/filesystem/paths.py` - Add new function, deprecate old `_normalize_path()`
- `datasets/path_utils.py` - Remove duplicate implementation
- `datasets/pyarrow/io.py` - Update `_normalize_path()` method
- Any modules importing from above - Update imports

### Migration Strategy

1. **Phase 1**: Create new unified function in `core/filesystem/paths.py`
2. **Phase 2**: Keep old `_normalize_path()` as deprecated alias
3. **Phase 3**: Update all internal call sites
4. **Phase 4**: Remove deprecated alias (future PR)

### Performance Impact
- **Negligible**: Same operations, just organized differently
- **Potential improvement**: Single function may enable better optimization

### Testing Impact
- Add tests for new unified function
- Verify all existing tests still pass
- Add tests for edge cases (different filesystems, validation modes)

## Open Questions

1. **Deprecation timeline**: Should we remove `_normalize_path()` in next major version or keep indefinitely?
2. **Validation without filesystem**: What level of validation when `validate=True` but `filesystem=None`?
3. **Method vs inline**: Should `PyarrowDatasetIO._normalize_path()` remain as method or inline the call?

## Dependencies

- None (internal refactoring)

## Risks

- **Low risk**: Pure refactoring, no behavior changes
- **Mitigation**: Comprehensive test coverage, gradual migration

## Alternatives Considered

### Option 1: Keep all three implementations
**Rejected**: Maintenance burden, inconsistent behavior

### Option 2: Only consolidate path_utils and PyarrowDatasetIO, keep paths._normalize_path separate
**Rejected**: Doesn't solve inconsistency issue

### Option 3: Create new module for path utilities
**Rejected**: Unnecessary reorganization, `paths.py` already exists

## Timeline Estimate

- **Design & proposal**: 2 hours (completed)
- **Implementation**: 11 hours (see tasks.md breakdown)
- **Testing**: 4 hours
- **Documentation**: 2 hours
- **Total**: ~19 hours

## References

- Original analysis: https://github.com/anomalyco/opencode/issues/XXX
- Related spec: `project-architecture` - Core filesystem path handling requirement
