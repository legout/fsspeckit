# Change: Update Recommended DuckDB Version in Documentation

## Why

The project documentation currently recommends outdated DuckDB versions that do not match the actual dependencies in `pyproject.toml`. This creates a mismatch between user expectations and the actual requirements, leading to:

1. **User confusion**: Installing the recommended version from docs may not work with all features
2. **Inconsistent requirements**: pyproject.toml requires `duckdb>=1.4.0` but docs say `>=0.9.0` or `>=0.8.0`
3. **Missing feature access**: Users may not get the performance and reliability benefits of DuckDB 1.4.0+ MERGE support

## What Changes

Update all documentation files to accurately reflect the minimum DuckDB version requirements:

1. **README.md** (line ~342): Change `duckdb>=0.9.0` to `duckdb>=1.4.0`
2. **docs/reference/api-guide.md** (line ~288): Change `duckdb>=0.9.0` to `duckdb>=1.4.0`
3. **examples/requirements.txt** (line 10): Change `duckdb>=0.8.0` to `duckdb>=1.4.0`

### Version Context

| Location | Current (Outdated) | Updated (Correct) |
|----------|-------------------|-------------------|
| README.md | `duckdb>=0.9.0` | `duckdb>=1.4.0` |
| api-guide.md | `duckdb>=0.9.0` | `duckdb>=1.4.0` |
| examples/requirements.txt | `duckdb>=0.8.0` | `duckdb>=1.4.0` |
| pyproject.toml | `duckdb>=1.4.0` | `duckdb>=1.4.0` (no change) |

## Impact

### Affected Files
- `README.md` - Main installation instructions
- `docs/reference/api-guide.md` - API reference documentation
- `examples/requirements.txt` - Example dependencies

### Breaking Changes
- **None**: This change aligns documentation with existing behavior
- Users who followed old recommendations will get a compatible version (just an older major version)

### User Benefits
1. **Accurate expectations**: Users know exactly which DuckDB version to install
2. **Feature access**: Users automatically get MERGE support and other 1.4.0+ features
3. **Performance**: Users benefit from DuckDB 1.4.0+ optimizations

### Maintenance
- **Reduced confusion**: No discrepancy between docs and actual requirements
- **Easier support**: Support questions about version compatibility reduced

## Risks / Trade-offs

### Risks
1. **Documentation lag**: Users with frozen dependencies may encounter issues
   - **Mitigation**: Document the reasoning (MERGE support in 1.4.0+)

### Trade-offs
1. **Documentation update effort**: Small change across 3 files
   - **Benefit**: Improved accuracy, reduced user confusion

## Related Work

- **Active**: `feature-duckdb-merge-sql` - Implements MERGE support requiring DuckDB 1.4.0+
- **Completed**: Documentation specs in `project-docs` capability
