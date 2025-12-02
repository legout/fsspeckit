# Fix MkDocs Build Warnings

## Summary

Address configuration and navigation warnings in MkDocs build to ensure clean documentation generation and proper API reference structure.

## Why

Clean documentation builds are essential for maintaining professional development standards and ensuring reliable CI/CD pipelines. Warning-free builds help developers quickly identify real issues, improve developer experience, and ensure documentation site reliability. This is particularly important for the fsspeckit project which has recently undergone package refactoring and needs documentation that accurately reflects the current codebase structure.

## Problem Statement

Current MkDocs build produces multiple warnings:

1. **Configuration Issues**:
   - `pypi_url`: Unrecognised configuration name
   - Deprecated `options` structure for mkdocstrings (needs `options.extra`)

2. **Navigation Mismatches**:
   - Missing API reference files for new domain packages
   - Orphaned `fsspeckit.utils.*` files that exist but aren't in nav
   - Broken internal links to non-existent documentation files

## Root Causes

1. **Legacy Configuration**: Using deprecated mkdocstrings options format
2. **Outdated Navigation**: Navigation structure references files that don't exist for new domain packages
3. **Missing API Generation**: Auto-generated API files for new packages haven't been created
4. **Broken Links**: Documentation contains links to removed/renamed files

## Proposed Solution

### Phase 1: Configuration Fixes
- Remove deprecated `pypi_url` configuration
- Update mkdocstrings options to use `options.extra` structure
- Move deprecated options to proper location

### Phase 2: Navigation Structure Updates
- Remove references to non-existent API files
- Add references to existing `fsspeckit.utils.*` files (backwards compatibility)
- Restructure navigation to reflect current package layout

### Phase 3: API Reference Generation
- Generate missing API reference files for domain packages
- Ensure all documented packages have corresponding API files
- Update internal links to point to correct locations

### Phase 4: Link Fixes
- Fix broken `../MIGRATION_GUIDE.md` reference in utils.md
- Update API index links to match actual file structure
- Validate all internal links resolve correctly

## Implementation Plan

### Tasks
- [ ] Remove `pypi_url` from mkdocs.yml (deprecated config)
- [ ] Restructure mkdocstrings options to use `options.extra` format
- [ ] Update navigation to include existing `fsspeckit.utils.*` files
- [ ] Remove navigation references to non-existent domain package API files
- [ ] Generate missing API reference files or temporarily exclude from nav
- [ ] Fix broken `../MIGRATION_GUIDE.md` link in utils.md
- [ ] Update API index internal links to match existing files
- [ ] Validate mkdocs build runs without warnings
- [ ] Test documentation site navigation works correctly

## Success Criteria

1. **Clean Build**: MkDocs build completes without warnings
2. **Valid Navigation**: All navigation links resolve to existing files
3. **Working Site**: Documentation site renders correctly with proper navigation
4. **No Broken Links**: All internal links resolve to valid targets
5. **Backwards Compatibility**: Existing fsspeckit.utils documentation remains accessible

## Impact Assessment

- **Risk**: Low - configuration changes only affect documentation build
- **Scope**: Documentation build system and navigation structure
- **Users**: Improves developer experience with cleaner build output
- **CI/CD**: May fix any failing documentation builds in pipelines

## Dependencies

- MkDocs and mkdocstrings package versions
- Auto-generated API documentation tools
- No external dependencies

## Timeline

**Estimated Effort**: 1-2 hours
- Configuration fixes: 15 minutes
- Navigation updates: 30 minutes
- Link fixes: 30 minutes
- Testing and validation: 15 minutes

## Testing Plan

1. Run `uv run mkdocs build` and verify zero warnings
2. Check all navigation links resolve correctly
3. Validate internal links work in generated site
4. Test local development server navigation
5. Verify documentation deployment pipeline (if applicable)