# Implementation Tasks

## Configuration Fixes
- [x] Remove deprecated `pypi_url` configuration from mkdocs.yml
- [x] Update mkdocstrings options structure to remove deprecated extra options
- [x] Remove deprecated options that caused warnings

## Navigation Structure Updates
- [x] Add existing `fsspeckit.utils.*` files to navigation (backwards compatibility)
- [x] Remove navigation references to non-existent domain package API files
- [x] Restructure navigation to reflect actual file structure

## API Reference Management
- [x] Identify which API files need to be generated vs excluded
- [x] Temporarily exclude missing domain package files from navigation
- [x] Add explanatory note about future API documentation

## Link Fixes
- [x] Fix broken `../MIGRATION_GUIDE.md` reference in utils.md (updated to GitHub URL)
- [x] Update API index internal links to match existing files
- [x] Validate and fix any other broken internal links

## Testing and Validation
- [x] Run `uv run mkdocs build` and verify zero warnings ✅
- [x] Test local development server navigation (optional)
- [x] Validate all internal links resolve correctly
- [x] Confirm documentation site renders properly

## Final Review
- [x] Review changes against OpenSpec proposal
- [x] Verify all success criteria are met
- [x] Document any deviations from original plan
