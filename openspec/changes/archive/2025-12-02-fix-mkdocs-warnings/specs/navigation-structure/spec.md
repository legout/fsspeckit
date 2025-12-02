# Navigation Structure Specification

## MODIFIED Requirements

### Requirement: Remove references to non-existent API files

The navigation structure in mkdocs.yml MUST only reference files that actually exist. Any references to non-existent API documentation files SHALL be removed to eliminate build warnings about missing files and ensure users don't encounter broken navigation links.
#### Scenario:
- When building documentation, navigation should only reference existing files
- Given current navigation references missing files like `api/fsspeckit.core.filesystem.md`
- When mkdocs processes navigation, it should not emit warnings about missing files
- Then all navigation links should resolve to actual documentation files
- And users should not encounter broken links in the documentation site

### Requirement: Include existing fsspeckit.utils API files

The navigation structure MUST include all existing fsspeckit.utils API documentation files to maintain backwards compatibility and eliminate warnings about existing files not being included in the navigation configuration.
#### Scenario:
- When users navigate API documentation, backwards compatibility files should be accessible
- Given API files exist for `fsspeckit.utils.*` modules (datetime, logging, misc, polars, pyarrow, sql, types)
- When building documentation, these files should be included in navigation structure
- Then users can access documentation for backwards compatibility utils
- And navigation warnings about existing files not being included should be eliminated

### Requirement: Fix broken internal links

All internal links within documentation files MUST resolve correctly. Broken links SHALL be identified and fixed to ensure proper navigation and user experience when browsing the documentation site.
#### Scenario:
- When users click internal links in documentation, they should resolve correctly
- Given utils.md contains link `../MIGRATION_GUIDE.md` which points outside docs directory
- Given api/index.md contains links to non-existent domain package files
- When users navigate the documentation site, all internal links should work
- Then MIGRATION_GUIDE.md link should point to correct location
- And API index links should only reference existing files

## ADDED Requirements

### Requirement: Temporary navigation structure for missing domain package docs

The navigation structure SHALL reflect the current reality of available documentation files, temporarily excluding references to domain package API files that haven't been generated yet, while maintaining the structure for future inclusion.
#### Scenario:
- When domain package API files are not yet generated, navigation should be clean
- Given current package refactoring is complete but API documentation generation is pending
- When building documentation, navigation should reflect current reality
- Then non-existent domain package files should be temporarily excluded from navigation
- And structure can be expanded later when API files are generated