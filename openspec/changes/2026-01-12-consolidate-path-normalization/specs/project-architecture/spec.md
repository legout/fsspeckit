# Spec Delta: Project Architecture

## MODIFIED Requirements

### Requirement: Core filesystem path handling is explicit and deterministic

Core filesystem factories SHALL handle local vs remote paths explicitly using the unified `normalize_path()` helper from `fsspeckit.core.filesystem.paths`.

#### Scenario: Unified path normalization helper is used
- **WHEN** a caller passes a local file or directory path to `fsspeckit.core.filesystem.filesystem()`
- **THEN** the factory SHALL normalize the path using `normalize_path()` from `fsspeckit.core.filesystem.paths`
- **AND** the resulting `DirFileSystem` or base filesystem SHALL have a clear and predictable root
- **AND** the normalization SHALL be consistent across all modules in the codebase

#### Scenario: Path normalization is centralized
- **WHEN** any module in fsspeckit needs to normalize paths
- **THEN** it SHALL use the `normalize_path()` function from `fsspeckit.core.filesystem.paths`
- **AND** SHALL NOT implement custom path normalization logic
- **AND** SHALL benefit from consistent behavior across the entire codebase
