# Spec Delta: Project Docs

## MODIFIED Requirements

### Requirement: Documentation reflects consolidated path normalization

Project documentation SHALL be updated to reference the unified `normalize_path()` function and provide clear examples of different usage patterns.

#### Scenario: Core paths module documentation updated
- **WHEN** developers review documentation for `core/filesystem/paths.py`
- **THEN** the `normalize_path()` function SHALL have comprehensive docstrings
- **AND** SHALL include examples for string-only normalization
- **AND** SHALL include examples for filesystem-aware normalization
- **AND** SHALL include examples for normalization with validation

#### Scenario: Deprecation warnings documented
- **WHEN** the `_normalize_path()` function is kept as a deprecated alias
- **THEN** the documentation SHALL clearly indicate it is deprecated
- **AND** SHALL point to the new `normalize_path()` function
- **AND** SHALL explain the migration path
