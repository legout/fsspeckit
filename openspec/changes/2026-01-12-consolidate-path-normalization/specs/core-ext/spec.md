# Spec Delta: Core Ext

## MODIFIED Requirements

### Requirement: Update call sites in core/ext modules

All modules in `core/ext/` that import or use path normalization functions SHALL be updated to use the unified `normalize_path()` function.

#### Scenario: Update imports in core/ext modules
- **WHEN** a module in `core/ext/` imports path normalization functions
- **THEN** it SHALL import `normalize_path` from `fsspeckit.core.filesystem.paths`
- **AND** SHALL NOT import `_normalize_path` or use the old function name

#### Scenario: Update function calls in core/ext modules
- **WHEN** code in `core/ext/` calls path normalization functions
- **THEN** it SHALL call `normalize_path()` with appropriate parameters
- **AND** SHALL maintain the same behavior as before the refactoring
- **AND** SHALL pass `filesystem` parameter if filesystem-aware normalization is needed
- **AND** SHALL pass `validate=True` if validation was previously performed
