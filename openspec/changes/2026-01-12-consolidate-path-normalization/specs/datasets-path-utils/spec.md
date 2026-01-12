# Spec Delta: Datasets Path Utils

## REMOVED Requirements

### Requirement: Duplicate normalize_path implementation

The `normalize_path()` function in `datasets/path_utils.py` SHALL be removed and replaced with an import from `core/filesystem/paths.py`.

#### Scenario: Function implementation removed from module
- **WHEN** the consolidated path normalization is implemented
- **THEN** the `normalize_path()` function implementation in `datasets/path_utils.py` SHALL be removed
- **AND** the module SHALL import `normalize_path` from `fsspeckit.core.filesystem.paths`

#### Scenario: validate_dataset_path remains unchanged
- **WHEN** path normalization consolidation is complete
- **THEN** the `validate_dataset_path()` function SHALL remain in `datasets/path_utils.py`
- **AND** SHALL continue to provide dataset-specific validation logic
- **AND** SHALL be callable independently of the normalization function
