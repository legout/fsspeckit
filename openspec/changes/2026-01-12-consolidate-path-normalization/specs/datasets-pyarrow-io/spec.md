# Spec Delta: Datasets PyArrow IO

## MODIFIED Requirements

### Requirement: PyarrowDatasetIO._normalize_path method

The `PyarrowDatasetIO._normalize_path()` method SHALL use the unified `normalize_path()` function from `core/filesystem/paths.py` instead of calling separate normalization and validation functions.

#### Scenario: Method calls unified normalize function
- **WHEN** `PyarrowDatasetIO._normalize_path(path, operation)` is called
- **THEN** the method SHALL call `normalize_path(path, filesystem=self._filesystem, validate=True, operation=operation)`
- **AND** SHALL NOT call `normalize_path` and `validate_dataset_path` separately
- **AND** SHALL return the result from the unified function

#### Scenario: Validation behavior preserved
- **WHEN** the method is called with `operation="read"`
- **THEN** the unified function SHALL perform the same validation as the previous implementation
- **AND** SHALL raise the same exceptions for invalid paths
- **AND** SHALL maintain identical behavior to the previous implementation
