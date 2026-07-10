# Spec Delta: Core Filesystem Paths

## ADDED Requirements

### Requirement: Unified path normalization function

The system SHALL provide a single `normalize_path()` function in `core/filesystem/paths.py` that handles both string-only and filesystem-aware normalization with optional validation.

#### Scenario: String-only normalization without filesystem
- **WHEN** a caller calls `normalize_path(path)` with no filesystem parameter
- **THEN** the function SHALL convert backslashes to forward slashes
- **AND** SHALL handle URL-like paths by splitting on `://` and normalizing the path portion
- **AND** SHALL use `posixpath.normpath` to normalize the result
- **AND** SHALL return the normalized path as a string

#### Scenario: Filesystem-aware normalization with local filesystem
- **WHEN** a caller calls `normalize_path(path, filesystem=LocalFileSystem())`
- **THEN** the function SHALL use `os.path.abspath` to normalize the path
- **AND** SHALL return an absolute local path

#### Scenario: Filesystem-aware normalization with remote filesystem
- **WHEN** a caller calls `normalize_path(path, filesystem=S3FileSystem())`
- **THEN** the function SHALL add the protocol prefix if missing (e.g., `s3://`)
- **AND** SHALL preserve the path structure for the remote filesystem
- **AND** SHALL return the path with protocol prefix

#### Scenario: Optional validation enabled
- **WHEN** a caller calls `normalize_path(path, validate=True, operation="read")`
- **THEN** the function SHALL normalize the path based on the filesystem parameter
- **AND** SHALL call appropriate validation functions
- **AND** SHALL raise `ValueError` or `DatasetPathError` if validation fails
- **AND** SHALL return the normalized path if validation succeeds

#### Scenario: Validation with operation context
- **WHEN** a caller calls `normalize_path(path, validate=True, operation="write")`
- **THEN** the function SHALL validate parent directory exists for write operations
- **AND** SHALL validate path exists for read operations
- **AND** SHALL use the operation parameter to determine appropriate validation checks

## MODIFIED Requirements

### Requirement: Existing _normalize_path function

The existing `_normalize_path()` function in `core/filesystem/paths.py` SHALL be maintained as a deprecated alias for backward compatibility.

#### Scenario: Deprecated alias still works
- **WHEN** existing code calls `_normalize_path(path)` from `core/filesystem/paths.py`
- **THEN** the function SHALL work as before
- **AND** SHALL emit a `DeprecationWarning` pointing to the new `normalize_path()` function
- **AND** SHALL maintain the same return value and behavior
