## MODIFIED Requirements

### Requirement: PyArrow dataset helpers validate paths and codecs

PyArrow-based helpers SHALL perform basic validation of paths and compression codecs before performing dataset operations.

#### Scenario: Invalid path input to PyArrow helper
- **WHEN** a caller passes a path value that is clearly malformed or unsafe
- **THEN** the helper SHALL raise a `ValueError` before attempting to read or write data
- **AND** SHALL not rely on downstream exceptions to signal this.

#### Scenario: Unsupported compression codec is rejected
- **WHEN** a caller passes a compression codec not recognised by the helper’s whitelist
- **THEN** the helper SHALL raise a `ValueError`
- **AND** SHALL not attempt to proceed with PyArrow calls using that codec.

