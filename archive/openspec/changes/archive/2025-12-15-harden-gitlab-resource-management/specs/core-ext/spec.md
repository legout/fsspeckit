## ADDED Requirements

### Requirement: GitLab filesystem SHALL implement proper resource cleanup

GitLabFileSystem SHALL provide proper cleanup of HTTP session resources to prevent resource leaks in long-running applications.

#### Scenario: Session cleanup on explicit close
- **WHEN** caller invokes `close()` method on GitLabFileSystem instance
- **THEN** filesystem SHALL cleanup underlying requests.Session
- **AND** subsequent API calls SHALL raise appropriate errors indicating closed state

#### Scenario: Session cleanup on garbage collection
- **WHEN** GitLabFileSystem instance is garbage collected without explicit close
- **THEN** filesystem SHALL cleanup underlying requests.Session in `__del__` method
- **AND** cleanup SHALL be logged at debug level for monitoring

#### Scenario: Multiple instances maintain separate sessions
- **WHEN** multiple GitLabFileSystem instances are created
- **THEN** each instance SHALL maintain its own independent session
- **AND** cleanup of one instance SHALL NOT affect others

### Requirement: GitLab filesystem SHALL implement pagination limits

GitLabFileSystem SHALL implement maximum page limits to prevent infinite loops when processing malformed API responses.

#### Scenario: Pagination limit enforcement
- **WHEN** `ls()` method encounters more than `max_pages` (default 1000) pages
- **THEN** filesystem SHALL stop pagination and return collected results
- **AND** SHALL log warning about pagination limit reached
- **AND** SHALL include metadata indicating incomplete results

#### Scenario: Configurable pagination limits
- **WHEN** caller specifies `max_pages` parameter during filesystem creation
- **THEN** filesystem SHALL use provided limit instead of default
- **AND** SHALL validate that limit is positive integer
- **AND** SHALL raise ValueError for invalid limits

#### Scenario: Malformed pagination header handling
- **WHEN** GitLab API returns non-numeric or invalid `X-Next-Page` header
- **THEN** filesystem SHALL log warning about malformed header
- **AND** SHALL stop pagination gracefully
- **AND** SHALL return results collected up to that point

### Requirement: GitLab filesystem SHALL validate input parameters

GitLabFileSystem SHALL validate all configuration parameters to prevent runtime errors and provide clear error messages.

#### Scenario: Timeout parameter validation
- **WHEN** caller provides timeout parameter during filesystem creation
- **THEN** filesystem SHALL validate timeout is positive number
- **AND** SHALL enforce maximum timeout (3600 seconds)
- **AND** SHALL raise ValueError with descriptive message for invalid values

#### Scenario: Maximum pages parameter validation
- **WHEN** caller provides max_pages parameter during filesystem creation
- **THEN** filesystem SHALL validate max_pages is positive integer
- **AND** SHALL enforce reasonable maximum (10000 pages)
- **AND** SHALL raise ValueError with descriptive message for invalid values

#### Scenario: Invalid configuration error messages
- **WHEN** filesystem detects invalid configuration parameters
- **THEN** error message SHALL clearly indicate which parameter is invalid
- **AND** SHALL provide guidance on valid range or format
- **AND** SHALL suggest corrective action when possible