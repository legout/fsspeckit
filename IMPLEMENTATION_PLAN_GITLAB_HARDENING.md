# Implementation Plan: Harden GitLab Resource Management

## Overview

This document provides a detailed implementation plan for the `harden-gitlab-resource-management` OpenSpec change. The goal is to prevent resource leaks, infinite loops, and production instability in the GitLab filesystem implementation.

## Current State Analysis

### Files to Modify

1. **`/home/volker/coding/fsspeckit/src/fsspeckit/core/filesystem/gitlab.py`** - Main GitLab filesystem implementation
   - Current issues:
     - Creates `requests.Session()` without cleanup (line 97)
     - No pagination limits in `ls()` method (lines 167-224)
     - No input validation for timeout parameter
     - Basic error handling for malformed headers (lines 201-206)

2. **`/home/volker/coding/fsspeckit/src/fsspeckit/storage_options/git.py`** - Storage options
   - Current issues:
     - `GitLabStorageOptions` lacks timeout and max_pages parameters
     - No validation for configuration parameters

3. **`/home/volker/coding/fsspeckit/tests/test_gitlab_filesystem.py`** - Test suite
   - Current coverage:
     - Basic initialization and URL encoding tests
     - Simple pagination tests
     - Missing: resource cleanup, pagination limits, validation

## Implementation Strategy

### Phase 1: Core Resource Management (GitLabFileSystem)

**Priority: HIGH - These changes prevent resource leaks**

#### Step 1.1: Add Session Resource Cleanup Methods

**File:** `/home/volker/coding/fsspeckit/src/fsspeckit/core/filesystem/gitlab.py`

**Changes:**
1. Add `close()` method after line 292 (after exists method)
2. Add `__del__()` method after `close()` method
3. Add instance variable `_closed` to track state

**Implementation Details:**

```python
def close(self) -> None:
    """Close the filesystem and cleanup resources.
    
    This method closes the underlying requests.Session to prevent
    resource leaks in long-running applications.
    """
    if hasattr(self, '_session') and self._session:
        self._session.close()
        self._session = None
        logger.debug("GitLab filesystem session closed")

def __del__(self) -> None:
    """Cleanup resources during garbage collection.
    
    This is a fallback cleanup mechanism in case the filesystem
    is not explicitly closed. Should not be relied upon for
    production cleanup.
    """
    try:
        self.close()
    except Exception:
        # Silently ignore errors during cleanup
        pass

# Add to __init__ after line 100:
self._closed = False

# Update close() method to set flag:
def close(self) -> None:
    if not self._closed:
        if hasattr(self, '_session') and self._session:
            self._session.close()
            self._session = None
            self._closed = True
            logger.debug("GitLab filesystem session closed")
```

**Order:** First - Required for all subsequent changes

#### Step 1.2: Add Pagination Limit Support

**File:** `/home/volker/coding/fsspeckit/src/fsspeckit/core/filesystem/gitlab.py`

**Changes:**
1. Add `max_pages` parameter to `__init__()` (line 60-70)
2. Add validation for `max_pages` in `__init__()`
3. Update `ls()` method to enforce limits (lines 167-224)

**Implementation Details:**

In `__init__()` after line 91:
```python
self.max_pages = max_pages
```

Add to parameter list:
```python
def __init__(
    self,
    base_url: str = "https://gitlab.com",
    project_id: str | int | None = None,
    project_name: str | None = None,
    ref: str = "main",
    token: str | None = None,
    api_version: str = "v4",
    timeout: float = 30.0,
    max_pages: int = 1000,
    **kwargs: Any,
):
```

Add validation after line 91:
```python
if max_pages is not None:
    if not isinstance(max_pages, int) or max_pages <= 0:
        raise ValueError(
            f"max_pages must be a positive integer, got {max_pages!r}"
        )
    if max_pages > 10000:
        raise ValueError(
            f"max_pages must not exceed 10000, got {max_pages}"
        )
    self.max_pages = max_pages
else:
    self.max_pages = 1000
```

Update `ls()` method loop (around line 184):
```python
all_files = []
page = 1
per_page = 100
pages_fetched = 0

while True:
    # ... existing code ...
    
    pages_fetched += 1
    if pages_fetched >= self.max_pages:
        logger.warning(
            "Reached maximum pages limit (%d), stopping pagination",
            self.max_pages
        )
        break
    
    # Check for pagination headers
    # ... existing code ...
```

**Order:** Second - Builds on session management

#### Step 1.3: Add Input Validation

**File:** `/home/volker/coding/fsspeckit/src/fsspeckit/core/filesystem/gitlab.py`

**Changes:**
1. Add timeout validation in `__init__()`
2. Add validation helper method for parameters

**Implementation Details:**

After line 91 (after existing validations), add:
```python
# Validate timeout parameter
if timeout is not None:
    if not isinstance(timeout, (int, float)) or timeout <= 0:
        raise ValueError(
            f"timeout must be a positive number, got {timeout!r}"
        )
    if timeout > 3600:
        raise ValueError(
            f"timeout must not exceed 3600 seconds, got {timeout}"
        )
```

**Order:** Third - Independent validation

#### Step 1.4: Enhance Pagination Error Handling

**File:** `/home/volker/coding/fsspeckit/src/fsspeckit/core/filesystem/gitlab.py`

**Changes:**
1. Improve handling of malformed pagination headers in `ls()` method
2. Add better error messages and logging

**Implementation Details:**

In `ls()` method around lines 201-206, replace existing header check:
```python
# Check for pagination headers
next_page = response.headers.get("X-Next-Page")
if not next_page:
    # No more pages
    break

# Validate pagination header
try:
    page = int(next_page)
except (ValueError, TypeError):
    logger.warning(
        "Malformed pagination header X-Next-Page: %r, stopping pagination",
        next_page
    )
    break
```

**Order:** Fourth - Complements pagination limits

### Phase 2: Storage Options Enhancement

**Priority: MEDIUM - Provides consistent validation interface**

#### Step 2.1: Add Parameters to GitLabStorageOptions

**File:** `/home/volker/coding/fsspeckit/src/fsspeckit/storage_options/git.py`

**Changes:**
1. Add `timeout` and `max_pages` attributes to `GitLabStorageOptions` class
2. Update `__post_init__()` to validate these parameters
3. Update `from_env()` to read environment variables
4. Update `to_fsspec_kwargs()` to pass parameters to filesystem

**Implementation Details:**

Around line 185-191, add to class definition:
```python
protocol: str = "gitlab"
base_url: str = "https://gitlab.com"
project_id: str | int | None = None
project_name: str | None = None
ref: str | None = None
token: str | None = None
api_version: str = "v4"
timeout: float | None = None
max_pages: int | None = None
```

Update `from_env()` (lines 227-235):
```python
return cls(
    protocol="gitlab",
    base_url=os.getenv("GITLAB_URL", "https://gitlab.com"),
    project_id=os.getenv("GITLAB_PROJECT_ID"),
    project_name=os.getenv("GITLAB_PROJECT_NAME"),
    ref=os.getenv("GITLAB_REF"),
    token=os.getenv("GITLAB_TOKEN"),
    api_version=os.getenv("GITLAB_API_VERSION", "v4"),
    timeout=float(os.getenv("GITLAB_TIMEOUT")) if os.getenv("GITLAB_TIMEOUT") else None,
    max_pages=int(os.getenv("GITLAB_MAX_PAGES")) if os.getenv("GITLAB_MAX_PAGES") else None,
)
```

Update `to_fsspec_kwargs()` (lines 264-288):
```python
kwargs = {
    "base_url": self.base_url,
    "project_id": self.project_id,
    "project_name": self.project_name,
    "ref": self.ref,
    "token": self.token,
    "api_version": self.api_version,
    "timeout": self.timeout,
    "max_pages": self.max_pages,
}
return {k: v for k, v in kwargs.items() if v is not None}
```

Update `__post_init__()` (lines 193-202):
```python
def __post_init__(self) -> None:
    """Validate GitLab configuration after initialization.

    Ensures either project_id or project_name is provided.
    Validates timeout and max_pages parameters.

    Raises:
        ValueError: If neither project_id nor project_name is provided
                   or if timeout/max_pages are invalid
    """
    if self.project_id is None and self.project_name is None:
        raise ValueError("Either project_id or project_name must be provided")
    
    # Validate timeout
    if self.timeout is not None:
        if not isinstance(self.timeout, (int, float)) or self.timeout <= 0:
            raise ValueError(
                f"timeout must be a positive number, got {self.timeout!r}"
            )
        if self.timeout > 3600:
            raise ValueError(
                f"timeout must not exceed 3600 seconds, got {self.timeout}"
            )
    
    # Validate max_pages
    if self.max_pages is not None:
        if not isinstance(self.max_pages, int) or self.max_pages <= 0:
            raise ValueError(
                f"max_pages must be a positive integer, got {self.max_pages!r}"
            )
        if self.max_pages > 10000:
            raise ValueError(
                f"max_pages must not exceed 10000, got {self.max_pages}"
            )
```

**Order:** Fifth - Independent of core changes

### Phase 3: Testing

**Priority: HIGH - Essential for verification**

#### Step 3.1: Add Resource Management Tests

**File:** `/home/volker/coding/fsspeckit/tests/test_gitlab_filesystem.py`

**Changes:**
1. Add test for `close()` method functionality
2. Add test for `__del__()` fallback cleanup
3. Add test for multiple instances with separate sessions

**Implementation Details:**

Add new test methods after line 262:

```python
def test_gitlab_filesystem_close_method(self):
    """Test that close() method properly cleanup resources."""
    from fsspeckit.core.filesystem import GitLabFileSystem

    fs = GitLabFileSystem(project_id="12345")
    assert fs._session is not None
    assert not fs._closed

    # Mock session.close to verify it's called
    with patch.object(fs._session, 'close') as mock_close:
        fs.close()
        mock_close.assert_called_once()
        assert fs._session is None
        assert fs._closed

    # Verify subsequent calls to close don't error
    fs.close()  # Should not raise

def test_gitlab_filesystem_del_fallback_cleanup(self):
    """Test that __del__ method provides fallback cleanup."""
    from fsspeckit.core.filesystem import GitLabFileSystem

    fs = GitLabFileSystem(project_id="12345")
    fs._session = MagicMock()

    # Call __del__ and verify session.close is called
    fs.__del__()

    fs._session.close.assert_called_once()

def test_gitlab_filesystem_multiple_instances_separate_sessions(self):
    """Test that multiple instances maintain separate sessions."""
    from fsspeckit.core.filesystem import GitLabFileSystem

    fs1 = GitLabFileSystem(project_id="12345")
    fs2 = GitLabFileSystem(project_id="67890")

    # Verify sessions are different objects
    assert fs1._session is not fs2._session

    # Cleanup
    fs1.close()
    fs2.close()
```

#### Step 3.2: Add Pagination Limit Tests

**File:** `/home/volker/coding/fsspeckit/tests/test_gitlab_filesystem.py`

**Changes:**
1. Add test for default max_pages
2. Add test for custom max_pages
3. Add test for max_pages validation
4. Add test for pagination limit enforcement
5. Add test for malformed pagination headers

**Implementation Details:**

Add new test methods:

```python
def test_gitlab_filesystem_max_pages_default(self):
    """Test default max_pages value."""
    from fsspeckit.core.filesystem import GitLabFileSystem

    fs = GitLabFileSystem(project_id="12345")
    assert fs.max_pages == 1000

def test_gitlab_filesystem_max_pages_custom(self):
    """Test custom max_pages value."""
    from fsspeckit.core.filesystem import GitLabFileSystem

    fs = GitLabFileSystem(project_id="12345", max_pages=500)
    assert fs.max_pages == 500

def test_gitlab_filesystem_max_pages_validation(self):
    """Test max_pages parameter validation."""
    from fsspeckit.core.filesystem import GitLabFileSystem

    # Test invalid types
    with pytest.raises(ValueError, match="max_pages must be a positive integer"):
        GitLabFileSystem(project_id="12345", max_pages="not_an_int")

    with pytest.raises(ValueError, match="max_pages must be a positive integer"):
        GitLabFileSystem(project_id="12345", max_pages=0)

    # Test too large
    with pytest.raises(ValueError, match="max_pages must not exceed 10000"):
        GitLabFileSystem(project_id="12345", max_pages=15000)

def test_gitlab_filesystem_ls_respects_max_pages(self):
    """Test that ls method respects max_pages limit."""
    from fsspeckit.core.filesystem import GitLabFileSystem

    fs = GitLabFileSystem(project_id="12345", max_pages=2)

    # Mock responses for more pages than max_pages
    responses = [
        Mock(
            status_code=200,
            json.return_value=[{"name": f"file{i}.txt", "type": "blob"}],
            headers={"X-Next-Page": str(i + 1)}
        )
        for i in range(1, 5)  # 4 pages of responses
    ]

    with patch.object(fs._session, 'get', side_effect=responses):
        with patch('fsspeckit.core.filesystem.logger') as mock_logger:
            result = fs.ls("/")

            # Should only fetch 2 pages max
            assert len(responses) >= 2  # At least 2 calls made
            # Should log warning about limit
            mock_logger.warning.assert_called_once_with(
                "Reached maximum pages limit (%d), stopping pagination",
                2
            )

def test_gitlab_filesystem_ls_malformed_pagination_header(self):
    """Test handling of malformed pagination headers."""
    from fsspeckit.core.filesystem import GitLabFileSystem

    fs = GitLabFileSystem(project_id="12345")

    # Mock responses with malformed header
    responses = [
        Mock(
            status_code=200,
            json.return_value=[{"name": "file1.txt", "type": "blob"}],
            headers={"X-Next-Page": "invalid"}
        ),
        Mock(
            status_code=200,
            json.return_value=[{"name": "file2.txt", "type": "blob"}],
            headers={}  # No next page
        )
    ]

    with patch.object(fs._session, 'get', side_effect=responses):
        with patch('fsspeckit.core.filesystem.logger') as mock_logger:
            result = fs.ls("/")

            # Should stop on malformed header
            assert len(result) == 1
            mock_logger.warning.assert_called_once_with(
                "Malformed pagination header X-Next-Page: %r, stopping pagination",
                "invalid"
            )
```

#### Step 3.3: Add Input Validation Tests

**File:** `/home/volker/coding/fsspeckit/tests/test_gitlab_filesystem.py`

**Changes:**
1. Add test for timeout validation
2. Add test for timeout range validation
3. Add test for timeout edge cases

**Implementation Details:**

Add new test methods:

```python
def test_gitlab_filesystem_timeout_validation(self):
    """Test timeout parameter validation."""
    from fsspeckit.core.filesystem import GitLabFileSystem

    # Test invalid types
    with pytest.raises(ValueError, match="timeout must be a positive number"):
        GitLabFileSystem(project_id="12345", timeout="not_a_number")

    with pytest.raises(ValueError, match="timeout must be a positive number"):
        GitLabFileSystem(project_id="12345", timeout=0)

    with pytest.raises(ValueError, match="timeout must be a positive number"):
        GitLabFileSystem(project_id="12345", timeout=-10)

def test_gitlab_filesystem_timeout_range_validation(self):
    """Test timeout maximum value validation."""
    from fsspeckit.core.filesystem import GitLabFileSystem

    # Test too large timeout
    with pytest.raises(ValueError, match="timeout must not exceed 3600 seconds"):
        GitLabFileSystem(project_id="12345", timeout=5000)

def test_gitlab_filesystem_valid_timeout_edge_cases(self):
    """Test valid timeout edge cases."""
    from fsspeckit.core.filesystem import GitLabFileSystem

    # Test minimum valid value
    fs = GitLabFileSystem(project_id="12345", timeout=0.1)
    assert fs.timeout == 0.1

    # Test maximum valid value
    fs = GitLabFileSystem(project_id="12345", timeout=3600)
    assert fs.timeout == 3600

    # Test float values
    fs = GitLabFileSystem(project_id="12345", timeout=30.5)
    assert fs.timeout == 30.5
```

#### Step 3.4: Add Storage Options Tests

**File:** Create new test file or add to existing git.py tests

**Changes:**
1. Add tests for GitLabStorageOptions timeout/max_pages validation
2. Add tests for environment variable handling
3. Add tests for error messages

**Implementation Details:**

Add to test file:

```python
def test_gitlab_storage_options_timeout_validation(self):
    """Test GitLabStorageOptions timeout validation."""
    from fsspeckit.storage_options import GitLabStorageOptions

    # Test invalid timeout
    with pytest.raises(ValueError, match="timeout must be a positive number"):
        GitLabStorageOptions(project_id=12345, timeout=0)

def test_gitlab_storage_options_max_pages_validation(self):
    """Test GitLabStorageOptions max_pages validation."""
    from fsspeckit.storage_options import GitLabStorageOptions

    # Test invalid max_pages
    with pytest.raises(ValueError, match="max_pages must be a positive integer"):
        GitLabStorageOptions(project_id=12345, max_pages=0)

def test_gitlab_storage_options_env_variables(self):
    """Test GitLabStorageOptions handles environment variables."""
    from fsspeckit.storage_options import GitLabStorageOptions

    with patch.dict(os.environ, {
        'GITLAB_PROJECT_ID': '12345',
        'GITLAB_TIMEOUT': '60',
        'GITLAB_MAX_PAGES': '500'
    }):
        options = GitLabStorageOptions.from_env()
        assert options.project_id == '12345'
        assert options.timeout == 60.0
        assert options.max_pages == 500
```

**Order:** Sixth - After all implementation changes

### Phase 4: Integration and Verification

#### Step 4.1: Run All Tests

**Commands:**
```bash
# Run GitLab-specific tests
pytest tests/test_gitlab_filesystem.py -v

# Run storage options tests
pytest tests/ -k "gitlab" -v
```

#### Step 4.2: Validate OpenSpec Specification

**Commands:**
```bash
openspec validate harden-gitlab-resource-management --strict
```

#### Step 4.3: Integration Testing

**Test scenarios:**
1. Create filesystem with default parameters
2. Create filesystem with custom timeout and max_pages
3. Test resource cleanup with context manager pattern
4. Test pagination with large repositories
5. Test error handling with malformed inputs

**Order:** Final - After all tests pass

## Order of Operations Summary

**Critical Path:**
1. Add session cleanup methods (close, __del__)
2. Add max_pages parameter and validation
3. Add timeout validation
4. Enhance pagination error handling
5. Update GitLabStorageOptions
6. Add comprehensive tests
7. Run validation and integration tests

**Dependencies:**
- Step 1.1 must be first (foundational)
- Step 1.2 depends on 1.1
- Step 2.1 can be done in parallel with 1.2-1.4
- Step 3.x must be done after all implementation changes

## Testing Strategy

### Unit Tests
- Resource management: close(), __del__(), multiple instances
- Pagination limits: default, custom, enforcement, malformed headers
- Input validation: timeout range, max_pages range, edge cases
- Storage options: validation, environment variables

### Integration Tests
- End-to-end filesystem operations
- Context manager pattern
- Long-running operation cleanup
- Error recovery scenarios

### Edge Cases
- Very large max_pages values
- Very small timeout values
- Malformed API responses
- Network failures during pagination
- Multiple concurrent instances

## Potential Risks and Mitigation

### Risk 1: Breaking Changes
**Impact:** Existing code may break
**Mitigation:** All changes are additive, no breaking changes
**Verification:** Run existing tests, ensure backward compatibility

### Risk 2: Performance Impact
**Impact:** Pagination limits may truncate results
**Mitigation:** Default limit of 1000 pages is generous (>100k files)
**Verification:** Test with large repositories, document behavior

### Risk 3: Session Cleanup Ordering
**Impact:** Sessions may be closed prematurely
**Mitigation:** Track closed state, idempotent close()
**Verification:** Test multiple close() calls, concurrent access

### Risk 4: Validation Errors
**Impact:** Valid configurations may be rejected
**Mitigation:** Conservative validation ranges (timeout 0-3600, max_pages 1-10000)
**Verification:** Test edge cases, clear error messages

### Risk 5: Environment Variable Parsing
**Impact:** Invalid env vars cause crashes
**Mitigation:** Handle parsing errors gracefully, log warnings
**Verification:** Test with invalid env vars

## Verification Steps

1. **Code Review:**
   - [ ] Review all changes for consistency
   - [ ] Verify error messages are clear and helpful
   - [ ] Check logging levels are appropriate
   - [ ] Ensure type hints are correct

2. **Test Coverage:**
   - [ ] All new code paths have tests
   - [ ] Edge cases are covered
   - [ ] Error conditions are tested
   - [ ] Integration scenarios work

3. **OpenSpec Validation:**
   - [ ] Run `openspec validate harden-gitlab-resource-management --strict`
   - [ ] All scenarios pass
   - [ ] No validation errors

4. **Manual Testing:**
   - [ ] Test with real GitLab instance
   - [ ] Test resource cleanup in long-running process
   - [ ] Test pagination with various repository sizes
   - [ ] Verify error messages in real scenarios

5. **Backward Compatibility:**
   - [ ] Existing code without new parameters works
   - [ ] Default behavior unchanged
   - [ ] No deprecation warnings

## Implementation Checklist

### Core Implementation
- [ ] 1.1 Add close() method to GitLabFileSystem
- [ ] 1.2 Add __del__() method to GitLabFileSystem
- [ ] 1.3 Add max_pages parameter to GitLabFileSystem
- [ ] 1.4 Add max_pages validation to GitLabFileSystem
- [ ] 1.5 Update ls() method to enforce pagination limits
- [ ] 1.6 Add timeout validation to GitLabFileSystem
- [ ] 1.7 Enhance malformed header handling in ls()

### Storage Options
- [ ] 2.1 Add timeout parameter to GitLabStorageOptions
- [ ] 2.2 Add max_pages parameter to GitLabStorageOptions
- [ ] 2.3 Update GitLabStorageOptions.from_env()
- [ ] 2.4 Update GitLabStorageOptions.to_fsspec_kwargs()
- [ ] 2.5 Add validation to GitLabStorageOptions.__post_init__()

### Testing
- [ ] 3.1 Add resource management tests
- [ ] 3.2 Add pagination limit tests
- [ ] 3.3 Add input validation tests
- [ ] 3.4 Add storage options tests
- [ ] 3.5 Run all tests successfully

### Validation
- [ ] 4.1 Run OpenSpec validation
- [ ] 4.2 Manual integration testing
- [ ] 4.3 Verify backward compatibility
- [ ] 4.4 Update task checklist

## Success Criteria

1. **Resource Management:**
   - [ ] Session cleanup works correctly
   - [ ] No resource leaks in long-running processes
   - [ ] Cleanup is idempotent and safe

2. **Pagination Safety:**
   - [ ] No infinite loops with malformed headers
   - [ ] Pagination limits enforced correctly
   - [ ] Default limits work for typical use cases

3. **Input Validation:**
   - [ ] All invalid inputs rejected with clear errors
   - [ ] Validation ranges are sensible and documented
   - [ ] Error messages are helpful and actionable

4. **Backward Compatibility:**
   - [ ] No breaking changes to existing code
   - [ ] Default behavior unchanged
   - [ ] All existing tests pass

5. **Code Quality:**
   - [ ] All new code has tests
   - [ ] Type hints correct
   - [ ] Documentation updated
   - [ ] OpenSpec specification satisfied

## References

- OpenSpec Specification: `/home/volker/coding/fsspeckit/openspec/changes/harden-gitlab-resource-management/`
- Current Implementation: `/home/volker/coding/fsspeckit/src/fsspeckit/core/filesystem/gitlab.py`
- Storage Options: `/home/volker/coding/fsspeckit/src/fsspeckit/storage_options/git.py`
- Test Suite: `/home/volker/coding/fsspeckit/tests/test_gitlab_filesystem.py`
