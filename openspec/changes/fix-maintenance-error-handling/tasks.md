## 1. Implementation

- [ ] 1.1 Audit `src/fsspeckit/core/maintenance.py` for:
  - [ ] 1.1.1 Broad `except Exception:` blocks.
  - [ ] 1.1.2 Cleanup logic that handles multiple resources inside a single
    generic `try/except`.
- [ ] 1.2 Replace broad exception handlers with:
  - [ ] 1.2.1 Specific exception types for expected failures (e.g.
    `FileNotFoundError`, `PermissionError`, `OSError`, `ValueError`).
  - [ ] 1.2.2 Catch-all handlers that log and re-raise unexpected errors.
- [ ] 1.3 Ensure maintenance helpers log failures per dataset or path while
  still attempting to process remaining work.

## 2. Testing

- [ ] 2.1 Add or extend tests for maintenance helpers to simulate:
  - [ ] 2.1.1 Missing or invalid paths.
  - [ ] 2.1.2 Failures during compaction/optimization of a subset of
    partitions.
- [ ] 2.2 Assert that:
  - [ ] 2.2.1 Errors are logged rather than silently ignored.
  - [ ] 2.2.2 Exceptions have specific, documented types.

## 3. Validation

- [ ] 3.1 Run `openspec validate fix-maintenance-error-handling --strict`
  and fix any spec issues.

