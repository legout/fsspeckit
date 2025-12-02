## 1. Implementation

- [ ] 1.1 Define simple validation helpers for dataset paths and compression codecs:
  - [ ] 1.1.1 Implement a path validation helper that rejects obviously unsafe strings (e.g. embedded nulls, certain control characters) and, where applicable, prevents escaping an expected base directory.
  - [ ] 1.1.2 Implement a codec validation helper that ensures compression values come from a known, whitelisted set.
- [ ] 1.2 Apply these helpers in:
  - [ ] 1.2.1 DuckDB dataset helpers before SQL strings are constructed or dataset operations are invoked.
  - [ ] 1.2.2 PyArrow dataset helpers before read/write operations or concatenations occur.
- [ ] 1.3 Implement a `scrub_credentials` helper in storage options that removes or masks likely credential values (keys, secrets, tokens) from error messages.
- [ ] 1.4 Use `scrub_credentials` in the places where storage options currently log errors that might include raw credentials.

## 2. Testing

- [ ] 2.1 Add tests that verify path validation:
  - [ ] 2.1.1 Valid paths pass without error and behave as before.
  - [ ] 2.1.2 Invalid or suspicious paths raise `ValueError` with clear messages.
- [ ] 2.2 Add tests that verify codec validation:
  - [ ] 2.2.1 Valid codecs are accepted and used as expected.
  - [ ] 2.2.2 Invalid codecs raise `ValueError` and do not reach the underlying engine.
- [ ] 2.3 Add tests for credential scrubbing:
  - [ ] 2.3.1 Simulate errors that include credential-like strings and verify that the logged message has those substrings redacted.

## 3. Documentation

- [ ] 3.1 Update docstrings for affected dataset helpers to mention:
  - [ ] 3.1.1 That certain path patterns or codecs may be rejected with `ValueError`.
  - [ ] 3.1.2 Any guarantees about path handling relative to base directories.
- [ ] 3.2 Add a brief note in storage-related documentation about how errors are logged and how credentials are protected in logs.

