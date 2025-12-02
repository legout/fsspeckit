## 1. Implementation

- [ ] 1.1 Audit core modules (`datasets/duckdb.py`, `datasets/pyarrow.py`, `core/ext.py`, storage option classes) for:
  - [ ] 1.1.1 `except Exception:` or equivalent broad catches.
  - [ ] 1.1.2 Cleanup code that performs multiple actions inside a single generic `try/except`.
- [ ] 1.2 Refactor identified blocks to:
  - [ ] 1.2.1 Use specific exception types where behaviour is expected and known (e.g. DuckDB error types, `FileNotFoundError`, `ValueError`).
  - [ ] 1.2.2 Add a catch-all only where necessary, logging the exception and re-raising, rather than swallowing it.
- [ ] 1.3 Introduce small cleanup helpers for DuckDB and similar contexts:
  - [ ] 1.3.1 Implement a helper that unregisters one DuckDB table at a time, logging any failure without aborting the entire cleanup.
  - [ ] 1.3.2 Replace monolithic “unregister N tables inside one try/except” blocks with calls to this helper.
- [ ] 1.4 Replace any remaining `print(...)` calls used for error reporting with logger calls obtained via `fsspeckit.common.logging.get_logger`.

## 2. Testing

- [ ] 2.1 Add tests that simulate error conditions (e.g. failing unregister, failing dataset read) and assert:
  - [ ] 2.1.1 Errors are logged (using test log capture) rather than silently ignored.
  - [ ] 2.1.2 The raised exceptions are of expected types, not generic `Exception`.
- [ ] 2.2 Add tests for cleanup helpers to ensure they:
  - [ ] 2.2.1 Attempt to clean up all intended resources even when some operations fail.
  - [ ] 2.2.2 Do not mask unexpected errors.

## 3. Documentation

- [ ] 3.1 Add or update contributor guidelines (or a brief section in `openspec/project.md`) outlining:
  - [ ] 3.1.1 Preferred error-handling patterns (no bare `except Exception`, log-and-rethrow for unexpected errors, etc.).
  - [ ] 3.1.2 Expectations around using the project’s logging utilities instead of `print()`.

