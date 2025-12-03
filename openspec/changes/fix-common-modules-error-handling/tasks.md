## 1. Implementation

- [ ] 1.1 Audit common modules for broad or silencing error handling:
  - [ ] 1.1.1 `src/fsspeckit/common/schema.py`
  - [ ] 1.1.2 `src/fsspeckit/common/misc.py`
- [ ] 1.2 Replace generic `except Exception:` blocks with:
  - [ ] 1.2.1 Narrow, expected exception types (e.g. `ValueError`, `TypeError`,
    `KeyError`, `AttributeError`).
  - [ ] 1.2.2 Catch-all handlers that log and re-raise unexpected exceptions
    instead of swallowing them.
- [ ] 1.3 Ensure error messages include useful context (operation, key/schema
  information) without leaking sensitive data.
- [ ] 1.4 Replace `print()`-based error reporting (if any) with loggers
  obtained via the centralized logging utilities.

## 2. Testing

- [ ] 2.1 Add or extend tests for schema and misc helpers to cover:
  - [ ] 2.1.1 Validation failures raising specific exception types.
  - [ ] 2.1.2 Error messages containing relevant context.
- [ ] 2.2 Add tests that simulate unexpected exceptions in callbacks or
  user-provided functions and assert they are logged and re-raised.

## 3. Validation

- [ ] 3.1 Run `openspec validate fix-common-modules-error-handling --strict`
  and fix any spec issues.
