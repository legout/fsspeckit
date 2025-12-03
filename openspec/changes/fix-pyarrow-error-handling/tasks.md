## 1. Implementation

- [ ] 1.1 Audit PyArrow modules for broad exception handling:
  - [ ] 1.1.1 `src/fsspeckit/datasets/pyarrow.py` for `except Exception:` blocks
  - [ ] 1.1.2 `src/fsspeckit/utils/pyarrow.py` for generic exception catches
- [ ] 1.2 Replace generic exceptions with specific PyArrow types:
  - [ ] 1.2.1 Use `pyarrow.ArrowInvalid` for invalid data/schema operations
  - [ ] 1.2.2 Use `pyarrow.ArrowNotImplementedError` for unsupported operations
  - [ ] 1.2.3 Use `pyarrow.ArrowIOError` for file I/O problems
  - [ ] 1.2.4 Use `pyarrow.ArrowTypeError` for type-related errors
  - [ ] 1.2.5 Use `pyarrow.ArrowKeyError` for missing keys/fields
  - [ ] 1.2.6 Use `pyarrow.ArrowIndexError` for out-of-bounds access
- [ ] 1.3 Add proper import fallbacks for PyArrow exception types
- [ ] 1.4 Enhance error messages with operation context (file paths, schema info)
- [ ] 1.5 Ensure dataset cleanup operations log failures but continue cleanup

## 2. Testing

- [ ] 2.1 Add tests that simulate PyArrow error conditions:
  - [ ] 2.1.1 Invalid schema operations raise `ArrowInvalid`
  - [ ] 2.1.2 File I/O failures raise `ArrowIOError`
  - [ ] 2.1.3 Type conversion failures raise `ArrowTypeError`
- [ ] 2.2 Test cleanup helpers ensure they:
  - [ ] 2.2.1 Attempt to clean up all intended resources even when some operations fail
  - [ ] 2.2.2 Log failures without masking unexpected errors
- [ ] 2.3 Verify error messages contain relevant context information

## 3. Documentation

- [ ] 3.1 Update PyArrow module docstrings with error handling patterns
- [ ] 3.2 Add examples of proper PyArrow exception handling in user documentation