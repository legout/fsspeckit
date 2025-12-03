## 1. Implementation

- [x] 1.1 Identify set of `fsspeckit.utils` imports used by:
  - [x] 1.1.1 The current test suite.
  - [x] 1.1.2 Documented examples and common usage patterns (if any).
- [x] 1.2 For each identified import path:
  - [x] 1.2.1 Ensure that a corresponding symbol is re-exported from `fsspeckit.utils` (either via `__init__` or a small shim module).
  - [x] 1.2.2 Where deeper paths are used (e.g. `fsspeckit.utils.misc.Progress`), add a small `utils/misc.py` that imports and re-exports `Progress` and other required names from the canonical module.
- [x] 1.3 Add deprecation notices for `utils.*` in docstrings and/or documentation where appropriate, while keeping behaviour unchanged for now.
- [x] 1.4 Ensure new implementation code continues to live in domain packages rather than in `utils`.

## 2. Testing

- [x] 2.1 Add tests that import chosen `fsspeckit.utils` entry points and verify that they refer to same objects as canonical domain modules.
- [x] 2.2 Add tests for any new shim modules (e.g. `fsspeckit.utils.misc`) to confirm that expected attributes are present and of right type.

## 3. Documentation

- [x] 3.1 Update architecture and usage documentation to:
  - [x] 3.1.1 Reiterate that `fsspeckit.utils` is a façade, not an implementation home.
  - [x] 3.1.2 List a small set of supported imports from `utils`.
  - [x] 3.1.3 Explain migration path towards importing directly from `datasets`, `sql`, or `common`.