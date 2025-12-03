## Context

The library already has a centralised optional dependency management module (`fsspeckit.common.optional`) that:

- Tracks availability flags for optional packages.
- Provides lazy `_import_*` helpers with consistent error messaging.

However, other modules (notably `common.misc` and some dataset helpers) still import optional dependencies directly at module import time, and there is a duplicate `check_optional_dependency` function with mismatched extras names. This undermines the benefits of lazy imports and makes it harder to reason about which extras are required.

## Goals

- Make the presence or absence of optional dependencies visible only when relevant features are used.
- Ensure all optional imports feed through a single, well-documented mechanism.
- Avoid confusion by having a single `check_optional_dependency` implementation with extras-aware messaging.

## Design Decisions

1. **Centralised lazy imports**
   - All optional libraries will be imported using a small set of helpers in `common.optional` (and any adjunct helper for joblib, if needed).
   - Modules elsewhere will not import optional packages at the top level.

2. **`run_parallel` behaviour**
   - `run_parallel` will:
     - Attempt to import joblib only when parallel execution is requested.
     - Raise a clear `ImportError` pointing to the appropriate extras group if joblib is unavailable.
     - Continue to behave as today when joblib is present.

3. **Single `check_optional_dependency`**
   - Only `fsspeckit.common.optional.check_optional_dependency` will be considered authoritative.
   - Any other helper with similar semantics will be removed or turned into a thin wrapper that delegates to `common.optional`.
   - Error messages will explicitly mention real extras defined in `pyproject.toml`.

4. **Test strategy**
   - Use environment or import mocking to simulate missing optional dependencies in a controlled way, without requiring changes to the test environment’s installed packages.

## Risks / Trade-offs

- Introducing additional indirection for imports may slightly increase call overhead when optional features are used, but the impact should be negligible compared to I/O and computation, and is outweighed by clarity and consistency.
- Tightening error messaging around extras may surface places where documentation and packaging need to be kept in sync; this is desirable but requires coordination with docs.

