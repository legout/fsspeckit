## Why

Optional dependencies in `fsspeckit` (Polars, PyArrow, DuckDB, sqlglot, orjson, joblib, etc.) are intended to be truly optional and only required when their associated features are used. The current implementation still has several places where:

- Optional packages are imported at module import time (e.g., `joblib` in `common.misc`).
- There is a duplicate `check_optional_dependency` helper with messaging that does not match the defined extras.
- Some modules bypass the central `common.optional` helpers and import optional libraries directly.

This leads to surprising `ImportError`s, inconsistent error messages, and ambiguity around which extras are required for which features.

## What Changes

- Audit and adjust optional dependency usage across core utility modules so that:
  - No optional dependency is imported unconditionally at module import time.
  - All usage flows through the shared helpers in `fsspeckit.common.optional` (or a small, well-defined extension of them).
- Make `run_parallel` treat joblib as truly optional:
  - Import joblib lazily only when parallel execution is requested.
  - Raise a guided `ImportError` if joblib is missing when parallelism is requested.
- Remove or delegate the duplicate `check_optional_dependency` in `fsspeckit.common.misc` to the canonical helper in `fsspeckit.common.optional`, with messages that reference the actual extras (`[datasets]`, `[sql]`, `[aws]`, etc.).

## Impact

- **Behaviour:**
  - Importing `fsspeckit.common` and other core modules works in environments without optional dependencies; errors are raised only when optional features are invoked.
  - Callers receive clear, consistent error messages that point to the correct extras group to install.
  - `run_parallel` behaves predictably: it either executes in parallel (when joblib is present) or fails fast with a clear message when joblib is absent and parallelism is requested.

- **Specs affected:**
  - `utils-optional-separation` (separation of base vs optional utilities).
  - `core-lazy-imports` (lazy import behaviour in core helpers).

- **Code affected (non-exhaustive):**
  - `src/fsspeckit/common/misc.py`
  - `src/fsspeckit/common/polars.py`
  - `src/fsspeckit/core/ext.py`
  - `src/fsspeckit/datasets/*.py`
  - `src/fsspeckit/common/optional.py` (for any new helpers).

