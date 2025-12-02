# Implementation Order for OpenSpec Changes (Phases 1–4)

This document proposes an execution order for all current change proposals under `openspec/changes/`. The ordering is designed to:

- Fix correctness and packaging issues first.
- Stabilise optional dependencies and backwards compatibility.
- Improve error-handling and security.
- Then perform structural refactors and strengthen typing/tests.

Within each tier, items can often be done in parallel if desired.

---

## Tier 1 – Correctness & Packaging (Phase 1)

1. **`fix-packaging-extras-version`**
   - Fix extras in `pyproject.toml`.
   - Make `__version__` initialisation robust for dev vs installed environments.

2. **`fix-core-io-helpers`**
   - Correct `_read_json`/`_read_csv` threading control flow.
   - Fix Parquet helpers’ imports and `include_file_path` behaviour.
   - Update `write_json` to use lazy `orjson` imports and clear errors.

3. **`align-datetime-and-partition-apis`**
   - Align `get_timestamp_column`, `get_timedelta_str`, and `get_partitions_from_path` with test expectations and documented semantics.

> Rationale: These changes directly affect correctness, tests, and packaging; they should be implemented and validated before building on top of them.

---

## Tier 2 – Optional Dependencies & Backwards Compatibility (Phase 2)

4. **`relax-optional-dependency-imports`**
   - Ensure all optional dependencies are imported lazily via `common.optional`.
   - Make `run_parallel` treat joblib as optional and fail with guided errors when missing.
   - Remove/redirect duplicate `check_optional_dependency`.

5. **`stabilize-utils-backwards-compat`**
   - Define supported `fsspeckit.utils` imports.
   - Add shims for legacy paths (e.g. `fsspeckit.utils.misc.Progress`).
   - Clarify that new implementation lives in domain packages, not in `utils`.

6. **`stabilize-apis-vs-tests`**
   - Realign `run_parallel` and other helpers with existing test expectations (generator support, error messages).
   - Clean up mutable defaults and unreachable code where tests already encode behaviour.

> Rationale: These changes depend on the baseline correctness from Tier 1 and ensure imports behave predictably across environments and versions.

---

## Tier 3 – Error Handling & Security (Phase 3)

7. **`standardize-error-handling-logging`**
   - Replace bare `except Exception` and silent `pass` patterns in core modules.
   - Introduce granular cleanup helpers, especially in DuckDB and dataset flows.
   - Route error reporting through `fsspeckit.common.logging` instead of `print`.

8. **`add-basic-security-validation`**
   - Add basic validation for dataset paths and compression codecs in DuckDB/PyArrow helpers.
   - Introduce credential-scrubbing for storage error logs (e.g. in AWS storage options).

> Rationale: Once APIs and imports are stable, harden error-handling and security to avoid hidden failures and sensitive data leaks.

---

## Tier 4 – Structural Refactors & Discipline (Phase 4)

9. **`refactor-large-modules-structure`**
   - Decompose `core.ext`, `datasets.pyarrow`, `datasets.duckdb`, and `core.filesystem` into smaller, focused submodules.
   - Preserve public entrypoints via re-exports/entry modules.

10. **`consolidate-schema-partition-logic`**
    - Centralise schema compatibility/unification and partition parsing into shared helper modules.
    - Ensure both DuckDB and PyArrow backends use the shared logic.

11. **`strengthen-types-and-tests`**
    - Introduce/extend static type checking for core modules.
    - Mark the package as typed once coverage is sufficient.
    - Formalise expectations around tests for refactors and new features.

> Rationale: These refactors and discipline improvements are safest and most effective once correctness, optional-dependency behaviour, error-handling, and basic security are in good shape.

---

## Suggested Milestones

- **Milestone A (Stability & Packaging):** Complete Tier 1 and run full test suite; ensure extras install correctly.
- **Milestone B (Runtime Robustness):** Complete Tier 2 and Tier 3; validate behaviour with and without optional dependencies, and under error conditions.
- **Milestone C (Refactor & Discipline):** Complete Tier 4; all major modules are decomposed, schema/partition logic is centralised, and type-checking/tests are integrated into CI.

