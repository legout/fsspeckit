# Plan: filesystem-confined format writes

**Status:** Ticketized, proposed for owner approval; neither this plan nor its tickets authorize implementation.

**Behavioral source:** [filesystem-confined Parquet, CSV, and JSON writes](../specs/2026-09-24-0001-filesystem-confined-format-writes.md), approved in the 2026-09-24 owner conversation for the scope and acceptance examples in that file. Approved source revision: Git blob `999170f4dcc9c08dc8c09c47e7d300a72ef8c341`; reconcile and reapprove material source changes before executing affected tickets. Planning contract v1; installed `legout/skills`, exact revision unknown.

**Capture checkpoint:** Existing `CONTEXT.md` vocabulary is sufficient; the approved design finds no qualifying new ADR. Behavior and non-goals live in the linked spec. No material design decision remains open. Execution authority is separate.

**Preflight:** Preserve unrelated `adaptive_tracker.py`, its test, and `uv.lock` work; do not reset or incorporate those changes. Before dispatch, verify that the approved source is available at its exact revision on a clean, owned base, and confirm baseline and ownership with the owner. Existing `TestWriteFilesPolarsConcat` passed (3 tests) via `.venv/bin/python -m pytest -q -o addopts='' tests/test_core_io_helpers.py -k TestWriteFilesPolarsConcat` on 2026-09-24. This does not establish the failing writer cases.

**Boundary and risk:** Public `AbstractFileSystem` methods are registered by `src/fsspeckit/core/ext/register.py`; callers supply an fsspec filesystem (including `DirFileSystem`), logical paths, and their own data. Writers must not escape that filesystem. No new auth, credentials, dependency, or attacker-controlled service boundary is introduced (`security: n/a`); overwrite and append are data-integrity risks. Keep PyArrow for Parquet, Polars for CSV formatting, and `orjson` for JSON. No IDL integration, folder-reader work, or JSONL dispatch here.

## Issue sequence

The issues own the canonical task bodies, file ownership, prerequisites, focused tests, and completion evidence; this plan only maps and orders them. They are labeled `needs-triage` pending plan approval and a clean-source handoff.

1. Spec acceptance example 1 → [#75 — Arrow/Parquet conversion](https://github.com/legout/fsspeckit/issues/75).
2. Spec acceptance example 2 → [#76 — filesystem-confined CSV and append](https://github.com/legout/fsspeckit/issues/76), after #75 because the focused test file is shared.
3. Spec acceptance example 3 → [#77 — direct JSON append separation](https://github.com/legout/fsspeckit/issues/77), after #76 for the same ownership reason.
4. Spec acceptance example 4 → [#78 — shared dispatch and overwrite confinement](https://github.com/legout/fsspeckit/issues/78), after #75–#77 because it consumes their corrected writers.

The [folder plan](https://github.com/legout/fsspeckit/blob/main/docs/plans/2026-09-24-0002-filesystem-confined-csv-jsonl-folders.md) follows for shared writer/dispatch surfaces. Sequential ownership avoids conflicting edits to `tests/test_core_ext_writers.py` and `src/fsspeckit/core/ext/io.py`.

## Candidate verification and handoff

- After assembly, run the focused writer module and existing concat/converter checks together once, then `uv run --frozen ruff check` on touched Python files. Required repository CI owns the full suite; do not rerun its matrix per ticket. Inspect the final diff for bare writer paths and `self.fs.rm` in these flows; review the high-risk CSV confinement and destructive overwrite tickets immediately after each, then review only unreviewed/integration effects at candidate assembly. Resolve only named, reachable findings under the project correction limit.
- Residual risk: no credentialed cloud run or remote append guarantee. Report the checks actually run and any filesystem capability not exercised. Owner approval of this plan and separate implementation authorization are required before dispatch; integration and publication retain their own gates.
