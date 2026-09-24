# Plan: filesystem-confined CSV and JSON(L) folders

**Status:** Ticketized, proposed for owner approval; neither this plan nor its tickets authorize implementation.

**Behavioral source:** [filesystem-confined CSV and JSON(L) folders](../specs/2026-09-24-0002-filesystem-confined-csv-jsonl-folders.md), approved by the owner after reviewing the written design in the 2026-09-24 conversation (“yes. approved”). Approved source revision: Git blob `eb30f325096aef8512facf71d0af338c396840cd`; reconcile and reapprove material source changes before executing affected tickets. Planning contract v1; installed `legout/skills`, exact revision unknown.

**Capture checkpoint:** Existing `CONTEXT.md` terminology covers files, folders, and datasets; no new term or qualifying ADR was approved. The linked spec owns behavior, empty-result shapes, non-goals, and acceptance examples. No material design decision remains open. Execution authority is separate.

**Preflight and order:** Preserve unrelated dirty source and lockfile work; do not reset or mix it into implementation. Before dispatch, verify both approved sources at their exact revisions on a clean, owned base, record a focused baseline, and obtain implementation authority. The [writer plan](https://github.com/legout/fsspeckit/blob/main/docs/plans/2026-09-24-0001-filesystem-confined-format-writes.md) must be accepted before this plan's writer/dispatch ticket consumes its corrected `write_files` and direct JSON behavior. Folder reading can be developed independently in principle, but one sequential writer avoids collisions in `src/fsspeckit/core/ext/io.py` and the shared tests.

**Boundary and risk:** The public `AbstractFileSystem` methods use caller-supplied fsspec paths and files. Preserve Polars DataFrame and raw JSON return shapes; do not substitute PyArrow datasets, which can drop columns from later files. Caller-owned paths and files introduce no new service auth, credentials, or dependency boundary (`security: n/a`). New-part collision handling has a data-integrity risk; no object-store atomicity or row-streaming promise.

## Issue sequence

The issues own the canonical task bodies, file ownership, prerequisites, focused tests, and completion evidence; this plan only maps and orders them. They are labeled `needs-triage` pending plan approval and a clean-source handoff.

1. Spec acceptance examples 1–3 → [#79 — CSV/JSON/JSONL folder reads](https://github.com/legout/fsspeckit/issues/79), after writer-dispatch #78 for shared `io.py` ownership.
2. Spec acceptance examples 4–5 → [#80 — collision-safe CSV/JSONL parts](https://github.com/legout/fsspeckit/issues/80), after #78 and #79; it consumes corrected direct writers and reads back its JSONL output.

## Candidate verification and handoff

- Run the full focused folder module once, then the writer module and existing `TestWriteFilesPolarsConcat` checks for shared-dispatch regressions; run `uv run --frozen ruff check` on touched Python files. Required repository CI owns the full suite. Review the collision-sensitive writer ticket immediately and the candidate for unreviewed reader and integration effects, without reopening settled writer findings. Do not add a separate test layer or review round without a distinct reachable failure mode.
- Residual risk: no cloud credentials, remote atomicity, automatic directory creation, or row-level streaming is established. Report those limits and the actual checks. Owner approval of this plan and separate implementation authorization are required before dispatch; integration and publication retain their own gates.
