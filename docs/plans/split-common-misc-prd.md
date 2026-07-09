# Split common/misc.py PRD

## Summary

Split `common/misc.py` (703 lines) into three focused modules: `common.parallel`
(parallelism helpers), `common.sync` (filesystem sync), and fold
`get_partitions_from_path` into the existing `common.partitions`. Remove the
"misc" catch-all.

## Problem

`common/misc.py` mixes three unrelated concepts behind one wide interface:
parallelism (`run_parallel` and its helpers), path utilities
(`get_partitions_from_path`, `path_to_glob`), and filesystem sync
(`sync_dir`, `sync_files`, `check_fs_identical`). The name "misc" signals "we
gave up on cohesion." Each concept independently satisfies the deletion test —
deleting the module concentrates complexity across callers, so it's earning its
keep, but at the wrong granularity.

## Goals

1. Extract `run_parallel` and its helpers into `common.parallel`.
2. Extract `sync_dir`, `sync_files`, `check_fs_identical`, and their helpers into
   `common.sync`.
3. Move `get_partitions_from_path` and `path_to_glob` into the existing
   `common.partitions`.
4. Delete `common/misc.py`.
5. Update all callers and the `utils/` backwards-compat façade.

## Non-Goals

- Do not change any function signatures or behavior.
- Do not split `common/security.py` or `common/datetime.py`.
- Do not change the `utils/` façade's public exports (it re-exports the same
  names from new locations).

## Implementation Plan

### Phase 1: Extract common.parallel

- Move `run_parallel`, `_prepare_parallel_args`, `_execute_parallel_with_progress`,
  `_execute_parallel_without_progress`, and the `Progress` wrapper into
  `common/parallel.py`.
- Update `common/__init__.py` to import `run_parallel` from `.parallel`.

### Phase 2: Extract common.sync

- Move `sync_dir`, `sync_files`, `check_fs_identical`, `_get_root_fs`,
  `server_side_copy_file`, `copy_file`, `delete_file` into `common/sync.py`.
- Update `common/__init__.py` to import `sync_dir`, `sync_files` from `.sync`.

### Phase 3: Move path utilities into common.partitions

- Move `get_partitions_from_path` and `path_to_glob` into `common/partitions.py`.
- Update `common/__init__.py` to import `get_partitions_from_path` from
  `.partitions` (where it logically belongs alongside other partition helpers).

### Phase 4: Delete common/misc.py and update callers

- Delete `common/misc.py`.
- Update all callers importing from `fsspeckit.common.misc` to import from the
  new module locations.
- Update `utils/__init__.py` to import from new locations.
- Update `core/ext/__init__.py` and `datasets/` callers.

### Phase 5: Verify

- `python scripts/check_layering.py` passes.
- `uv run ruff check` passes.
- Existing tests pass.

## Acceptance Criteria

- `common/misc.py` does not exist.
- `common/parallel.py` contains `run_parallel` and its helpers.
- `common/sync.py` contains `sync_dir`, `sync_files`, and helpers.
- `common/partitions.py` contains `get_partitions_from_path` and `path_to_glob`.
- No source file imports from `fsspeckit.common.misc`.
- `common/__init__.py` re-exports `run_parallel`, `sync_dir`, `sync_files`, and
  `get_partitions_from_path` from their new locations.
- `python scripts/check_layering.py` passes.
- `uv run ruff check` passes for changed files.
- `utils/` façade exports remain compatible.

## Risks

### Risk: Import cycle

Mitigation: `common.parallel` and `common.sync` import only from stdlib and
fsspec — no fsspeckit-internal dependencies. No cycle possible.

### Risk: Missed caller

Mitigation: grep for `fsspeckit.common.misc` across the entire codebase after
the move. The layering checker validates import rules.

## References

- [ADR-0001: Import Layering Rules for Package Architecture](../adr/0001-layering-rules.md)
- [ADR-0003: Common Layer Independence](../adr/0003-common-layer-independence.md)
