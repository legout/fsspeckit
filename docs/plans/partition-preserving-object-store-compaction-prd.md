# Partition-Preserving Object-Store Compaction PRD

> **Status:** Proposed. Tracks GitHub issue
> [#54](https://github.com/legout/fsspeckit/issues/54).
> Implements user story #11 of the
> [coordinated dataset maintenance spec](coordinated-dataset-maintenance-spec.md)
> for the `best_effort_object_store` guarantee level, closing the deferral
> recorded in `DatasetMaintenanceCoordinator.plan_compaction`.

## Summary

Make compaction on the `best_effort_object_store` path (S3, GCS, Azure, and
every other non-local fsspec filesystem) **partition-preserving by default**,
matching the `atomic_local` path and the already-correct best-effort
deduplication and optimization paths. The change is a planning swap plus a
live-key placement fix — all machinery already exists in
`fsspeckit.core.maintenance`.

## Problem

Today, `fs.compact_parquet_dataset(...)` on a partitioned dataset flattens it
when the filesystem is not native-local:

```
BEFORE: dataset/country=DE/year=2023/part_0.parquet   (8 files, 4 hive partitions)
AFTER:  dataset/compacted-b9faee3ac86a47e0-0000-0000.parquet   (1 file, root)
```

A hive read afterwards loses the partition columns entirely
(`['id', 'val', 'country', 'year']` → `['id', 'val']`). Rows survive but their
partition identity is silently destroyed — wrong query results and no
partition pruning for every downstream consumer.

Root cause, two layers:

1. **Planning** (`maintenance.py:5021`): the best-effort branch uses
   partition-agnostic `plan_compaction_groups`, bin-packing files by size
   across partition boundaries. The in-code comment defers this explicitly:
   *"keeps its own planning policy until its partition-subtree work lands."*
2. **Execution** (`_execute_best_effort_compaction`, `maintenance.py:3429`):
   live keys are `posixpath.join(dataset_root, f"compacted-{run_id}-...")` —
   always the dataset root, never the group's partition directory.

The failure is silent: `MaintenanceResult.succeeded` is `True` and nothing in
the plan or result signals that the layout changed.

## Goals

1. Compaction preserves input partition layout on **every** filesystem, by
   default. Each compaction group's output is published under its partition
   directory; a hive read after compaction still exposes the partition
   columns.
2. Planning for best-effort compaction is partition-scoped, reusing
   `_plan_partition_local_compaction_groups` (the same planner `atomic_local`
   uses).
3. Best-effort publication places live keys under
   `posixpath.join(dataset_root, partition_dir, name)`, mirroring
   `_execute_best_effort_partition_local_deduplication` and
   `_execute_best_effort_coordinated_optimization`, which already do this.
4. No change to the public facade signatures (`plan_parquet_compaction`,
   `compact_parquet_dataset`) — this is a behavior fix, not an API change.

## Non-Goals

- No new "global flatten" opt-in flag. Cross-partition compaction, if ever
  desired, follows ADR-0006's rule (global operations are explicit, separate
  plan types) and is a future proposal.
- No changes to the `atomic_local` path (already partition-preserving).
- No changes to best-effort deduplication or coordinated optimization
  (already partition-preserving).
- No manifest/pointer protocol for object-store atomicity (deferred by
  ADR-0006).
- No change to staging-prefix layout, validation, or drift-check phases
  beyond what the live-key placement requires.
- No changes to goal-aware file eligibility (skip logic). Planning already
  drops singleton groups and bypasses at-target files; surfacing skip
  reasons in plans and honoring codec-change intent is tracked separately
  in issue [#55](https://github.com/legout/fsspeckit/issues/55).

## Proposed Interface

No public API change. Internal changes only:

```python
# DatasetMaintenanceCoordinator.plan_compaction (maintenance.py:5021)
# Before: guarantee-level branch selects planner.
# After: one partition-scoped planner for both guarantee levels.
compaction_groups = _plan_partition_local_compaction_groups(
    file_stats, snapshot.dataset_path, target_mb_per_file, target_rows_per_file
)
```

```python
# _execute_best_effort_compaction (maintenance.py:3429)
# Before: live_path = posixpath.join(dataset_root, f"compacted-{run_id}-{group_idx:04d}-{chunk_idx:04d}.parquet")
# After: publish under the group's partition directory.
partition_dir = _group_partition_dir(group, dataset_root)
staged_path = posixpath.join(staging_prefix, partition_dir, name)
live_path = posixpath.join(dataset_root, partition_dir, name)
```

`PartitionScope` on the plan already records the affected partitions; no
plan-type changes are required.

## Implementation Plan

### Phase 1: Partition-scoped planning for best-effort

- Remove the guarantee-level planner branch in `plan_compaction`; call
  `_plan_partition_local_compaction_groups` unconditionally.
- Update the in-code comment that documents the deferral.
- Unit test: planning on a multi-partition memory-fs dataset yields only
  single-partition groups (no group spans two partition directories).

### Phase 2: Partition-subtree publication

- In `_execute_best_effort_compaction`, compute each group's partition
  directory via `_group_partition_dir` and build staged/live keys under it,
  mirroring the deduplication path.
- Keep staging, validate, copy, per-key validation, drift-check, and cleanup
  phases unchanged.
- Unit/integration test: the issue #54 reproduction — memory-fs hive dataset
  compacts to partitioned output; `pyarrow.dataset` hive read still exposes
  `country`/`year`; row counts match the source snapshot.

### Phase 3: Coverage and docs

- Add mixed-size partitions (one partition already above target size →
  untouched; others compacted) and `partition_filter` scoping tests.
- Confirm object-store drift semantics: sources are revalidated per object
  before deletion, unchanged by this work.
- Update `docs/how-to/maintain-parquet-datasets.md` if any caveat about
  object-store layouts was added; none required — the guide already describes
  partition-local behavior as the default.

### Phase 4: Verify

- `uv run pytest tests/test_core/ -q` green (existing maintenance suites
  cover both guarantee levels).
- `python scripts/check_layering.py` passes.
- `uv run ruff check` passes for changed files.

## Acceptance Criteria

- The issue #54 reproduction produces partitioned output on a memory
  filesystem, and the hive read-back exposes the original partition columns.
- No compaction group spans more than one partition directory on either
  guarantee level.
- `MaintenanceResult.actual_metrics` reflects the same row counts; only file
  placement changes.
- All existing maintenance tests pass without modification (or with explicit,
  reviewed updates where a test asserted the flattening).
- `docs/plans/coordinated-dataset-maintenance-spec.md` user story #11 is
  honored on both guarantee levels.

## Risks

### Risk: More output files on highly fragmented partitions

Partition-scoped groups can produce one output per small partition instead of
one global output, increasing file counts on datasets with many tiny
partitions.

Mitigation: this is the accepted `atomic_local` behavior since #38 and the
semantics users expect from partition-aware compaction; byte-size targets
remain advisory and `max_rows_per_file` still bounds file size from below.

### Risk: A test or downstream user depends on the flattening

Mitigation: the flattening was never documented as a feature; the spec and
ADR-0006 both describe partition-local as the intended default. Review any
test asserting root-level live keys and update it to assert partition
placement instead.

### Risk: Staging-prefix layout change breaks recovery tooling

Mitigation: the deduplication path already stages under
`staging_prefix/partition_dir/`; compaction adopting the same layout makes
recovery handling more uniform, not less.

## References

- GitHub issue [#54](https://github.com/legout/fsspeckit/issues/54) —
  reproductions and root-cause analysis.
- GitHub issue [#55](https://github.com/legout/fsspeckit/issues/55) —
  follow-up: skip-reason observability and codec-migration intent in
  compaction planning.
- [Coordinated dataset maintenance spec](coordinated-dataset-maintenance-spec.md) —
  user story #11 (partition-subtree compaction).
- [ADR-0006: Coordinated dataset maintenance guarantees](../adr/0006-coordinated-dataset-maintenance.md) —
  "partition-local is the default; global is explicit".
- `src/fsspeckit/core/maintenance.py`: `plan_compaction` (planner branch,
  ~5021), `_plan_partition_local_compaction_groups` (1433),
  `_group_partition_dir` (1903), `_execute_best_effort_compaction` (3429),
  `_execute_best_effort_partition_local_deduplication` (3896, the
  partition-preserving publication pattern).
