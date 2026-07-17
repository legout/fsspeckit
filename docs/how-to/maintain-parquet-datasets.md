# Maintain Parquet Datasets

This guide covers physical dataset maintenance: compaction, deduplication,
repartitioning, and optimization. Since 0.25.0 these operations run through a
coordinator that produces an immutable, typed plan and executes it with
explicit safety guarantees, returning a typed `MaintenanceResult`.

Two workflows are available on every fsspec filesystem (registered when you
import `fsspeckit`):

- **One-call** - plan and execute in a single step
  (`fs.compact_parquet_dataset(...)`).
- **Plan-then-execute** - create a plan, inspect it, then execute
  (`fs.plan_parquet_compaction(...)` + `fs.execute_maintenance_plan(plan)`).
  Planning never mutates the dataset; it replaces the removed `dry_run=True`
  mode of the pre-0.25 helpers.

Migrating from the dictionary-returning helpers? See
[Coordinator-backed Maintenance](../migration/maintenance-api.md).

Maintenance uses the PyArrow backend by default and requires no extra beyond
the base install (PyArrow is a core dependency).

## Compact small files

Fragmented datasets (many small parquet files) slow down reads. Compaction
combines them:

```python
import fsspec

import fsspeckit  # registers the filesystem maintenance facades

fs = fsspec.filesystem("file")

result = fs.compact_parquet_dataset(
    "dataset/",
    target_rows_per_file=100_000,  # or target_mb_per_file=128
)

assert result.succeeded
print(result.actual_metrics.file_count, "files after compaction")
```

`max_rows_per_file` is a hard upper bound on output rows per file;
`target_mb_per_file` is advisory. Output defaults to Snappy compression unless
you pass `compression=`.

## Plan first, then execute

Planning is lock-free and touches nothing. The plan captures an immutable
snapshot of the source dataset, so you can review exactly what will happen:

```python
plan = fs.plan_parquet_compaction("dataset/", target_rows_per_file=100_000)

print(plan.operation)               # MaintenanceOperation.COMPACTION
print(plan.guarantee_level)         # GuaranteeLevel.ATOMIC_LOCAL (local fs)
print(len(plan.source_snapshot.files), plan.source_snapshot.total_rows)
print(len(plan.compaction_groups))  # how many output groups will be written

result = fs.execute_maintenance_plan(plan)
```

Use this workflow in production pipelines: log or persist the plan, require
approval, then execute.

## Choose the right operation

| Goal | One-call method | Planning method |
| --- | --- | --- |
| Combine small files | `compact_parquet_dataset` | `plan_parquet_compaction` |
| Remove duplicate rows, keep partitions | `deduplicate_parquet_dataset` | `plan_parquet_partition_local_deduplication` |
| Remove duplicates *and* change partitioning | `deduplicate_and_repartition_parquet_dataset` | `plan_parquet_global_repartition_deduplication` |
| Deduplicate + compact in one pass | `optimize_parquet_dataset` | `plan_parquet_optimization` |

Partition-local deduplication is the default: it only rewrites files within
each physical partition, never moving rows across partition boundaries.
Global repartitioning is deliberately explicit - it rewrites the whole dataset
into a new partition layout, which is why it requires `partition_columns`.

## Deduplicate by key

```python
result = fs.deduplicate_parquet_dataset(
    "dataset/",
    key_columns=["tenant_id", "record_id"],
    dedup_order_by=["event_timestamp"],
)
```

Key semantics:

- One row survives per key combination. With `dedup_order_by`, the **first**
  row per key wins after sorting: a bare column name sorts **ascending**, and
  a leading `-` (for example `-event_timestamp`) sorts **descending** so
  keep-first selects the most recent row per key. Ties fall back to physical
  order. Without `dedup_order_by`, physical order
  `(partition path, file path, row offset)` decides.
- Omit `key_columns` to remove rows that are identical across **all** columns
  (exact duplicates).
- Null and `NaN` key components compare equal; strings compare by exact
  stored value.
- To keep the *latest* record per key, prefix the ordering column with `-`,
  e.g. `dedup_order_by=["-event_timestamp"]`. Only a leading `-` is special;
  every other column name (including one that starts with `+`) is used
  literally as an ascending column.

## Repartition globally

```python
result = fs.deduplicate_and_repartition_parquet_dataset(
    "dataset/",
    partition_columns=["year", "month"],  # required: new output layout
    key_columns=["id"],
    dedup_order_by=["updated_at"],
)
```

This reads the entire dataset, deduplicates globally, and writes it out under
the given hive-style partition columns. Expect a full rewrite.

## Optimize (dedup + compaction)

`optimize_parquet_dataset` runs optional key-based deduplication followed by
compaction in one coordinated pass:

```python
result = fs.optimize_parquet_dataset(
    "dataset/",
    deduplicate_key_columns=["id"],   # optional
    dedup_order_by=["updated_at"],    # optional
    target_rows_per_file=500_000,
    compression="zstd",
)
```

Pass no `deduplicate_key_columns` to get pure compaction with the same
planning and validation machinery.

## Scope work with a partition filter

All planning and one-call methods accept `partition_filter`, a list of
partition-path prefixes. Only matching files are considered:

```python
result = fs.compact_parquet_dataset(
    "dataset/",
    target_mb_per_file=128,
    partition_filter=["year=2024/month=01"],
)
```

## Understand the safety guarantees

The coordinator classifies every plan into a guarantee level, recorded on both
`plan.guarantee_level` and `result.guarantee_level`:

- **`atomic_local`** (native local/POSIX filesystems): bounded advisory locks
  for cooperating fsspeckit readers/writers, staged writes in a sibling
  workspace, validation, atomic rename publication, and rollback before
  publication. Concurrent readers see either the old or the new dataset.
- **`best_effort_object_store`** (S3, GCS, Azure, and every other fsspec
  filesystem): no distributed lock, no atomic visibility, no automatic
  rollback. The coordinator writes and validates the full staged rewrite,
  copies outputs to their live keys, revalidates them, then revalidates every
  source object before deleting anything. If a source drifted mid-operation,
  it deletes **no** inputs and preserves staging and partial outputs as
  recovery artifacts on the result.

Practical guidance: on object stores, run maintenance against quiescent
datasets (no concurrent writers), and check `result.succeeded` plus
`result.recovery` before assuming the operation published.

## Interpret the result

```python
result = fs.optimize_parquet_dataset("dataset/", deduplicate_key_columns=["id"])

if not result.succeeded:
    print("Maintenance failed:", result.error)
    if result.recovery and result.recovery.workspace_path:
        print("Recovery artifacts kept at:", result.recovery.workspace_path)
else:
    metrics = result.actual_metrics
    print(f"{metrics.file_count} files, {metrics.row_count} rows, "
          f"{metrics.total_bytes} bytes")
    for phase in result.phase_outcomes:
        print(f"  {phase.phase}: {'ok' if phase.succeeded else phase.error}")
```

Key fields:

- `succeeded` / `error` - overall outcome and failure summary.
- `actual_metrics` - published output row/file/byte counts (only on success;
  `None` otherwise).
- `phase_outcomes` - ordered per-phase detail (stage, write, validate, lock,
  drift_check, publish, cleanup).
- `validation` / `publication` - staged-output validation and atomic-rename
  outcomes.
- `recovery` - workspace and backup locations retained after a failure.
- `plan` - the executed plan, including the source snapshot (use it for
  before/after comparisons).

## Use the coordinator directly

The filesystem facade always uses the PyArrow backend. To pin DuckDB (or hold
a coordinator across operations), construct one yourself:

```python
from fsspeckit.core.maintenance import DatasetMaintenanceCoordinator

coordinator = DatasetMaintenanceCoordinator("duckdb")
plan = coordinator.plan_compaction("dataset/", target_rows_per_file=100_000)
result = coordinator.execute(plan)  # pass filesystem=... for object stores
```

The planning methods mirror the facade:
`plan_compaction`, `plan_partition_local_deduplication`,
`plan_global_repartition_deduplication`, `plan_coordinated_optimization`.

## Working examples

Runnable, validated examples live in the repository:

- `examples/datasets/getting_started/06_pyarrow_maintenance.py` - stats,
  compaction planning, execution, and one-call optimization.
- `examples/maintenance/dataset_deduplication_example.py` - key-based dedup,
  exact-duplicate removal, optimization, and multi-column keys.

## Related documentation

- [Coordinator-backed Maintenance migration](../migration/maintenance-api.md) -
  replacement table for the removed dictionary-returning helpers.
- [Dataset Handlers](../dataset-handlers.md) - the read/write/merge interface
  (maintenance is no longer part of it).
- [Optimize Performance](optimize-performance.md) - caching, parallel reads,
  and type optimization.
- [Generated API: fsspeckit.core.maintenance](../api/fsspeckit.core.maintenance.md) -
  full plan/result type reference.
