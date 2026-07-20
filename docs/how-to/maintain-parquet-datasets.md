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
| Change partitioning only (keep all rows) | `repartition_parquet_dataset` | `plan_parquet_repartition` |
| Cast to a caller-supplied target schema | `schema_rewrite_parquet_dataset` | `plan_parquet_schema_rewrite` |
| Compact *and* sort within each partition | `ordered_compact_parquet_dataset` | `plan_parquet_ordered_compaction` |
| Deduplicate + compact in one pass | `optimize_parquet_dataset` | `plan_parquet_optimization` |

Partition-local deduplication is the default: it only rewrites files within
each physical partition, never moving rows across partition boundaries.
Global repartitioning is deliberately explicit - it rewrites the whole dataset
into a new partition layout, which is why it requires `partition_columns`.
Pure full-dataset repartitioning (`repartition_parquet_dataset`) changes the
deduplicating; `deduplicate_and_repartition_parquet_dataset` also selects one
winner per key.

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

Timestamp-derived destination keys can be computed during the coordinated
rewrite instead of being stored in the source files:

```python
result = fs.deduplicate_and_repartition_parquet_dataset(
    "events/",
    partition_columns=["year", "year_month"],
    key_columns=["id"],
    derived_partition_columns={
        "year": ("year", "event_ts"),
        "year_month": ("strftime", "event_ts", "%Y-%m"),
    },
    partition_timezone="UTC",
)
```

Supported functions are `year`, `month`, `day`, `date`, and `strftime`.
Use a concrete timestamp column when the schema has multiple timestamp fields.
The special source name `"auto"` is accepted only when exactly one timestamp
column exists. The selected timezone and normalized definitions are recorded
in the immutable plan. Derived keys are hive path metadata and are not
duplicated in physical Parquet file schemas.


## Pure full-dataset repartitioning

When you only want a new partition layout and need to preserve **every**
source row (including exact duplicates), use pure full-dataset repartitioning.
It performs no winner selection and carries no deduplication fields:

```python
result = fs.repartition_parquet_dataset(
    "dataset/",
    partition_columns=["year", "month"],  # required: new output layout
)
```

Every source row appears exactly once in the output. Destination partition
columns may be source columns or timestamp-derived keys (same derived-key
vocabulary as global repartitioning). `max_rows_per_file` is a hard
per-destination-partition bound. The operation is full-dataset scope:
`partition_filter` is not accepted because every source file is in scope.

### Bounded-memory repartitioning

Pure full-dataset repartitioning hash-buckets rows by their destination
partition tuple, then writes each bucket as one or more
`max_rows_per_file`-bounded files. Pass `memory_budget_mb` to spill
destination buckets whose materialized size exceeds the maintenance memory
budget to a per-bucket temporary file under the maintenance workspace,
re-read through a row-batch reader during output writing:

```python
plan = fs.plan_parquet_repartition(
    "events/",
    partition_columns=["region"],
    memory_budget_mb=512,
)
print(plan.repartition_memory_budget_mb)  # 512
```

When `memory_budget_mb` is `None` (the default), the operation is
group-bounded and materializes each destination-partition bucket in memory,
matching `plan_parquet_global_repartition_deduplication`'s behavior. The
selected budget is recorded on the plan as `repartition_memory_budget_mb`
so a result without spill can be audited against the plan.

`FULL_DISTINCT_KEY_SCAN` validation is rejected at planning time: pure
full-dataset repartitioning has no key semantics.

## Rewrite the dataset schema

Schema rewrite publishes an explicitly supplied target schema under a typed
cast policy. It is distinct from maintenance schema reconciliation, which
preserves meaning through predefined lossless promotions. Use it when you
have already decided on a target schema (possibly proposed by `opt_dtype`
helpers) and need a coordinated path to publish it safely.

```python
import pyarrow as pa

target_schema = pa.schema([
    ("id", pa.int32()),       # narrowed from int64
    ("value", pa.string()),
])

result = fs.schema_rewrite_parquet_dataset(
    "dataset/",
    target_schema=target_schema,
    cast_policy="safe",
)
```

### Cast policies

| Policy | Allows | Behavior |
 | --- | --- | --- |
| `strict` | Value-preserving promotions only (widening). | Rejects narrowing at plan time. A superset of ADR-0006 lossless reconciliation. |
| `safe` (default) | Promotions **and** lossless narrowing. | PyArrow `safe=True` cast: raises on the first value that does not fit the target type. |
| `loose` | Narrowing that may truncate. | Cast proceeds, but every value is validated across the full scope before publication; any overflow or unexpected null aborts. |

### Proposing a target schema with `opt_dtype`

The publication protocol **never** calls dtype inference. Use the existing
`opt_dtype` helpers to *propose* a target schema, review it, then pass the
approved schema to `plan_schema_rewrite`:

```python
from fsspeckit.datasets.schema import opt_dtype

# Propose a schema — review the output before publishing.
proposed = opt_dtype(table, strict=False)
# ... caller reviews and adjusts proposed.schema ...
result = fs.schema_rewrite_parquet_dataset(
    "dataset/",
    target_schema=proposed.schema,
    cast_policy="safe",
)
```

### Metadata preservation

The output file schema exactly matches `target_schema`, including its
schema-level and field-level metadata. Source-file metadata is **replaced**
by the target schema's metadata — the caller controls the output schema.
If you need to preserve source metadata, copy it into `target_schema`
before calling `plan_schema_rewrite`.

### Bounded-memory schema rewrite

Schema rewrite reads source files in `RecordBatch`-sized chunks and casts
each chunk in place. Pass `memory_budget_mb` to cap the batch size for both
reading and casting:

```python
plan = fs.plan_parquet_schema_rewrite(
    "dataset/",
    target_schema=target_schema,
    memory_budget_mb=256,
)
print(plan.schema_rewrite_memory_budget_mb)  # 256
```

When `memory_budget_mb` is `None` (the default), the operation uses the
PyArrow scanner default batch size and is row-batch-bounded. The
`opt_dtype` Python-side regex loops are explicitly **not** in the
publication path.
## Partition-ordered compaction

Ordinary compaction (`compact_parquet_dataset`) combines small files but
makes **no** promise about row order. Ordered compaction produces one globally
ordered output sequence per physical partition, split into contiguous
`max_rows_per_file`-bounded chunks. Adjacent output files form a single sorted
run, which gives predicate pushdown real pruning power on range-scannable
columns (event timestamps, telemetry ids).

```python
result = fs.ordered_compact_parquet_dataset(
    "events/",
    sort_keys=["-event_ts"],       # descending; nulls first (SQL default)
    target_rows_per_file=500_000,
)
```

`sort_keys` accepts typed `SortKey` values or strings using the existing
`+col` / `-col` convention. A leading `-` sorts descending; every other name
(including `+col`) sorts ascending. String keys receive SQL-standard null
placement: nulls last for ascending, nulls first for descending. A typed
`SortKey(column, descending=..., nulls_first=...)` overrides direction and
supplies explicit null placement.

Ordering is validated **within** each output file **and across** adjacent
output-file boundaries within each partition before publication. Sorting is
stable; equal caller sort keys fall back to the physical tie-breaker
`(partition path, file path, row offset)` from the source snapshot.

`partition_filter` restricts which physical partitions are in scope; ordering
stays partition-complete within each selected partition.

### Bounded-memory ordered compaction

Ordered compaction uses an external merge sort per physical partition. Pass
`memory_budget_mb` to bound peak memory; when a partition's materialized size
exceeds the budget, each source file is sorted in memory and written as a
sorted run under `spill_directory`, then merged through a streaming k-way
merge:

```python
plan = fs.plan_parquet_ordered_compaction(
    "events/",
    sort_keys=["event_ts"],
    memory_budget_mb=512,
    spill_directory="/var/tmp/fsspeckit-spill",
)
```

When `memory_budget_mb` is `None` (the default), the operation sorts each
partition in memory and is partition-bounded; the planner rejects a partition
whose estimated size exceeds the default budget unless `spill_directory` is
supplied. For `atomic_local` the spill directory must be on the same filesystem
as the dataset root. The selected budget and spill directory are recorded on
the plan as `sort_memory_budget_mb` / `sort_spill_directory`.

`FULL_DISTINCT_KEY_SCAN` validation is rejected: ordered compaction has no key
semantics.

### What ordered compaction is *not*

Ordered compaction is distinct from several neighboring concepts:

- **Ordinary compaction** is explicitly unordered; no sort flag is added to
  `plan_compaction` or `plan_coordinated_optimization`.
- **Dedup winner ordering** (`dedup_order_by`) decides *which* row survives per
  key; ordered compaction preserves every row and only reorders.
- **z-ordering / multi-dimensional clustering** is not provided; ordered
  compaction is a single lexicographic sort key sequence per partition.
- **Business ordering across partitions** is not claimed. Ordering is
  partition-local; two partitions may overlap in sort-key ranges.

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
`plan_global_repartition_deduplication`, `plan_repartition`,
`plan_schema_rewrite`, `plan_ordered_compaction`, `plan_coordinated_optimization`.

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
