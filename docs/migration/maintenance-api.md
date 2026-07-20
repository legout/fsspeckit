# Migrate to coordinator-backed maintenance

The maintenance API is a major-version migration. Dictionary-returning PyArrow
and DuckDB maintenance helpers, including their `dry_run` execution flag, are
no longer supported public APIs. Use an explicit immutable plan for review and
a typed `MaintenanceResult` for execution.

## Direct coordinator workflow

For a task-oriented walkthrough with examples, see
[Maintain Parquet Datasets](../how-to/maintain-parquet-datasets.md).

Direct callers select their backend, create a plan without mutating the dataset,
and execute that accepted plan through the single generic entry point.

```python
from fsspeckit.core.maintenance import DatasetMaintenanceCoordinator

coordinator = DatasetMaintenanceCoordinator("pyarrow")
plan = coordinator.plan_compaction(
    "dataset/", target_rows_per_file=100_000
)
# inspect plan.source_snapshot, plan.partition_scope, and plan.guarantee_level
result = coordinator.execute(plan)
assert result.succeeded
```

Pass the fsspec filesystem to both planning and execution for object-store
plans. Their result reports `best_effort_object_store`; it does not promise
atomic visibility, distributed locking, or automatic rollback.

## Filesystem convenience workflow

The fsspec extension selects the always-available PyArrow backend and records
that choice in the plan. It keeps the one-call workflow while returning the
same typed result:

```python
from fsspeckit import filesystem

fs = filesystem("file")
result = fs.compact_parquet_dataset("dataset/", target_rows_per_file=100_000)
assert result.succeeded
```

For review before mutation, use `fs.plan_parquet_compaction(...)` followed by
`fs.execute_maintenance_plan(plan)`. Corresponding planning and one-call
methods exist for partition-local deduplication and coordinated optimization.
Global deduplication is deliberately explicit through
`plan_parquet_global_repartition_deduplication` and
`deduplicate_and_repartition_parquet_dataset`. Pure full-dataset repartition
(change the partition layout without winner selection, preserving every
source row including exact duplicates) is available through
`plan_parquet_repartition` and `repartition_parquet_dataset` since #60;
it accepts the same destination partition columns and derived-key vocabulary
as global deduplication but carries no `key_columns` or `dedup_order_by`.
Caller-directed schema rewrite (publish an explicitly supplied target schema
under a typed cast policy, distinct from lossless reconciliation) is available
through `plan_parquet_schema_rewrite` and `schema_rewrite_parquet_dataset`
since #62; it accepts a `target_schema`, `cast_policy`
(`strict`/`safe`/`loose`), and the common target/validation/codec parameters.
Dtype inference is never invoked by the publication protocol — use `opt_dtype`
helpers to *propose* a target schema, then pass the approved schema to
`plan_schema_rewrite`.

## Replacements

| Removed dictionary API | Replacement |
| --- | --- |
| `compact_parquet_dataset_pyarrow` | `DatasetMaintenanceCoordinator.plan_compaction` then `execute`, or `fs.compact_parquet_dataset` |
| `deduplicate_parquet_dataset_pyarrow` | `plan_partition_local_deduplication` then `execute`, or `fs.deduplicate_parquet_dataset` |
| `optimize_parquet_dataset_pyarrow` | `plan_coordinated_optimization` then `execute`, or `fs.optimize_parquet_dataset` |
| `compact_parquet_dataset_duckdb` | Construct `DatasetMaintenanceCoordinator("duckdb")`, then plan and execute |
| Any `dry_run=True` maintenance call | The corresponding `plan_*` method |

`MaintenanceResult` replaces dictionary indexing. Its `plan`, `phase_outcomes`,
`validation`, `publication`, `recovery`, and `actual_metrics` fields provide the
observed execution details.

## Semantic changes

- `dedup_order_by` accepts the legacy `-column` descending prefix: a leading
  `-` sorts that column descending so the keep-first-wins rule selects the
  most recent row per key. Bare column names sort ascending. Physical order
  breaks ties. For example, `dedup_order_by=["-event_timestamp"]` keeps the
  latest record per key.
- Byte-size targets (`target_mb_per_file`) are advisory;
  `max_rows_per_file` is a hard upper bound on output rows per file.
- Rewrites without an explicit codec target use Snappy.
