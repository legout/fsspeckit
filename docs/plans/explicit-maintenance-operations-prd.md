# Explicit Maintenance Operations PRD

> **Status:** Proposed. Tracks GitHub issue
> [#59](https://github.com/legout/fsspeckit/issues/59). Unblocks child issues
> [#60](https://github.com/legout/fsspeckit/issues/60) (pure repartitioning),
> [#61](https://github.com/legout/fsspeckit/issues/61) (ordered compaction),
> [#62](https://github.com/legout/fsspeckit/issues/62) (schema rewrite), and
> [#63](https://github.com/legout/fsspeckit/issues/63) (ADR reconciliation and
> vocabulary). The architecture decision that reconciles these operations with
> ADR-0006 is recorded in
> [ADR-0007](../adr/0007-explicit-repartition-ordered-compaction-schema-rewrite.md);
> this PRD is the product-level commitment that the ADR encodes.

## Summary

Extend `DatasetMaintenanceCoordinator` with three explicit, operation-specific
physical rewrites that ADR-0006 deliberately does not provide as side effects
of ordinary compaction or coordinated optimization:

1. **Pure full-dataset repartitioning** — a new physical layout without any
   winner selection; every source row, including exact duplicates, is preserved.
2. **Partition-ordered compaction** — compaction whose output is one globally
   ordered sequence per affected physical partition, split into contiguous
   `max_rows_per_file`-bounded chunks.
3. **Caller-directed schema rewrite** — publication of an explicitly supplied
   target schema under a typed cast policy, distinct from lossless maintenance
   schema reconciliation.

Each operation gets its own immutable plan/result types and reuses the shared
plan → stage → validate → publish → recover lifecycle. No operation is added
as a flag on coordinated optimization, and none weakens ADR-0006's contract
that ordinary maintenance output is unordered and that maintenance
reconciliation is inference-free.

## Problem

ADR-0006 deliberately leaves three capabilities outside the coordinated
maintenance surface:

- **Ordinary maintenance output is unordered.** Callers that need a
  query-prunable physical order have no coordinated path today.
- **Global repartitioning is available only as an explicit deduplication
  operation.** Callers that only want a new partition layout cannot use it
  without also changing row multiplicity, because the only existing global
  plan (`GlobalRepartitionDeduplicationPlan`) is coupled to winner selection.
- **Maintenance schema reconciliation is lossless and inference-free.**
  Callers that want to publish a narrower target schema (for example to
  optimize storage cost) have no coordinated path; they rewrite files
  incrementally and can leave mixed schemas after failure.

The three operations are compatible with ADR-0006 only if they are separate,
explicit plan types with their own semantics. Adding them as flags on
coordinated optimization, or routing them through deduplication, would
silently weaken the guarantees ADR-0006 records.

## Product decisions

These decisions are locked down by #59 and become the contract the child
issues implement:

1. **Pure repartitioning preserves all rows, including exact duplicates, and
   performs no implicit winner selection.** It is a physical rewrite, not a
   deduplication.
2. **Ordered compaction means a globally ordered output sequence within each
   affected physical partition**, not merely a per-output-file sort.
   Adjacent output files form a single sorted run; ordinary compaction
   remains explicitly unordered.
3. **Ordinary compaction remains unordered.** No sort, z-order, clustering,
   or implicit repartitioning is added to the existing `CompactionPlan`.
4. **Dtype inference and target-schema selection are separate from physical
   publication.** Schema-rewrite execution receives an explicit target
   schema; the maintenance publication protocol never calls dtype inference.
5. **All three operations expose local atomic vs object-store best-effort
   guarantees through typed plans and results**, reusing ADR-0006's
   `atomic_local` and `best_effort_object_store` profiles unchanged.
6. **Execution has an explicit memory/spill contract.** Whole-dataset or
   whole-partition materialization is never an undocumented requirement;
   each operation declares its bounded-memory strategy and a configurable
   memory budget.

## Public plan/result names and semantics

Public names follow the existing `MaintenancePlan` / `MaintenanceResult`
hierarchy. Operation-specific plans subclass `MaintenancePlan` and add
operation-specific fields; operation-specific results subclass
`MaintenanceResult` (and `BestEffortCompactionResult` for the object-store
lane), mirroring `GlobalRepartitionDeduplicationPlan` /
`BestEffortGlobalRepartitionDeduplicationResult`.

### 1. Pure repartitioning

```python
@dataclass(frozen=True)
class RepartitionPlan(MaintenancePlan):
    """Immutable plan for a pure full-dataset repartition.

    Preserves every source row, including exact duplicates. Performs no
    winner selection. Destination partition columns may be source columns
    or validated derived partition keys; partition columns are path
    metadata only and are not stored in physical file schemas.
    """
    partition_columns: tuple[str, ...]
    derived_partition_keys: tuple[DerivedPartitionKey, ...] = ()
    repartition_memory_budget_mb: int | None = None
    repartition_groups: tuple[CompactionGroup, ...] = ()

@dataclass(frozen=True)
class RepartitionResult(MaintenanceResult):
    """Typed result of executing a RepartitionPlan."""

@dataclass(frozen=True)
class BestEffortRepartitionResult(BestEffortCompactionResult, RepartitionResult):
    """Repartition result with object-store recovery details."""
```

Coordinator entry point:

```python
def plan_repartition(
    self,
    dataset_path: str,
    partition_columns: list[str],
    filesystem: AbstractFileSystem | None = None,
    target_mb_per_file: int | None = None,
    target_rows_per_file: int | None = None,
    validation_level: ValidationLevel | str | None = None,
    codec: str | None = None,
    derived_partition_columns: dict[str, tuple[str, ...]] | None = None,
    partition_timezone: str = "UTC",
    memory_budget_mb: int | None = None,
) -> RepartitionPlan: ...
```

Semantics locked down:

- No `key_columns`, no `dedup_order_by`. The plan does not carry dedup fields.
- Destination partition columns may be source columns or validated
  `DerivedPartitionKey`s; the existing `_normalize_derived_partition_keys`
  helper is reused unchanged.
- Destination partition columns are not stored in physical file schemas
  (`_repartition_file_schema` applies unchanged).
- `max_rows_per_file` remains a hard per-destination-partition bound.
- The plan records source snapshot, destination partition columns, schema,
  codec, backend, validation level, memory budget, and guarantee profile.
- Every source file is in scope; `partition_filter` is rejected. Unrelated
  partitions and source files are replaced exactly as the full-dataset plan
  specifies.

### 2. Partition-ordered compaction

```python
@dataclass(frozen=True)
class SortKey:
    """One sort column for ordered compaction."""
    column: str
    descending: bool = False
    nulls_first: bool = False

@dataclass(frozen=True)
class OrderedCompactionPlan(MaintenancePlan):
    """Immutable plan for partition-ordered compaction.

    Output chunks are contiguous slices of one partition-level sorted
    sequence per affected physical partition. Does not claim global
    business ordering across partitions.
    """
    sort_keys: tuple[SortKey, ...]
    ordered_groups: tuple[CompactionGroup, ...] = ()
    sort_memory_budget_mb: int | None = None
    sort_spill_directory: str | None = None

@dataclass(frozen=True)
class OrderedCompactionResult(MaintenanceResult):
    """Typed result of executing an OrderedCompactionPlan."""

@dataclass(frozen=True)
class BestEffortOrderedCompactionResult(
    BestEffortCompactionResult, OrderedCompactionResult
):
    """Ordered compaction result with object-store recovery details."""
```

Coordinator entry point:

```python
def plan_ordered_compaction(
    self,
    dataset_path: str,
    sort_keys: list[SortKey | str],
    filesystem: AbstractFileSystem | None = None,
    target_mb_per_file: int | None = None,
    target_rows_per_file: int | None = None,
    partition_filter: list[str] | None = None,
    validation_level: ValidationLevel | str | None = None,
    codec: str | None = None,
    memory_budget_mb: int | None = None,
    spill_directory: str | None = None,
) -> OrderedCompactionPlan: ...
```

Semantics locked down:

- Ordering scope is the complete affected physical partition, not the
  individual output file.
- Output chunks are contiguous slices of one partition-level sorted
  sequence; adjacent files form a single sorted run.
- The result does not claim global business ordering across partitions.
- Sorting is stable and deterministic for equal keys. The physical
  tie-breaker is the ADR-0006 tuple `(partition path, file path, row
  offset)` captured in the source snapshot, applied only after the
  caller-supplied sort keys tie.
- String `sort_keys` items accept the existing `+col` / `-col` convention
  used by `parse_dedup_order_by`; a typed `SortKey` overrides direction
  and adds explicit null placement (default `nulls_last` for ascending,
  `nulls_first` for descending, matching SQL semantics).
- The planner must include every file required to establish the claimed
  partition order. A `partition_filter` restricts which physical
  partitions are in scope; ordering is still partition-complete within
  each selected partition.
- Ordinary `CompactionPlan` remains unordered. No sort flag is added to
  `plan_compaction` or `plan_coordinated_optimization`.

### 3. Caller-directed schema rewrite

```python
class CastPolicy(str, Enum):
    """Cast policy for schema rewrite."""
    STRICT = "strict"          # allow only value-preserving promotions
    SAFE = "safe"              # allow promotions and lossless narrowing
    LOOSE = "loose"            # allow narrowing; validate full scope before publish

@dataclass(frozen=True)
class SchemaRewritePlan(MaintenancePlan):
    """Immutable plan for a caller-directed schema rewrite.

    The target schema is supplied by the caller. Dtype inference is not
    invoked. The plan exposes source schema, target schema, and the set of
    fields whose type changes.
    """
    target_schema: Any  # pa.Schema
    source_schema: Any  # pa.Schema
    cast_policy: CastPolicy
    changed_fields: tuple[str, ...]
    schema_rewrite_memory_budget_mb: int | None = None

@dataclass(frozen=True)
class SchemaRewriteResult(MaintenanceResult):
    """Typed result of executing a SchemaRewritePlan."""

@dataclass(frozen=True)
class BestEffortSchemaRewriteResult(
    BestEffortCompactionResult, SchemaRewriteResult
):
    """Schema rewrite result with object-store recovery details."""
```

Coordinator entry point:

```python
def plan_schema_rewrite(
    self,
    dataset_path: str,
    target_schema: Any,
    cast_policy: CastPolicy | str = CastPolicy.SAFE,
    filesystem: AbstractFileSystem | None = None,
    target_mb_per_file: int | None = None,
    target_rows_per_file: int | None = None,
    partition_filter: list[str] | None = None,
    validation_level: ValidationLevel | str | None = None,
    codec: str | None = None,
    memory_budget_mb: int | None = None,
) -> SchemaRewritePlan: ...
```

Semantics locked down:

- The caller supplies the target schema and cast policy. The plan exposes
  source schema, target schema, and the tuple of fields whose type
  changes.
- Planning validates target fields against the full rewrite scope, not
  only an inference sample. Every source file's schema is checked.
- `STRICT` allows only value-preserving promotions (superset of ADR-0006
  lossless reconciliation). `SAFE` adds lossless narrowing
  (`int64 → int32` when every value fits). `LOOSE` allows narrowing that
  may truncate, but validates every value across the full scope before
  publication; any value that would overflow or become null aborts the
  plan before any live mutation.
- Failed or lossy casts abort before publication and retain actionable
  recovery information through the standard recovery artifacts.
- Partition columns remain path metadata and retain compatible logical
  types; partition column types may be widened but never narrowed
  (narrowing a partition column would invalidate the path encoding).
- Row counts, null counts, schema/field metadata policy, compression,
  and output bounds are validated.
- The operation is coordinated and never leaves a partially converted
  live dataset for cooperating readers.
- The operation does not call or hide dtype inference. Documentation
  explains how callers may use existing `opt_dtype` helpers to *propose*
  a target schema; the publication protocol only accepts an explicit
  target schema supplied by the caller.

## Guarantee profiles

All three operations reuse ADR-0006's guarantee classification unchanged.
The coordinator selects a profile automatically from the filesystem and
records it in the plan and result.

| Operation | `atomic_local` | `best_effort_object_store` |
|---|---|---|
| Pure repartition | Sibling workspace, partition-subtree staging, validated local rename publication, rollback before publication, immediate successful-backup cleanup. Full-dataset scope: every source partition subtree is staged and swapped under one exclusive maintenance window. | Staged rewrite at a sibling prefix, validate, copy every output to its live key, validate every planned live key individually, revalidate every planned source object before deletion, delete no source on drift, retain staging and partial live outputs as recovery artifacts on failure. |
| Ordered compaction | Same partition-subtree publication as compaction; spill directory, when configured, must be on the same filesystem as the dataset root. | Same staged-copy publication as compaction; the spill directory is caller-managed and is not part of recovery artifacts. |
| Schema rewrite | Same partition-subtree publication. Cast validation across the full scope is a publication gate; rollback before publication is mandatory if any post-write validation fails. | Same staged-copy publication. Cast validation across the full scope runs on staged output before any live copy; no source deletion occurs until the staged rewrite validates and sources revalidate. |

Object-store disclaimer: every best-effort result carries the existing
`BEST_EFFORT_CONCURRENCY_DISCLAIMER`. No operation adds a distributed
lock, atomic visibility, or automatic object-store rollback.

## Memory and spill behavior

Each operation declares a bounded-memory strategy and accepts an optional
`memory_budget_mb`. Whole-dataset or whole-partition materialization is
never an undocumented requirement.

### Pure repartitioning

- Strategy: hash-bucket by destination partition tuple, then stream each
  bucket through a row-batch reader. Each destination partition is
  written by `pyarrow.dataset.write_dataset` with `max_rows_per_file`
  honored as a hard per-file bound.
- Bounded by: per-bucket peak memory, configurable via
  `memory_budget_mb`. Buckets larger than the budget spill to a
  per-bucket temporary file under the maintenance workspace and are
  re-read during output writing.
- Default: when `memory_budget_mb` is `None`, the operation uses the
  existing PyArrow scanner batch size and behaves group-bounded, matching
  `GlobalRepartitionDeduplicationPlan`'s current behavior. The plan
  records the selected behavior.

### Ordered compaction

- Strategy: external merge sort per physical partition. Each source file
  is read, sorted in memory by the sort keys plus the physical
  tie-breaker, and written as a sorted run file. Run files are merged
  through a k-way merge that streams output into `max_rows_per_file`
  chunks.
- Bounded by: `memory_budget_mb`, which caps both per-run sort memory
  and the k-way merge fan-in. When the budget is exceeded, run files
  are smaller and merge fan-in is reduced.
- Spill: `spill_directory` is required when the affected partition is
  known to exceed `memory_budget_mb`; the planner records whether the
  configured budget and partition size require spill. For
  `atomic_local`, the spill directory must be on the same filesystem as
  the dataset root so that rename-into-place remains atomic.
- Default: when `memory_budget_mb` is `None`, the operation sorts each
  partition in memory and is partition-bounded; the planner rejects a
  partition whose estimated decoded size exceeds the default budget
  unless a spill directory is supplied.

### Schema rewrite

- Strategy: batch stream over the current file set. Source files are
  read in `RecordBatch`-sized chunks; each chunk is cast in place under
  the configured `CastPolicy` and appended to a staged output writer.
- Bounded by: `memory_budget_mb`, which sets the batch size for both
  reading and casting. No whole-column materialization; the existing
  `opt_dtype` helpers' Python-side regex loops are explicitly not in
  the publication path.
- Validation: every cast is checked across the full scope before any
  live mutation. `LOOSE` narrowing runs once over the staged output to
  confirm no value overflowed or became null unexpectedly; any failure
  aborts before publication.
- Default: when `memory_budget_mb` is `None`, the operation uses the
  PyArrow scanner default batch size and is row-batch-bounded.

## Validation levels

All three operations honor the existing `ValidationLevel` enum and add
operation-specific invariants on top.

| Operation | Default `STAGED_FILE` validation | Optional `FULL_DISTINCT_KEY_SCAN` |
|---|---|---|
| Pure repartition | Readable staged files; schema compatibility; destination partition placement; per-source and per-destination row-count invariant (every source row appears exactly once in the output). | Not applicable; pure repartition has no key semantics. A `ValueError` is raised if a caller requests it. |
| Ordered compaction | Readable staged files; schema compatibility; partition placement; row-count invariant; per-file sort order; sort order across adjacent output-file boundaries within each partition. | Not applicable; ordered compaction has no key semantics. A `ValueError` is raised if a caller requests it. |
| Schema rewrite | Readable staged files; target schema exactly matches; per-field cast validation across the full scope; row-count and null-count invariants; partition column type compatibility; compression and metadata policy. | Reused as "full-scope cast validation" semantics: every value is checked, not just a sample. Default for `LOOSE`; opt-in for `SAFE`; mandatory for `STRICT`. |

## pydala2 integration requirements

[`legout/pydala2`](https://github.com/legout/pydala2) consumes
fsspeckit's maintenance façades to compact, deduplicate, and optimize
stored datasets. The integration requirements locked down here are
delivered by the child issues; this section is the contract the pydala2
maintainer can plan against.

### 1. Pure repartition (delivered by #60)

- pydala2 performs rolling time-partition rewrites (for example
  re-bucketing by `year`/`month` derived from a `timestamp` column)
  without winner selection. Today it works around the absence of a pure
  repartition by calling `plan_global_repartition_deduplication` with a
  no-op key and discarding the deduplication statistics, which is
  fragile and reports misleading metrics.
- Requirement: a public `RepartitionPlan` whose result reports actual
  output partitions, file count, byte size, and rows preserved; no
  deduplication fields on the result.
- Migration: pydala2 swaps its rolling-repartition call site from
  `plan_global_repartition_deduplication` to `plan_repartition`. The
  existing call site remains supported; it is not removed.

### 2. Partition-ordered compaction (delivered by #61)

- pydala2 stores range-prunable datasets (event logs, telemetry) and
  benefits from physical ordering within each partition for predicate
  pushdown. Today it has no coordinated path; downstream consumers see
  unspecified row order.
- Requirement: a public `OrderedCompactionPlan` that accepts typed
  `SortKey` values, validates order within and across output files, and
  reports actual min/max statistics per output file when available.
- Migration: pydala2 adds an ordered-compaction call site alongside its
  existing compaction call site; ordinary compaction remains the default
  for unordered datasets.

### 3. Schema rewrite (delivered by #62)

- pydala2 uses the existing `opt_dtype` helpers to *propose* a target
  schema to a user and then needs a coordinated path to publish that
  schema. Today it casts files incrementally and can leave mixed schemas
  after failure.
- Requirement: a public `SchemaRewritePlan` that accepts an explicit
  target schema and `CastPolicy`, validates the full scope before
  publication, and reports actual output schema and null counts per
  field.
- Migration: pydala2 wraps its `opt_dtype` proposal flow with a
  `plan_schema_rewrite` call. `opt_dtype` remains a proposal helper and
  is never called by the maintenance publication protocol.

### Cross-linked documentation

- The pydala2 maintainer updates the pydala2 maintenance guide to
  reference these three fsspeckit operations when #60, #61, and #62
  ship.
- The fsspeckit migration guide
  (`docs/migration/maintenance-api.md`) gains a section pointing pydala2
  users at the new operations, mirroring the existing
  `DatasetMaintenanceCoordinator` migration section.

## Non-goals

- No z-ordering, multi-dimensional clustering, or business-ordering
  guarantee across partitions.
- No target-schema inference inside the publication protocol. The
  `opt_dtype` helpers remain proposal-only.
- No implicit repartitioning, sorting, or schema rewrite as a side
  effect of `plan_coordinated_optimization`.
- No new guarantee profile. `atomic_local` and
  `best_effort_object_store` from ADR-0006 are reused unchanged.
- No manifest or version-pointer protocol for object-store atomicity;
  ADR-0006's deferral stands.
- No long-lived parallel `*_v2` APIs. The new plan/result types are
  additive at the same major version that already ships
  `MaintenancePlan` and `MaintenanceResult`.
- No removal of the existing `plan_global_repartition_deduplication`
  API; it remains the canonical deduplicating repartition.

## Acceptance criteria mapping

Maps each #59 acceptance criterion to the deliverable that satisfies it.

| #59 criterion | Delivered by |
|---|---|
| The child operation specifications are agreed and linked to this issue. | This PRD; #59 issue body task list linking #60, #61, #62, #63. |
| ADR 0006 is explicitly reconciled by a new accepted ADR rather than silently weakened. | #63. |
| Public plan/result names and semantics distinguish repartition, ordered compaction, and schema rewrite. | "Public plan/result names and semantics" above. |
| Local and object-store guarantee profiles are specified for each operation. | "Guarantee profiles" above. |
| Memory/spill behavior and validation levels are documented. | "Memory and spill behavior" and "Validation levels" above. |
| Downstream integration requirements for `legout/pydala2` are documented and cross-linked. | "pydala2 integration requirements" above. |

## Risks

### Risk: Operation-specific plan types proliferate

Three new plan types and three new result types increase the public
surface.

Mitigation: each plan type is a strict subclass of `MaintenancePlan`
and reuses the existing `execute(plan)` dispatch. The result type
hierarchy mirrors the existing `BestEffortGlobalRepartitionDeduplicationResult`
pattern. The operation set is closed: the three operations in this PRD
are the complete set of explicit data-transforming operations planned
for this major version.

### Risk: Memory/spill contracts are tested inadequately

Bounded-memory promises are easy to claim and hard to verify.

Mitigation: each child issue (#60, #61, #62) ships a large-partition
test that exercises the documented spill path, not just a happy-path
small-dataset test. The validation level and memory budget selected at
planning time are recorded on the plan so that a result without spill
behavior can be audited against the plan.

### Risk: pydala2 integration diverges from fsspeckit's locked names

The pydala2 maintainer may adopt names that drift from this PRD.

Mitigation: this PRD is merged before any child implementation ships,
and the pydala2 maintainer is a reviewer on each child PR. The
migration-guide section named above is the single source of truth for
the pydala2 call sites.

### Risk: Schema rewrite is misused as a license for lossy publication

A `LOOSE` cast policy can narrow types and lose precision.

Mitigation: `LOOSE` validates the full scope before publication and
aborts on any value that would overflow or become null unexpectedly.
`STRICT` is the default-strict option; `SAFE` is the documented
default; `LOOSE` is opt-in. The result reports the cast policy used.

## Implementation plan

The PRD itself does not implement code. Child issues deliver the code:

- #60 — pure full-dataset repartitioning.
- #61 — partition-ordered compaction.
- #62 — caller-directed schema rewrite.
- #63 — ADR reconciliation with ADR-0006 and `CONTEXT.md` vocabulary.

Each child issue references this PRD as its locked-down specification
and may not change the public names, guarantee profiles, memory/spill
contracts, or pydala2 integration points without amending this PRD
first.

## References

- GitHub issue [#59](https://github.com/legout/fsspeckit/issues/59) —
  this PRD's tracking issue.
- GitHub issue [#60](https://github.com/legout/fsspeckit/issues/60) —
  pure repartitioning child.
- GitHub issue [#61](https://github.com/legout/fsspeckit/issues/61) —
  ordered compaction child.
- GitHub issue [#62](https://github.com/legout/fsspeckit/issues/62) —
  schema rewrite child.
- GitHub issue [#63](https://github.com/legout/fsspeckit/issues/63) —
  ADR reconciliation child.
- [ADR-0006: Coordinated dataset maintenance guarantees](../adr/0006-coordinated-dataset-maintenance.md) —
  the accepted ADR this PRD reconciles with.
- [Coordinated dataset maintenance spec](coordinated-dataset-maintenance-spec.md) —
  the spec for the existing maintenance coordinator.
- [Partition-preserving object-store compaction PRD](partition-preserving-object-store-compaction-prd.md) —
  the most recent maintenance PRD; reference for execution and recovery
  patterns.
- `src/fsspeckit/core/maintenance.py` — `MaintenancePlan`,
  `GlobalRepartitionDeduplicationPlan`,
  `BestEffortGlobalRepartitionDeduplicationResult`,
  `_normalize_derived_partition_keys`, `_repartition_file_schema`,
  `parse_dedup_order_by`, the atomic-local and best-effort execution
  helpers that the new operations reuse.
