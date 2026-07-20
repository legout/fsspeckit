# ADR-0007: Explicit repartition, ordered compaction, and schema rewrite operations

**Status:** Accepted

## Context

ADR-0006 codifies three deliberate omissions from coordinated maintenance:

1. **Ordordinary maintenance output is unordered.** Compaction and
   coordinated optimization preserve validated dataset semantics but make
   no promise about row order. `CompactionPlan` carries no sort keys.
2. **Global repartitioning is only available as an explicit deduplication
   operation.** `GlobalRepartitionDeduplicationPlan` is coupled to winner
   selection. Callers that only want a new partition layout have no
   coordinated path.
3. **Maintenance schema reconciliation is lossless and inference-free.**
   Reconciliation permits only defined promotions with identical schema
   and field metadata; dtype inference and downcasting are explicitly
   prohibited from the maintenance publication protocol.

These omissions are correct as defaults. ADR-0006 prevents silent
weakening of the publication contract and prevents maintenance from
becoming a hidden schema-migration or business-ordering facility. Three
real caller needs remain unmet, however:

- A caller that needs a new physical partition layout without changing
  row multiplicity (for example a rolling time-partition rewrite) has no
  coordinated path today.
- A caller that benefits from range-prunable physical order within a
  partition (event logs, telemetry) has no coordinated path today.
- A caller that wants to publish a narrower target schema (for storage
  optimization, after a separate dtype-inference step) has no coordinated
  path today.

The [explicit maintenance operations
PRD](../plans/explicit-maintenance-operations-prd.md) (issue #59) locks
down the product decision that these three needs are met as separate,
explicit, operation-specific maintenance operations rather than as flags
on existing operations. This ADR records the architecture decision and
reconciles it with ADR-0006.

## Decision

Add three explicit coordinated physical rewrite operations to the
maintenance coordinator. Each is a separate subclass of
`MaintenancePlan` with its own typed result, its own validation
invariants, and its own memory/spill contract. Each reuses ADR-0006's
shared plan → stage → validate → publish → recover lifecycle and
ADR-0006's two guarantee profiles unchanged.

### 1. Pure full-dataset repartitioning

`RepartitionPlan` / `RepartitionResult` is a pure physical rewrite that
preserves every source row, including exact duplicates, and performs no
winner selection. It reuses the destination-partition machinery already
present in `GlobalRepartitionDeduplicationPlan`
(`_normalize_derived_partition_keys`, `_repartition_file_schema`,
`_hive_partition_path`) but drops the dedup fields entirely. Destination
partition columns are path metadata only and are not stored in physical
file schemas. `partition_filter` is rejected: every source file is in
scope, and unrelated partitions and source files are replaced exactly as
the full-dataset plan specifies.

This operation is distinct from global-repartitioning deduplication.
`plan_global_repartition_deduplication` remains the canonical
deduplicating repartition and is not removed or deprecated.

### 2. Partition-ordered compaction

`OrderedCompactionPlan` / `OrderedCompactionResult` produces one
globally ordered output sequence per affected physical partition,
split into contiguous `max_rows_per_file`-bounded chunks. Adjacent
output files form a single sorted run; the result does not claim
global business ordering across partitions. Sorting is stable and
deterministic for equal keys, with the ADR-0006 physical tie-breaker
tuple `(partition path, file path, row offset)` captured in the source
snapshot applied only after the caller-supplied sort keys tie.

This operation is distinct from ordinary `CompactionPlan`, which
remains explicitly unordered. No sort flag is added to `plan_compaction`
or `plan_coordinated_optimization`. The sort scope is the complete
affected physical partition, not the individual output file: per-file
sorting presented as partition ordering is rejected (see *Rejected
alternatives*).

### 3. Caller-directed schema rewrite

`SchemaRewritePlan` / `SchemaRewriteResult` publishes an explicitly
supplied target schema under a typed `CastPolicy` (`STRICT`, `SAFE`,
`LOOSE`). The target schema is supplied by the caller; dtype inference
is never invoked by the publication protocol. Planning validates target
fields against the full rewrite scope, not only an inference sample.
`LOOSE` narrowing validates every value across the full scope before
publication; any value that would overflow or become null aborts the
plan before any live mutation. Partition columns remain path metadata
and may be widened but never narrowed (narrowing would invalidate the
path encoding).

This operation is distinct from maintenance schema reconciliation.
ADR-0006's lossless, metadata-preserving reconciliation remains the
contract for every other operation; `SchemaRewritePlan` is the only
maintenance operation that intentionally targets a caller-approved
schema different from the reconciled source schema. The `opt_dtype`
helpers remain proposal-only: they may produce a target schema that the
caller then passes to `plan_schema_rewrite`, but the publication
protocol never calls them.

### Where each concern belongs

| Concern | Belongs to | Not in |
|---|---|---|
| Target-schema inference | Caller (may use `opt_dtype` to propose) | Maintenance publication protocol |
| Target-schema execution | `SchemaRewritePlan` | Maintenance schema reconciliation, `opt_dtype` |
| Physical ordering | `OrderedCompactionPlan` | `CompactionPlan`, `plan_coordinated_optimization` |
| Partition placement | `RepartitionPlan`, `GlobalRepartitionDeduplicationPlan` | `CompactionPlan`, `plan_coordinated_optimization` |
| Lossless schema reconciliation | ADR-0006 reconciliation (every operation) | `SchemaRewritePlan` (caller supplies the schema) |
| Publication lifecycle | ADR-0006 (every operation) | Operation-specific paths |
| Guarantee classification | ADR-0006 (every operation) | Operation-specific profiles |

### Reconciliation with ADR-0006

ADR-0006's contract is preserved unchanged:

- **Unordered ordinary output.** `CompactionPlan` and
  `CoordinatedOptimizationPlan` remain unordered. Ordered output is a
  separate explicit operation, not a flag.
- **Lossless, inference-free reconciliation.** Reconciliation remains
  the only schema mechanism for every operation except
  `SchemaRewritePlan`. `SchemaRewritePlan` is explicit, caller-directed,
  and never invokes inference.
- **Global repartitioning is explicit.** Pure repartitioning is a new
  explicit operation; it does not make global repartitioning implicit
  in compaction or optimization. Deduplicating repartition remains
  available.
- **Guarantee profiles.** `atomic_local` and `best_effort_object_store`
  are reused unchanged. No new profile is introduced.
- **Plan-driven execution.** Every new operation is a subclass of
  `MaintenancePlan` dispatched through the existing `execute(plan)`
  entry point. No operation-specific public execution lifecycle.
- **Typed results.** Every new result is a subclass of
  `MaintenanceResult` (and `BestEffortCompactionResult` for the
  object-store lane), exposing phase outcomes, validation, publication,
  recovery, and actual metrics.

## Memory and spill contracts

Each operation declares a bounded-memory strategy and accepts an
optional `memory_budget_mb`. Whole-dataset or whole-partition
materialization is never an undocumented requirement. The strategy and
selected budget are recorded on the plan so that a result without spill
behavior can be audited against the plan.

- **Pure repartitioning:** hash-bucket by destination partition tuple,
  stream each bucket through a row-batch reader, spill per-bucket to
  the maintenance workspace when the bucket exceeds the budget.
- **Ordered compaction:** external merge sort per physical partition.
  Each source file is sorted in memory and written as a sorted run;
  run files are merged through a k-way merge that streams output into
  `max_rows_per_file` chunks. The spill directory must be on the same
  filesystem as the dataset root for `atomic_local` so that
  rename-into-place remains atomic.
- **Schema rewrite:** batch stream over the current file set.
  Source files are read in `RecordBatch`-sized chunks; each chunk is
  cast in place under the configured `CastPolicy` and appended to a
  staged output writer. No whole-column materialization; the
  `opt_dtype` Python-side regex loops are explicitly not in the
  publication path.

These contracts are operation-specific and do not change ADR-0006's
existing operations. `CompactionPlan`, `PartitionLocalDeduplicationPlan`,
`GlobalRepartitionDeduplicationPlan`, and `CoordinatedOptimizationPlan`
retain their existing memory behavior.

## Validation invariants

Each operation honors ADR-0006's `ValidationLevel` enum and adds
operation-specific invariants on top:

- **Pure repartition:** per-source and per-destination row-count
  invariant (every source row appears exactly once in the output);
  destination partition placement; schema compatibility.
  `FULL_DISTINCT_KEY_SCAN` is rejected — pure repartition has no key
  semantics.
- **Ordered compaction:** per-file sort order; sort order across
  adjacent output-file boundaries within each partition; partition
  placement; row-count invariant. `FULL_DISTINCT_KEY_SCAN` is rejected.
- **Schema rewrite:** target schema exactly matches; per-field cast
  validation across the full scope; row-count and null-count
  invariants; partition column type compatibility; compression and
  metadata policy. `FULL_DISTINCT_KEY_SCAN` is reused as full-scope
  cast validation semantics (mandatory for `STRICT`, opt-in for `SAFE`,
  default for `LOOSE`).

## Considered options

### Add the operations as flags on coordinated optimization

Rejected. A `sort=True` or `target_schema=...` flag on
`plan_coordinated_optimization` would make optimization mean "compaction
plus whatever flags are set," silently weakening ADR-0006's contract
that optimization means optional deduplication plus compaction and
nothing else. The result contract would have to grow operation-specific
fields conditionally, and callers could no longer reason about what a
`CoordinatedOptimizationResult` guarantees. Each operation is a
separate plan type so that the plan itself describes its contract.

### Per-file sorting presented as partition ordering

Rejected. Independently sorting each output file does not establish
ordered ranges across a partition: the maximum of file *N* may exceed
the minimum of file *N+1*, defeating range pruning and giving callers a
false sense of physical ordering. Ordered compaction's contract is a
globally ordered sequence per partition split into contiguous chunks,
validated both within each file and across adjacent file boundaries.

### Repartition via deduplication

Rejected as a general pattern. `plan_global_repartition_deduplication`
with a no-op key is the existing workaround callers use today when they
only want a new partition layout; it discards deduplication statistics
and reports misleading metrics. Making it the canonical path would
couple pure repartition to winner selection and require callers to
understand deduplication semantics they do not want. The
`GlobalRepartitionDeduplicationPlan` remains the canonical
deduplicating repartition; the new `RepartitionPlan` is the canonical
pure repartition.

### Inference during publication

Rejected. Calling dtype inference from the maintenance publication
protocol would re-introduce the exact risk ADR-0006 records:
maintenance silently becoming a lossy schema migration. The
`SchemaRewritePlan` accepts an explicit target schema supplied by the
caller; the caller may use `opt_dtype` to *propose* a target schema,
but the proposal is reviewed and approved by the caller before the plan
is created. The publication protocol only ever casts to an
already-approved schema.

### Operation-specific guarantee profiles

Rejected. ADR-0006's two profiles (`atomic_local`,
`best_effort_object_store`) are sufficient. Introducing
operation-specific profiles (for example a "near-atomic" profile for
ordered compaction) would fragment the publication contract and require
operation-specific recovery semantics. Each new operation reuses the
existing profiles and the existing recovery artifact reporting.

### Operation-specific execution lifecycles

Rejected. ADR-0006's plan → stage → validate → publish → recover
lifecycle is the contract that makes backend implementations unable to
reintroduce divergent publication guarantees. Each new operation
contributes its staged-write algorithm and its validation invariants;
the lifecycle itself is shared.

## Consequences

### Positive

- **Closed operation set.** The three operations in this ADR are the
  complete set of explicit data-transforming operations planned for
  this major version. The surface grows by three plan types and three
  result types, each a strict subclass of existing types, and stops.
- **ADR-0006 unchanged.** ADR-0006's guarantee profiles, publication
  lifecycle, reconciliation rules, and existing plan semantics are
  preserved without amendment. This ADR reconciles by addition, not by
  weakening.
- **Caller needs met.** Pure repartition, physical ordering, and safe
  schema publication each have a coordinated path that reuses the
  existing safety contract.
- **pydala2 integration unblocked.** The three downstream call sites
  identified in the PRD (rolling time-partition rewrite, range-prunable
  storage ordering, post-`opt_dtype` schema publish) each have a named
  operation to migrate to.

### Negative

- **Public surface grows.** Three new plan types and three new result
  types, plus `SortKey` and `CastPolicy`. Each is a strict subclass of
  an existing type and dispatches through the existing `execute(plan)`,
  but the surface is larger than before.
- **Memory/spill implementations are non-trivial.** External merge sort
  and hash-bucketed spill require real implementations and real
  large-partition tests. Each child issue must ship a test that
  exercises the documented spill path, not just a happy-path
  small-dataset test.
- **Schema rewrite is easy to misuse.** A `LOOSE` cast policy can
  narrow types and lose precision. Mitigation: `LOOSE` validates the
  full scope before publication and aborts on any value that would
  overflow or become null unexpectedly; `STRICT` and `SAFE` are the
  documented defaults.

### Neutral

- **`opt_dtype` remains in the codebase.** It is not removed or
  deprecated by this ADR; it remains a proposal helper that callers may
  use to produce a target schema for `SchemaRewritePlan`. Its existing
  null-handling and sampling caveats are unchanged.

## Out of scope

- **Z-ordering, multi-dimensional clustering, or business-ordering
  guarantees across partitions.** Physical ordering is per-partition
  only. Cross-partition clustering is a future proposal and would
  require its own ADR.
- **A manifest or version-pointer protocol for object-store
  atomicity.** ADR-0006's deferral stands; the new operations reuse
  `best_effort_object_store` without claiming atomic visibility,
  distributed locking, or automatic rollback.
- **Long-lived parallel `*_v2` maintenance APIs.** The new plan/result
  types are additive at the same major version that already ships
  `MaintenancePlan` and `MaintenanceResult`.
- **Removal of `plan_global_repartition_deduplication`** or any
  existing maintenance API. Existing APIs remain supported.

## References

- [ADR-0006: Coordinated dataset maintenance guarantees](0006-coordinated-dataset-maintenance.md)
- [Explicit maintenance operations PRD](../plans/explicit-maintenance-operations-prd.md)
  (issue [#59](https://github.com/legout/fsspeckit/issues/59))
- Issue [#60](https://github.com/legout/fsspeckit/issues/60) — pure
  full-dataset repartitioning child.
- Issue [#61](https://github.com/legout/fsspeckit/issues/61) —
  partition-ordered compaction child.
- Issue [#62](https://github.com/legout/fsspeckit/issues/62) —
  caller-directed schema rewrite child.
- Issue [#63](https://github.com/legout/fsspeckit/issues/63) — this
  ADR's tracking issue.
- [Coordinated dataset maintenance spec](../plans/coordinated-dataset-maintenance-spec.md)

## Date

2026-07-20

## Authors

- fsspeckit team
