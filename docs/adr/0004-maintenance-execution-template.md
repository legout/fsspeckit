# ADR-0004: Maintenance Execution Template

## Status

Accepted

## Context

`core/maintenance.py` already provides backend-neutral planning functions
(`plan_compaction_groups`, `plan_optimize_groups`, `plan_deduplication_groups`)
that both PyArrow and DuckDB backends delegate to. Planning is testable without
backend writes.

The friction is in **execution orchestration**. Both backends have ~30 lines of
near-byte-for-byte identical compaction execution code:

1. Dry-run early exit (identical).
2. Empty-groups early exit (identical).
3. For each group: read files → concat → write new file (backend-specific).
4. Remove original files (identical loop).
5. Return `planned_stats.to_dict()` (identical).

Only step 3 differs: DuckDB builds SQL `COPY` queries; PyArrow reads tables,
concatenates, and writes with `pq.write_table`. The surrounding boilerplate is
duplicated.

Deduplication execution, by contrast, is fundamentally different between
backends: PyArrow uses table-level group_by with memory monitoring; DuckDB uses
SQL queries with temp tables and window functions. An existing
`execute_deduplication_template` function provides a callback-based template for
dedup, but the execution models diverge too much to share meaningfully.

## Decision

### 1. Extract a shared compaction execution template

Add `execute_compaction_template` to `core/maintenance.py` that owns the full
execution lifecycle:

- Dry-run early exit.
- Empty-groups early exit.
- Output path generation (shared format, no drift).
- Group iteration, calling a backend-provided `compact_group_fn` callback.
- Original file removal (calls `filesystem.rm()` directly — identical in both
  backends).
- Return `planned_stats.to_dict()`.

The backend provides one callback:

```python
compact_group_fn: Callable[[CompactionGroup, str], None]
```

The callback receives the group and a generated output path. It performs the
backend-specific read/concat/write and returns nothing. The template handles
everything else.

### 2. Optimize is covered for free

`optimize_parquet_dataset` delegates to compaction + deduplication. Once
compaction uses the template, optimize is covered without a separate template.

### 3. Deduplication execution stays backend-local

The execution models diverge too much to share a template without it becoming a
shallow pass-through. The existing `execute_deduplication_template` remains for
backends that want it, but the core dedup logic stays in backend adapters.

### 4. Planning functions stay as-is

The planning functions are already backend-neutral and testable without writes.
No `MaintenanceOperationPlan` dataclass is introduced — that would be ceremony
without depth.

## Consequences

### Positive

- **Locality:** compaction execution bugs fix in one place.
- **Leverage:** both backends consume one template; output path format cannot
  drift.
- **Testability:** the template can be tested with a fake
  `compact_group_fn` and a memory filesystem — no backend writes needed.
- **Consistency:** mirrors the existing `execute_deduplication_template` pattern.

### Negative

- **Callback indirection:** backends must wrap their read/concat/write logic in
  a closure or function. This is minor — the closure is ~10 lines.
- **Template owns file removal:** if a future backend needs custom removal
  semantics, the template's direct `filesystem.rm()` calls would need to become a
  callback. No such backend exists today.

## Alternatives Considered

### Introduce a MaintenanceOperationPlan dataclass (mirror merge planning)

Rejected: planning is already backend-neutral and testable. A dataclass would
add ceremony without depth. The real friction is execution duplication, not
planning.

### Generalize the template for compaction + deduplication

Rejected: deduplication execution models are fundamentally different between
backends (SQL window functions vs table-level group_by). Forcing a shared
template would produce a shallow pass-through.

### Separate read/concat/write callbacks

Rejected: over-engineering. The backends don't share enough at each sub-step to
justify three callbacks. One `compact_group_fn` is the right granularity.

## References

- ADR-0002: Merge Planning Seam Before Backend Writes (unpublished)
- [Maintenance Execution Template PRD](../plans/maintenance-execution-template-prd.md)

## Date

2026-07-08

## Authors

- fsspeckit team
