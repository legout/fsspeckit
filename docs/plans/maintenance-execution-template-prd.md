# Maintenance Execution Template PRD

## Summary

Extract the duplicated compaction execution orchestration from both backend
adapters into a shared `execute_compaction_template` in `core.maintenance`. The
template owns the full execution lifecycle (dry-run check, group iteration,
output path generation, file removal, stats return); backends provide a single
`compact_group_fn` callback for the backend-specific read/concat/write step.

## Problem

`compact_parquet_dataset_duckdb` and `compact_parquet_dataset_pyarrow` share
~30 lines of near-byte-for-byte identical execution code surrounding the
backend-specific read/concat/write call. The only divergence is step 3: DuckDB
uses SQL `COPY`, PyArrow uses `pq.write_table`. Everything else — dry-run exit,
empty-groups exit, file removal loop, stats return — is duplicated.

## Goals

1. Extract the shared compaction execution lifecycle into
   `execute_compaction_template` in `core/maintenance.py`.
2. Both backends consume the template, providing only a `compact_group_fn`
   callback.
3. Optimize path benefits automatically (it delegates to compaction).
4. Deduplication execution stays backend-local.
5. Template is testable with a fake callback and memory filesystem.

## Non-Goals

- Do not introduce a `MaintenanceOperationPlan` dataclass.
- Do not change planning function signatures.
- Do not extract a deduplication execution template (already exists and execution
  models diverge too much).
- Do not change public `BaseDatasetHandler` maintenance method signatures.

## Proposed Interface

```python
def execute_compaction_template(
    groups: list[CompactionGroup],
    planned_stats: MaintenanceStats,
    dataset_path: str,
    compact_group_fn: Callable[[CompactionGroup, str], None],
    filesystem: Any,
    dry_run: bool,
) -> dict[str, Any]:
    """Execute a compaction plan across groups using a backend-provided callback."""
```

The callback signature:

```python
compact_group_fn(group: CompactionGroup, output_path: str) -> None
```

The callback reads the group's files, concatenates them, and writes the result
to `output_path`. The template handles everything else.

## Implementation Plan

### Phase 1: Add the template to core/maintenance.py

- Implement `execute_compaction_template` with the full lifecycle:
  dry-run check, empty-groups check, output path generation, group iteration,
  file removal, stats return.
- Add unit tests with a fake `compact_group_fn` and a memory filesystem.

### Phase 2: DuckDB adoption

- Update `compact_parquet_dataset_duckdb` to call `execute_compaction_template`.
- The `compact_group_fn` closure builds and executes the SQL `COPY` query.
- Remove the duplicated boilerplate (dry-run exit, file removal loop, stats
  return).

### Phase 3: PyArrow adoption

- Update `compact_parquet_dataset_pyarrow` to call `execute_compaction_template`.
- The `compact_group_fn` closure reads tables, concatenates, and writes with
  `pq.write_table`.
- Remove the duplicated boilerplate.

### Phase 4: Verify

- `python scripts/check_layering.py` passes.
- `uv run ruff check` passes.
- Existing compaction and optimize tests pass for both backends.
- New template unit tests pass.

## Acceptance Criteria

- `execute_compaction_template` exists in `core/maintenance.py`.
- Both `compact_parquet_dataset_duckdb` and `compact_parquet_dataset_pyarrow`
  consume the template.
- No duplicated dry-run/empty-groups/file-removal/stats-return boilerplate in
  backend compaction functions.
- Template unit tests pass with a fake callback.
- `python scripts/check_layering.py` passes.
- `uv run ruff check` passes for changed files.
- Public maintenance method signatures remain compatible.

## Risks

### Risk: Behavior change during extraction

Mitigation: existing backend compaction tests verify the same behavior before
and after the extraction. Run both DuckDB and PyArrow compaction test suites.

### Risk: Output path format drift

Mitigation: the template generates the output path. Both backends receive the
same format. No drift possible.

## References

- [ADR-0004: Maintenance Execution Template](../adr/0004-maintenance-execution-template.md)
- ADR-0002: Merge Planning Seam Before Backend Writes (unpublished)
