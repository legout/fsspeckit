# Design: Backend-Neutral Merge Layer

## Context

`fsspeckit` exposes merge functionality in two places today:

- `DuckDBParquetHandler.merge_parquet_dataset` in `src/fsspeckit/utils/duckdb.py`.
- `merge_parquet_dataset_pyarrow` in `src/fsspeckit/utils/pyarrow.py`.

Both aim to implement the same high-level semantics (UPSERT/INSERT/UPDATE/FULL_MERGE/DEDUPLICATE) as defined in the `utils-duckdb` and `utils-pyarrow` specs, but:

- They duplicate key/schema validation logic and do not always agree on edge cases.
- Statistics are computed differently and sometimes heuristically.
- Only the PyArrow implementation is consciously streaming/batch-oriented; DuckDB currently materializes the target dataset into a single Arrow table.
- The “filtered scanner” requirement for PyArrow is only partially satisfied (a preflight pass rather than driving the main merge).

We want merge semantics to be a core capability of the library, independent of which execution engine is used.

## Goals

- Define a single, canonical interpretation of merge strategies and statistics across backends.
- Centralize merge input validation (keys, schemas, NULL keys) so that behavior cannot silently diverge.
- Ensure both DuckDB and PyArrow implementations are streaming-friendly and avoid “read the entire dataset into memory”.
- Preserve the existing public APIs (`DuckDBParquetHandler.merge_parquet_dataset`, `merge_parquet_dataset_pyarrow`) while improving correctness and scalability.
- Keep the backend-neutral layer small and testable, without over-engineering a full query planner.

## Non-Goals

- Replacing DuckDB or PyArrow’s own optimizers.
- Designing a fully generic, cross-engine relational algebra layer.
- Changing the user-facing method signatures or the set of supported strategies.
- Implementing cross-backend transactions or distributed merge.

## High-Level Approach

Introduce a small backend-neutral merge core under `src/fsspeckit/core/merge.py` that:

1. **Normalizes inputs**
   - Convert `key_columns` to a normalized list of strings.
   - Encapsulate `source` and `target` as abstract “table descriptors” that expose schemas and (optionally) counts without forcing full materialization.

2. **Validates inputs**
   - Ensure all key columns exist in source (and target, if non-empty).
   - Enforce “no NULL keys” with hooks that backends can implement efficiently (e.g. filtered scanners).
   - Perform schema-compatibility checks using the existing `unify_schemas` logic in `utils.pyarrow` where appropriate.

3. **Defines canonical strategy semantics**
   - For each strategy (`upsert`, `insert`, `update`, `full_merge`, `deduplicate`), define what it means in terms of:
     - Which rows should be kept as-is.
     - Which rows should be replaced by source rows.
     - Which rows should be deleted.
   - Keep this logic expressed in terms of keys and simple sets/indices so that both backends can implement it by mapping keys to row groups.

4. **Computes statistics**
   - Provide a small helper that accumulates `inserted`, `updated`, `deleted`, `total` based on the actual rows written to the merged dataset.
   - Ensure DuckDB and PyArrow backends call into this helper instead of hand-rolling stats.

5. **Backend-specific execution**
   - DuckDB backend:
     - Use SQL and DuckDB’s parquet support to read relevant parts of the target dataset (ideally via `parquet_scan` and key filters).
     - Construct temporary tables for source and target data as needed, then execute SQL that reflects the canonical strategy semantics.
     - Write merged output back to the dataset (preferably via a temp directory + swap to improve atomicity).
   - PyArrow backend:
     - Use `pyarrow.dataset.Dataset` scanners with key-based filters where possible to discover and transform only relevant rows.
     - Build merged output as a sequence of Arrow tables per output file, then write them via existing `pq.write_table` helpers.

The backend-neutral core focuses on *semantics* and *validation*, while each backend focuses on *how* to implement those semantics efficiently.

## API Sketch

```python
# core/merge.py (sketch – names tentative)

from typing import Protocol, Literal, Iterable, Any
import pyarrow as pa

MergeStrategy = Literal["upsert", "insert", "update", "full_merge", "deduplicate"]


class TableLike(Protocol):
    """Minimal protocol for a table-like object used by merge core."""

    @property
    def schema(self) -> pa.Schema: ...

    @property
    def num_rows(self) -> int: ...


class MergeStats(dict):
    inserted: int
    updated: int
    deleted: int
    total: int


def normalize_keys(key_columns: list[str] | str) -> list[str]: ...


def validate_keys_and_schema(
    source: TableLike,
    target: TableLike | None,
    key_columns: list[str],
) -> None:
    """Shared checks for key existence, schema compatibility, and NULL keys.

    Backends may pass lightweight table descriptors that expose only the needed
    metadata; the function should avoid forcing full materialization.
    """
    ...


def compute_merge_stats(
    *,
    strategy: MergeStrategy,
    target_before: int,
    target_after: int,
    inserted_rows: int,
    updated_rows: int,
    deleted_rows: int,
) -> MergeStats:
    """Produce spec-compliant stats from actual row counts."""
    ...
```

Backends can:

- Use `normalize_keys` and `validate_keys_and_schema` before executing their merge.
- Track inserted/updated/deleted counts as they build output tables or SQL results, then call `compute_merge_stats` once at the end.

## DuckDB Execution Sketch

1. Convert `source` (table or path) into a DuckDB relation (`source_data`).
2. Represent the target dataset as `target_dataset` via `parquet_scan('<path>')`, optionally restricted by key filters if this proves practical.
3. Use SQL templates (similar to today’s `_execute_merge_strategy`) but:
   - Ensure the logic matches the canonical semantics (e.g. full_merge with empty source deletes all rows and reports correct `deleted`).
   - Avoid unnecessary materialization into Python if results can be written directly via `COPY`.
4. Use `write_parquet_dataset(..., mode="overwrite")` or a temp-directory-and-swap pattern to write the merged dataset.
5. Derive `inserted/updated/deleted` either via:
   - `COUNT` queries on source and target, or
   - small side computations in SQL (e.g. marking rows via flags).

We keep the SQL-centric flavor but reduce the amount of bespoke Python logic and heuristics.

## PyArrow Execution Sketch

1. Load the source as a `pa.Table` using the existing `_load_source_table_pyarrow`.
2. Represent the target as a `ds.Dataset` if it exists; otherwise, treat it as empty.
3. Use the backend-neutral helpers to validate keys and schemas before scanning.
4. Iterate the target dataset in batches via `scanner.to_batches()`, but:
   - Optionally pre-filter batches by key using `_build_filter_expression` or a more direct `ds.field().isin(...)` expression.
   - For each batch, decide whether each row is kept, updated, or deleted based on the canonical semantics, building per-batch output tables.
5. After processing all target batches, append any remaining source rows that should be inserted.
6. Write the resulting list of tables using `_write_tables_to_dataset`.
7. Track inserted/updated/deleted counts and call `compute_merge_stats`.

This keeps the current streaming orientation while making the key-filter usage central to the implementation rather than an ancillary test hook.

## Edge Cases & Alignment

The shared layer will define clear behavior for:

- **Empty target, UPSERT/INSERT**: treat as initial load; all source rows are inserted, `deleted = 0`.
- **Empty target, UPDATE**: backend should either:
  - treat as a no-op with zero updated rows (preferred), or
  - raise a consistent, clearly specified error.  
  The spec deltas will choose one behavior and both backends will adopt it.
- **FULL_MERGE with empty source**: target becomes empty; `deleted` equals the original target row count; `inserted = updated = 0`.
- **Schema incompatibility**: define when we *must* raise versus when we can rely on `unify_schemas` and type promotion.

These behaviors will be captured in the spec deltas and verified via tests for both backends.

## Open Questions

- Should we expose the backend-neutral merge core as a public API (e.g. `fsspeckit.core.merge`) or keep it as an internal implementation detail?
- For DuckDB, how far do we go in avoiding Python materialization versus using DuckDB’s `arrow()` interface as a bridge?
- For PyArrow, should we introduce an explicit “plan” structure (e.g. a list of key ranges or row actions) or keep the execution logic directly in the backend?

These can be resolved during implementation; the proposal focuses on the shared semantics and validation layer as the primary unification point.

