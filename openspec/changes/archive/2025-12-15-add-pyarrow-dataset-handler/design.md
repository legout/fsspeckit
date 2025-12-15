## Context
PyArrow support is currently function-based. DuckDB offers class-based ergonomics (`DuckDBDatasetIO` and a handler
wrapper). Adding a PyArrow handler increases parity and reduces cognitive overhead for users switching backends.

## Goals / Non-Goals
- Goals:
  - Provide class-based PyArrow dataset interface matching DuckDB naming where practical.
  - Keep imports lazy with respect to PyArrow.
  - Avoid breaking existing function-based entry points.
- Non-Goals:
  - Re-implement functionality already in PyArrow helpers; the class will delegate.
  - Introduce new behaviours beyond what existing functions support.

## Decisions
- `PyarrowDatasetIO` will internally call existing functions (`merge_parquet_dataset_pyarrow`, `write_pyarrow_dataset`,
  etc.), keeping logic centralized.
- `PyarrowDatasetHandler` will be a thin wrapper (similar to `DuckDBParquetHandler`) and re-exported from
  `fsspeckit.datasets`.
- Method surface will mirror DuckDB where feasible; unsupported operations will be omitted or clearly documented.

## Risks / Trade-offs
- Slight duplication of wrappers, but improves UX symmetry.
- Need to ensure optional dependency handling is consistent with `core-lazy-imports`.

## Migration Plan
1) Implement `PyarrowDatasetIO` delegating to existing functions.
2) Add `PyarrowDatasetHandler` wrapper and exports.
3) Add tests and docs showing parallel usage between PyArrow and DuckDB handlers.

## Open Questions
- Should we add a common protocol/interface type for handlers now or later? (Can be deferred.)
