# Common Layer Independence PRD

## Summary

Move schema, polars, and type-conversion utilities out of `fsspeckit.common`
into `fsspeckit.datasets`, where their heavy optional dependencies (pyarrow,
numpy, polars, pandas) are already required. Separate `core/ext` into its own
layering tier above `datasets`. Delete the `datasets/pyarrow/schema.py` re-export
shim.

After this refactor, `import fsspeckit.common` works in a clean environment
without `[datasets]` extras installed.

## Problem

`common/schema.py` imports `numpy` and `pyarrow` at module top level.
`common/polars.py` imports `polars` and `numpy` at module top level.
`common/__init__.py` re-exports schema unconditionally, so
`import fsspeckit.common` fails without those packages.

`core/ext/` has four known layering violations (lazy imports from `datasets`)
maintained in a temporary `ALLOWLIST`. These are architecturally correct —
`core/ext` is the fsspec registration layer — but the checker treats them as
exceptions rather than reflecting the real tier structure.

## Goals

1. Make `fsspeckit.common` importable without any optional dependencies.
2. Move schema, polars, and types modules to `fsspeckit.datasets`.
3. Make `core/ext` its own layering tier in the checker; remove the ALLOWLIST.
4. Delete `datasets/pyarrow/schema.py` re-export shim.
5. Update all callers and tests.
6. No public `BaseDatasetHandler` signature changes.

## Non-Goals

- Do not split `common/misc.py` (separate candidate).
- Do not collapse the logging split (separate candidate).
- Do not add a maintenance planning seam (separate candidate).
- Do not change the `utils/` backwards-compat façade exports.
- Do not add new modules or features.

## Implementation Plan

### Phase 1: Move schema, polars, and types to datasets

- Move `common/schema.py` → `datasets/schema.py`.
- Move `common/polars.py` → `datasets/polars.py`.
- Move `common/types.py` → `datasets/types.py`.
- Update `common/__init__.py`: remove schema, polars, and types re-exports.
  Remove the `try/except` polars guard.
- Update `datasets/__init__.py`: export schema, polars, and types functions
  from their new canonical locations.

### Phase 2: Delete the schema re-export shim

- Delete `datasets/pyarrow/schema.py`.
- Update `datasets/pyarrow/__init__.py`: drop schema re-exports.
- Update `datasets/__init__.py`: point `cast_schema`, `opt_dtype_pa`,
  `unify_schemas_pa` at `datasets.schema`.

### Phase 3: Update the layering checker

- Add `"core.ext"` as a separate package tier in `DISALLOWED_PREFIXES`
  (empty disallow list).
- Update `package_for()` to check `relative.parts[1]` when
  `parts[0] == "core"`.
- Delete the `ALLOWLIST` and its four entries.

### Phase 4: Update all callers

- `core/ext/parquet.py`: `from fsspeckit.datasets.schema import ...`
- `core/ext/csv.py`: `from fsspeckit.datasets.polars import ...`
- `core/ext/json.py`: `from fsspeckit.datasets.polars import ...` and
  `from fsspeckit.datasets.schema import ...`
- `datasets/pyarrow/io.py`: `from fsspeckit.datasets.schema import cast_schema`
- `datasets/duckdb/dataset.py`: `from fsspeckit.datasets.schema import cast_schema`
- `utils/__init__.py`: `from fsspeckit.datasets.schema import ...`
- All test files that import from `common.schema` or `common.polars`.

### Phase 5: Verify

- `python scripts/check_layering.py` passes.
- `uv run ruff check` passes.
- `pip install fsspeckit && python -c "import fsspeckit.common"` works in a
  clean environment.
- Existing tests pass (excluding known pre-existing failures).

## Acceptance Criteria

- `fsspeckit.common` imports successfully without `pyarrow`, `numpy`, or
  `polars` installed.
- `datasets/schema.py`, `datasets/polars.py`, `datasets/types.py` are the
  canonical locations for schema, polars, and type-conversion utilities.
- `common/__init__.py` does not re-export schema, polars, or types.
- `datasets/pyarrow/schema.py` does not exist.
- The layering checker has no `ALLOWLIST` and treats `core/ext` as its own
  tier.
- `python scripts/check_layering.py` passes.
- `uv run ruff check` passes.
- Public `BaseDatasetHandler.merge(...)` and `BaseDatasetHandler.read_parquet(...)`
  signatures remain compatible.

## Risks

### Risk: Breaking external callers of `from fsspeckit.common import cast_schema`

Mitigation: the `utils/` backwards-compat façade still re-exports these.
Document the migration in a changelog entry. Bump minor version.

### Risk: Circular imports after moving types.py

Mitigation: `datasets/types.py` imports from `datasets/schema.py` — both in the
same package, no cycle. `common/optional.py` stays in `common` and is imported
lazily inside function bodies, same as today.

### Risk: Layering checker regression

Mitigation: the checker change is small and testable. The existing
`tests/test_layering_compliance.py` test suite validates the rules.

## References

- [ADR-0003: Common Layer Independence and core/ext Tier Separation](../adr/0003-common-layer-independence.md)
- [ADR-0001: Import Layering Rules for Package Architecture](../architecture/0001-layering-rules.md)
