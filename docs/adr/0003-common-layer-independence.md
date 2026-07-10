# ADR-0003: Common Layer Independence and core/ext Tier Separation

## Status

Accepted

## Context

ADR-0001 establishes strict import layering rules: `common` is the lowest level,
importable by anything, with no dependencies on other fsspeckit packages. The
rationale explicitly states "reusability: lower-level packages can be reused
without pulling in higher-level dependencies."

In practice, this contract is broken:

- `common/schema.py` (1251 lines) imports `numpy` and `pyarrow` at module top
  level.
- `common/polars.py` (1056 lines) imports `polars` and `numpy` at module top
  level.
- `common/__init__.py` re-exports schema functions unconditionally at line 15,
  so `import fsspeckit.common` transitively triggers `import pyarrow` and
  `import numpy`.
- `common/__init__.py` wraps the polars re-export in a `try/except` that
  silently sets `opt_dtype_pl = None` when polars is missing.

These packages are declared as optional extras in `pyproject.toml`
(`[datasets]` extra includes `pyarrow`, `numpy`, `polars`, `pandas`, `duckdb`).
But `import fsspeckit.common` — the foundational layer — fails without them.

Additionally, `core/ext/` contains four known layering violations where
`core/ext` modules import from `datasets`:

- `core/ext/dataset.py` lazy-imports from `datasets.duckdb` and
  `datasets.pyarrow`.
- `core/ext/json.py` lazy-imports from `datasets.pyarrow`.
- `core/ext/parquet.py` top-level imports from `common.schema` (which will
  become `datasets.schema` after this refactor).

These are currently maintained in a temporary `ALLOWLIST` in
`scripts/check_layering.py` with a TODO comment: "remove these legacy extension
allowlist entries once the extension layer is moved behind backend-neutral
adapters."

`core/ext` is the fsspec monkey-patch layer — it wires concrete dataset
backends onto `AbstractFileSystem` methods. It is architecturally at the top of
the stack, not at the core level, despite its directory path under `core/`.

## Decision

### 1. Make `common` truly dependency-free

Move all modules that require heavy optional dependencies out of `common` and
into `datasets`, where those dependencies are already required:

| Module | From | To |
|---|---|---|
| Schema utilities | `common/schema.py` | `datasets/schema.py` |
| Polars utilities | `common/polars.py` | `datasets/polars.py` |
| Type conversion | `common/types.py` | `datasets/types.py` |

After the move, `common` contains only modules that depend on the standard
library and `fsspec`:

- `common/datetime.py`
- `common/logging/`
- `common/logging_config.py`
- `common/misc.py`
- `common/optional.py`
- `common/partitions.py`
- `common/path_validation.py`
- `common/security.py`
- `common/sql_filters.py`

`common/__init__.py` stops re-exporting schema, polars, and types. The `try/except`
polars guard is removed. Callers update their imports to point at `datasets`.

### 2. Separate `core/ext` into its own layering tier

`core/ext` is the fsspec method registration layer. It wires concrete dataset
backends onto `AbstractFileSystem`. It is architecturally above `datasets`,
not below it.

The layering checker (`scripts/check_layering.py`) treats `core/ext/` files as
belonging to a separate `"core.ext"` package tier with no disallowed imports.
This replaces the temporary `ALLOWLIST` mechanism.

### 3. Delete the `datasets/pyarrow/schema.py` re-export shim

`datasets/pyarrow/schema.py` exists only to re-export from `common.schema`.
After schema moves to `datasets/schema.py`, the shim adds indirection without
depth. Delete it; update callers to import from `datasets.schema` directly.

## Consequences

### Positive

- **Locality:** `common` is honestly standalone. Schema, polars, and type bugs
  concentrate in `datasets` where the deps are present.
- **Leverage:** `import fsspeckit.common` works in a clean environment without
  `[datasets]` extras. The test is one line: `python -c "import
  fsspeckit.common"`.
- **Honest checker:** The layering checker reflects the real architecture
  instead of maintaining a growing exception list.
- **No silent None:** The `try/except` guard that silently set
  `opt_dtype_pl = None` is gone. Missing dependencies fail at use time with a
  clear error through `common.optional`.
- **Deletion test:** `datasets/pyarrow/schema.py` passes the deletion test — no
  complexity reappears, callers use a shorter import path.

### Negative

- **Breaking change for `from fsspeckit.common import cast_schema`:** callers
  must update to `from fsspeckit.datasets.schema import cast_schema`. The
  `utils/` backwards-compat façade still re-exports these for existing users.
- **Import path churn:** ~10 source files and ~5 test files update their import
  paths. Mechanical, but wide.
- **ADR-0001 refinement:** this ADR narrows ADR-0001's "common has no
  dependencies on other fsspeckit packages" to also mean "common has no hard
  dependencies on heavy optional packages." ADR-0001 is not superseded; its
  fsspeckit-internal layering rules stand unchanged.

## Alternatives Considered

### Keep schema/polars in common, make all imports lazy

This would route heavy imports through `common.optional` without moving the
files. Less disruptive, but `common` still conceptually owns PyArrow/Polars
logic. Rejected: the deletion test passes for the move — no complexity
reappears across callers, and schema is inherently PyArrow-specific.

### Keep the ALLOWLIST for core/ext

Rejected: the ALLOWLIST is documented as temporary. Accepting `core/ext` as a
real tier makes the checker honest and eliminates the growing exception list.

### Duplicate thin schema wrappers in core/ext

Rejected: reintroduces the duplication the original refactor eliminated.

## References

- [ADR-0001: Import Layering Rules for Package Architecture](../architecture/0001-layering-rules.md)
- ADR-0002: Merge Planning Seam Before Backend Writes (unpublished)
- [Common Layer Independence PRD](../plans/common-layer-independence-prd.md)

## Date

2026-07-08

## Authors

- fsspeckit team
