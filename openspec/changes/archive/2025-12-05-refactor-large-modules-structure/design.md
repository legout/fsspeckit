## Context

The current domain-driven package layout (`core`, `storage_options`, `datasets`, `sql`, `common`, `utils`) establishes clear high-level boundaries, but some modules have accumulated many responsibilities internally. This increases cognitive load and makes it harder to maintain import layering and optional dependency separation.

Refactoring these large modules into smaller, focused submodules is a natural next step to realise the full benefits of the architecture.

## Design Goals

- Preserve existing public APIs and import paths.
- Keep each submodule focused on a single responsibility (e.g. format-specific IO, schema utilities, connection handling).
- Strengthen import layering by ensuring:
  - `core` does not depend on `datasets` or `sql`.
  - `datasets` reuses `core` and `common`, not the other way around.
- Avoid creating new cycles or reintroducing unconditional imports of optional dependencies.

## Proposed Submodule Structure (Illustrative)

> Final names and boundaries to be refined during implementation; this sketch is for orientation.

### `fsspeckit.core`

- `filesystem.py` – high-level factory functions and core filesystem types.
- `filesystem_paths.py` – path-normalisation, protocol detection, and related helpers.
- `filesystem_cache.py` – cache-mapper and monitored cache filesystem helpers.
- `ext_json.py` – JSON/JSONL filesystem extension helpers.
- `ext_csv.py` – CSV filesystem extension helpers.
- `ext_parquet.py` – Parquet filesystem extension helpers.
- `ext_register.py` – wiring layer that attaches helpers to `AbstractFileSystem`.

### `fsspeckit.datasets`

- `pyarrow_schema.py` – schema/type inference, unification, timezone handling.
- `pyarrow_dataset.py` – merge/maintenance helpers built on `core.merge`/`core.maintenance`.
- `duckdb_connection.py` – DuckDB connection and filesystem registration helpers.
- `duckdb_dataset.py` – dataset IO and maintenance helpers.

## Public Surface

- Existing imports such as:
  - `from fsspeckit.core import filesystem, AbstractFileSystem`
  - `from fsspeckit.datasets import DuckDBParquetHandler`
  - `from fsspeckit.datasets.pyarrow import merge_parquet_dataset_pyarrow`
  will remain valid and will be supported via re-exports or thin entrypoint modules.

## Risks / Mitigations

- **Risk:** Large refactors can introduce subtle behavioural regressions.
  - **Mitigation:** Take small, incremental steps; run the full test suite after each module split; keep changes as mechanical as possible.

- **Risk:** New submodules could accidentally import optional dependencies at top level.
  - **Mitigation:** Apply the lazy import and optional-dependency patterns defined in the `core-lazy-imports` and `utils-optional-separation` specs.

