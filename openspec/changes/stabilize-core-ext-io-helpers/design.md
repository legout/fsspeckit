## Context

`fsspeckit.core.ext_io` exposes “universal” I/O helpers that sit on top of the format-specific modules (`ext_json`,
`ext_csv`, `ext_parquet`). The current implementation:

- Delegates to the format-specific functions, but with verbose and duplicated `if/elif` chains.
- Mixes runtime references to Polars/Pandas/PyArrow with `TYPE_CHECKING` imports inconsistently.
- Has a complex `write_files` implementation that is hard to reason about and has at least one broken branch.

At the same time, the project has centralised optional-dependency handling in `fsspeckit.common.optional`, and we want
these helpers to behave consistently with that model.

## Goals / Non-Goals

- Goals:
  - Make `read_files` / `write_file` / `write_files` safe to import and call without NameErrors.
  - Align optional-dependency usage with `core-lazy-imports` and `utils-optional-separation`.
  - Simplify control flow in `write_files` so indexing and path handling are straightforward.
  - Keep public function signatures stable.
- Non-Goals:
  - Adding new formats beyond JSON/CSV/Parquet.
  - Changing high-level semantics of which concrete types are returned per format (e.g. `pl.DataFrame` vs `pa.Table`).

## Decisions

- Use a small mapping structure for dispatch:
  - For example, `READ_HANDLERS = {"json": _read_json_json, "csv": _read_csv_csv, "parquet": _read_parquet_parquet}`.
  - This keeps the universal functions thin and makes it easy to extend with new formats later.
- For `write_files`:
  - Normalise `data` to a list and `path` to a list before pairing.
  - Use an inner helper with an explicit `(data_item, path_item, index)` signature for both threaded and non-threaded paths.
  - Respect existing modes but keep structure linear and easy to test.
- Optional dependencies:
  - All runtime imports of Polars/Pandas/PyArrow go through `common.optional` helpers.
  - Type hints use `TYPE_CHECKING` imports to satisfy static checkers without affecting runtime.

## Risks / Trade-offs

- Any change to internal behaviour of `write_files` can reveal previously hidden bugs in tests or calling code; however,
  this is preferable to preserving the current broken semantics.
- Simplifying dispatch may require adjusting tests if they rely on overly specific error messages; this will be handled
  via aligned spec updates if necessary.

## Migration Plan

1. Refactor `ext_io` to use the new dispatch and path normalisation; keep changes local.
2. Align CSV/Parquet helpers to use the same lazy-import pattern.
3. Extend tests to cover both threaded and non-threaded execution paths and all write modes.

