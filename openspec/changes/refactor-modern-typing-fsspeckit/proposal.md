# Change: Refactor fsspeckit to modern typing conventions

## Why
The codebase still uses legacy typing constructs such as `typing.Union`, `Optional`, `List`, and `Dict` in several modules.
This creates unnecessary verbosity, hides intent, and has already led to at least one import bug (a `Union[...]` annotation
in `core.maintenance` without the corresponding import).

Modern Python supports PEP 604 union syntax (`X | Y`) and built-in generic types (`list[str]`, `dict[str, Any]`) which:

- Simplify annotations and improve readability.
- Reduce the need for additional imports from `typing`.
- Align with current best practices and type-checker expectations.
- Make it easier to keep optional dependency type usage consistent with `TYPE_CHECKING` imports.

## What Changes
- Replace `Union[...]` and `Optional[...]` with PEP 604 style unions (`X | Y`, `X | None`) across:
  - `fsspeckit.core.*`
  - `fsspeckit.common.*`
  - `fsspeckit.storage_options.*`
  - `fsspeckit.datasets.*`, `fsspeckit.sql.*`, and public façades in `fsspeckit.utils`.
- Replace `List[...]` / `Dict[...]` with `list[...]` / `dict[...]` consistently.
- Remove any now-unused imports from `typing` that are only needed for `Union`, `Optional`, `List`, or `Dict`.
- Ensure that optional-dependency types (polars, pandas, pyarrow, duckdb, sqlglot, orjson) remain:
  - Imported only under `TYPE_CHECKING` or
  - Obtained via helpers in `fsspeckit.common.optional`.

## Impact
- Affects type annotations across most modules, but:
  - Does not change runtime behaviour or public APIs.
  - Keeps type names and shapes semantically identical (only representation changes).
- Fixes at least one concrete bug where `Union` was referenced without being imported.
- Makes it easier to reason about optional dependency handling in combination with `core-lazy-imports` and `utils-optional-separation`.

