# Change: Stabilize and simplify core ext I/O helpers

## Why

The universal I/O helpers in `fsspeckit.core.ext_io` are central to reading and writing JSON/CSV/Parquet via fsspec
filesystems, but they currently have several issues:

- `write_file` and `write_files` reference `pl`, `pa`, and `pd` at runtime without importing them, causing `NameError`
  when called.
- The `write_files` implementation has a broken non-threaded path (`use_threads=False`) that iterates over a string
  `path` character-by-character and calls `_write` with incorrect argument order.
- The format dispatch logic in `read_files`/`write_file` is verbose and duplicated across formats.
- Optional dependency handling (Polars, Pandas, PyArrow) is not fully aligned with the centralised lazy-import
  patterns specified in `core-lazy-imports` and `utils-optional-separation`.

These issues make the APIs fragile, surprising, and harder to maintain.

## What Changes

- Refactor `fsspeckit.core.ext_io` to:
  - Use `TYPE_CHECKING` imports and `fsspeckit.common.optional` helpers for Polars, Pandas, and PyArrow.
  - Normalise `path` handling in `write_files` so that:
    - Both string and list paths are supported.
    - Data elements and paths are paired by index.
    - The non-threaded execution path is correct and easy to reason about.
  - Simplify format dispatch in `read_files` and `write_file` using a small mapping of format → handler functions.
- Align `fsspeckit.core.ext_csv` and `fsspeckit.core.ext_parquet` with the same optional-dependency patterns.
- Ensure that importing these modules never requires optional dependencies; only calling format-specific paths does.

## Impact

- Fixes runtime `NameError` issues in `ext_io`.
- Prevents incorrect behaviour in `write_files` when `use_threads=False`.
- Makes behaviour around optional dependencies predictable and aligned with existing lazy-import specifications.
- No intentional breaking changes to public signatures, but behaviour becomes more robust and better specified.

