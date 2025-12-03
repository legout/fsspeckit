## Context

The `fsspeckit.core.ext` module wires additional read/write helpers onto `AbstractFileSystem`. These helpers:

- Provide multi-format I/O (JSON, CSV, Parquet) with optional parallelism.
- Depend on optional libraries (`pyarrow`, `orjson`, Polars) to implement their logic.

Recent reviews identified several mismatches:

- Control flow in `_read_json` and `_read_csv` discards the parallel result when `use_threads=True`.
- Parquet helpers implicitly rely on `pyarrow` being imported, bypassing the central optional-dependency layer.
- `include_file_path` on Parquet readers uses Polars types against a PyArrow `Table`.
- `write_json` uses `orjson` directly instead of the existing lazy import infrastructure.

## Goals

- Honour the `use_threads` parameter in JSON/CSV helpers without redundant work.
- Ensure Parquet helpers are compatible with the project’s lazy import design.
- Make the type story for Parquet helpers consistent (PyArrow in, PyArrow out).
- Keep JSON writing aligned with optional dependencies and error messaging.

## Non-Goals

- No changes to public function signatures or return types beyond bug fixes.  
- No reorganisation of `core.ext` into new modules (covered by a separate architectural change).

## Decisions

1. **Threading control flow**
   - Implement a simple pattern:
     - If `use_threads` is truthy, call `run_parallel` and do not follow up with a sequential re-read.
     - If `use_threads` is falsy, use a single sequential loop/comprehension.

2. **PyArrow dependency management**
   - All uses of `pyarrow`/`pyarrow.parquet` in `core.ext` will go through `_import_pyarrow` or a similar helper, guaranteeing consistent optional-dependency error messages.

3. **`include_file_path` semantics**
   - When reading Parquet, `include_file_path=True` will always:
     - Return a PyArrow `Table`.
     - Use a simple string `file_path` column implemented as a PyArrow array of the same length.

4. **JSON writer optionality**
   - `write_json` will:
     - Use `_import_orjson` to obtain `orjson`.
     - Raise a clear `ImportError` with an extras hint (e.g. `pip install fsspeckit[sql]` or `...[datasets]`) when `orjson` is not available.

## Risks / Trade-offs

- Slight behaviour change for users who inadvertently relied on the old broken threading behaviour (parallel flag ignored). This is considered a bug fix and acceptable.
- Introducing lazy imports inside helpers slightly increases call overhead but is negligible compared to I/O cost and is consistent with the library’s design.

