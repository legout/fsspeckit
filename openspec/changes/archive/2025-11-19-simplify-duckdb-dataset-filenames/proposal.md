# Change Proposal: Simplify DuckDB Dataset Filename Requirements

## Why

The archived `add-duckdb-dataset-write` change and the current `utils-duckdb` spec describe relatively rich filename behavior for `write_parquet_dataset`, including:

- UUID-based filenames (`part-{uuid}.parquet`).
- Timestamp-based filename options.
- Sequential filename options (`data_000.parquet`, `data_001.parquet`, …).

The current implementation of `DuckDBParquetHandler.write_parquet_dataset` provides:

- Universally unique filenames based on a short UUID, created by `_generate_unique_filename(template)`.
- A `basename_template` parameter that allows callers to control the prefix and pattern, but still uses UUIDs (e.g. `data_a1b2c3d4.parquet`).

It does **not** implement:

- Explicit timestamp-based naming.
- True sequential numbering across files or across multiple calls.

In practice, the UUID-based scheme is:

- Simple and robust (no collision management needed).
- Sufficient for most real-world dataset scenarios.
- Already used in tests (which only assert basic prefix behavior, not specific numbering).

The extra naming strategies described in the spec increase complexity without strong evidence that they are needed, and they diverge from the current implementation. Rather than adding more code to match the spec, it is more consistent with “simplicity first” to adjust the spec to match the implementation and treat more advanced naming as a possible future enhancement.

## What Changes

- Simplify the `write_parquet_dataset` filename requirements in `utils-duckdb` spec to:
  - Emphasize that filenames are generated to be unique using a UUID-based mechanism.
  - Clarify how `basename_template` is used (i.e. a template with `{}` substituted with a unique token) without promising timestamp- or sequence-based semantics.
  - Remove or soften requirements that demand specific timestamp or sequential numbering strategies, treating them as *examples* that may be implemented by callers via `basename_template` if they need deterministic names.
- Update docs and examples to:
  - Describe the actual behavior of `_generate_unique_filename` (UUID insertion).
  - Avoid promising names like `data_000.parquet` unless we implement that feature explicitly.
- Keep the current implementation of `write_parquet_dataset` and `_generate_unique_filename` intact for now, as it already satisfies the simplified spec.

## Impact

- **Affected specs**
  - `utils-duckdb` (requirements around dataset filename generation).
- **Affected code**
  - No functional changes required; only potential docstring/example tweaks in `src/fsspeckit/utils/duckdb.py`.
  - Tests: review `tests/test_utils/test_duckdb.py` to ensure none assert hard-coded sequential patterns (currently they only check for uniqueness and prefixes).
- **Behavioral impact**
  - No runtime behavior change; only clarification and simplification of expectations.
- **Risk / rollout**
  - Low: we are aligning the spec to actual behavior, reducing the risk of future over-engineering and unexpected obligations.

