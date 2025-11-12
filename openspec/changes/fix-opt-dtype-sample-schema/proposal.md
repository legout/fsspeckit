# Change Proposal: Ensure `opt_dtype` honors sample-based schema inference

## Why
`opt_dtype` today iterates over entire columns even when `sample_size` is set. Users expect that the sample is their primary signal—especially for large tables where inspecting every row is prohibitively expensive. That expectation currently fails once inference continues to scan the remaining rows before casting, defeating the whole purpose of sampling and causing wasted CPU/memory (and sometimes OOMs).

## What
- Adjust the Polars and PyArrow implementations so `sample_size`/`sample_method` control the schema inference stage, then reuse the inferred dtype for casting the full column instead of re-running regex parsing on every value.
- Ensure both helpers expose the same sampling knobs in their public signatures and keep the optimized schema derived from the sample for downstream casting (and shrink decisions).
- Add regression tests and documentation covering the sample-driven behavior, illustrating how first/random sampling affects dtype inference.

## Impact
- Specs: Add requirements for the sample-driven inference capability.
- Code: `src/fsspeckit/utils/polars.py` and `src/fsspeckit/utils/pyarrow.py` need updates around `_optimize_string_column/_optimize_string_array` and their callers.
- Docs: Update the API reference and `docs/utils.md` to describe the sample schema guarantee.
- Tests: Extend `tests/test_utils/test_utils_polars.py` and `tests/test_utils/test_utils_pyarrow.py` so they assert the optimized schema is based solely on the sample and remains stable when new data is appended.
