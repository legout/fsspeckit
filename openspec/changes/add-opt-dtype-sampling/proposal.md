# Change Proposal: Sample-based dtype estimation for opt_dtype

## Why
`opt_dtype` currently inspects every value in every string column with regex-heavy expressions, and PyArrow’s parallelized implementation does the same. On wide tables this repetitive scanning consumes memory, CPU, and can even OOM before the cast happens. We only need to *estimate* the target dtype before the actual cast, so a light-weight sample should suffice for the majority of real-world data while keeping the existing fallback paths.

## What
- Introduce `sample_size` and `sample_method` parameters on both the Polars and PyArrow `opt_dtype` helpers, with defaults chosen to balance cost and reliability.
- Use the provided sample (first `n` rows or a random `n` subset) to drive the regex-based detection logic, while keeping the downstream casts/downsizing safe by validating conversions on the eager data.
- Share supporting helpers where appropriate so the two implementations stay aligned in behavior and performance characteristics.
- Document the new knobs and add regression tests that cover the sampling switches.

## Impact
- Code: `src/fsspeckit/utils/polars.py` and `src/fsspeckit/utils/pyarrow.py` gain new arguments and sampling helpers.
- Tests: extend `tests/test_utils/test_utils_polars.py` and `tests/test_utils/test_utils_pyarrow.py` to exercise the new sampling parameters and guard against mis-inference.
- Docs: update the `opt_dtype` descriptions in the utilities reference so users know about the sampling trade-offs.
- No new dependencies; sampling logic keeps existing type-safety guards intact.
