## 1. Implementation
- [x] Add `sample_size`/`sample_method` arguments and sampling helpers to the Polars `opt_dtype` implementation.
- [x] Mirror the sampling strategy in the PyArrow `opt_dtype`, keeping the shared inference behavior in sync with Polars.
- [x] Update the documentation for both helpers to describe the new sampling knobs and their defaults.
- [x] Extend the Polars and PyArrow unit tests to cover the sampling parameters and mis-inference fallbacks.

## 2. Validation
- [x] Run the Polars/pyarrow dtype utility tests (ideally using Python 3.11+) to ensure the new parameters and fallback code behave as expected.
