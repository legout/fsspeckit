## 1. Implementation
- [x] Update Polars `opt_dtype` to infer dtype purely from the requested sample (first/random `n`) and reuse that schema when casting the full DataFrame.
- [x] Mirror the same sample-driven schema inference in the PyArrow `opt_dtype` helper so both code paths share behavior.
- [x] Document the guarantee that `sample_size`/`sample_method` control inference and that the inferred schema is reused for casting.

## 2. Validation
- [x] Add tests demonstrating that the inferred schema remains consistent when additional rows fall outside the sampled values, for both Polars and PyArrow.
