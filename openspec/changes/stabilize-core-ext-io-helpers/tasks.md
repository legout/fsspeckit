## 1. Implementation

- [ ] 1.1 Refactor `fsspeckit.core.ext_io` imports to use `TYPE_CHECKING` and `fsspeckit.common.optional` helpers for Polars, Pandas, and PyArrow.
- [ ] 1.2 Rewrite `write_files` to:
  - [ ] 1.2.1 Normalise `path` to a list and pair each data element with a corresponding path by index.
  - [ ] 1.2.2 Fix the non-threaded execution branch (`use_threads=False`) so it calls `_write` with correct arguments and does not iterate over string paths.
  - [ ] 1.2.3 Keep mode handling (`append`, `overwrite`, `delete_matching`, `error_if_exists`) clear and well-structured.
- [ ] 1.3 Simplify `read_files` and `write_file` format dispatch using a small format → handler map.
- [ ] 1.4 Ensure `fsspeckit.core.ext_csv` and `fsspeckit.core.ext_parquet` follow the same lazy optional-dependency patterns and do not import optional packages at module import time.

## 2. Testing

- [ ] 2.1 Add or extend tests for `write_files` covering:
  - [ ] 2.1.1 Single path (string) vs list of paths for each format.
  - [ ] 2.1.2 `use_threads=True` and `use_threads=False` for all supported modes.
- [ ] 2.2 Add or extend tests for `read_files` ensuring:
  - [ ] 2.2.1 Each format delegates to the correct underlying helper.
  - [ ] 2.2.2 Behaviour when optional dependencies are missing is a clear `ImportError` with the documented message.

## 3. Documentation

- [ ] 3.1 Update reference documentation to clarify:
  - [ ] 3.1.1 Behaviour of `read_files` / `write_files` in threaded and non-threaded modes.
  - [ ] 3.1.2 Requirements for optional dependencies and the extras groups required for each format.

