## 1. Implementation

- [x] 1.1 Add a `merge_parquet_dataset_pyarrow` helper to
        `src/fsspeckit/utils/pyarrow.py` under the `utils-pyarrow` capability.
- [x] 1.2 Implement strategy dispatch for `upsert`, `insert`, `update`,
        `full_merge`, and `deduplicate`, mirroring the semantics of
        `DuckDBParquetHandler.merge_parquet_dataset`.
- [x] 1.3 Implement input validation for:
        - supported strategies
        - existence of all `key_columns` in both source and target
        - absence of NULLs in key columns (raise `ValueError` otherwise)
        - schema compatibility using existing `utils-pyarrow` helpers
          (e.g. `_is_type_compatible`, `unify_schemas`, `cast_schema`).
- [x] 1.4 Implement merge execution using `pyarrow.dataset` and
        `pyarrow.compute`:
        - Stream the source in batches (for example, using a simple row-chunk
          on the input table or dataset).
        - For each source batch, derive the set of key values and build a
          filter such as `pc.field(key).is_in(batch_keys)` for the target
          dataset.
        - Use `Dataset.scanner(filter=...)` / `scanner.to_table()` to load only
          the relevant subset of the target into memory for that batch.
        - Combine the batch results into a final merged representation without
          ever calling unfiltered `dataset.to_table()` on the full target.
- [x] 1.5 Implement deduplication logic for the `deduplicate` strategy by
        deduplicating the source on `key_columns` (using `Table.sort_by` and
        group operations or `pc.unique`) before applying UPSERT semantics.
- [x] 1.6 Implement merge statistics calculation so that the returned dict
        contains `inserted`, `updated`, `deleted`, `total` and matches the
        DuckDB helper’s interpretation.
- [x] 1.7 Implement writing of the merged dataset back to `target_path` using
        `pyarrow.parquet` and an fsspec filesystem, with a configurable
        `compression` codec.

## 2. Testing

- [x] 2.1 Add unit tests to `tests/test_utils/test_utils_pyarrow.py` (or a new
        `test_pyarrow_merge.py`) to cover:
        - UPSERT, INSERT, UPDATE, FULL_MERGE, and DEDUPLICATE behaviors on
          small synthetic datasets (both Table- and path-based sources).
        - Composite key merges (e.g. `["user_id", "date"]`).
        - Schema mismatch and NULL key validation errors.
- [x] 2.2 Add tests that simulate larger targets by many small files and verify
        that:
        - the merge results are correct, and
        - the code path uses filtered dataset scans (for example, via a small
          wrapper that asserts no full unfiltered `.to_table()` is invoked).

## 3. Documentation

- [x] 3.1 Add a “PyArrow Parquet Merge” section to `docs/utils.md`:
        - Explain the merge strategies and their semantics.
        - Emphasize that only the necessary parts of the target are scanned
          into memory via filters.
- [x] 3.2 Update any relevant API docs (e.g.
        `docs/api/fsspeckit.utils.pyarrow.md`) to include the new helper,
        signature, and examples.

## 4. Validation

- [x] 4.1 Run targeted tests:
        `pytest tests/test_utils/test_utils_pyarrow.py -k \"merge\"`.
- [x] 4.2 Run `ruff check src/fsspeckit/utils/pyarrow.py`.
- [x] 4.3 Run `mypy --ignore-missing-imports src/fsspeckit/utils/pyarrow.py`.
- [x] 4.4 Validate OpenSpec:
        `openspec validate add-pyarrow-dataset-merge --strict`.

