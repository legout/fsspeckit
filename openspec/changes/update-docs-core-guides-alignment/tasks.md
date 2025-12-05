## 1. Logging and Environment Variable Docs

- [x] 1.1 Update logging examples in `README.md`, `docs/utils.md`, and `docs/api/fsspeckit.common.md` to use the top-level `setup_logging()` entrypoint and `FSSPECKIT_LOG_LEVEL`.
- [x] 1.2 Document how `fsspeckit.common.logging.get_logger()` integrates with the package-level logging configuration.
- [x] 1.3 Replace or deprecate `docs/api/fsspeckit.utils.logging.md` so it points users to `fsspeckit.common.logging` and no longer references `fsspec-utils`.

## 2. Filesystem and Caching Examples

- [x] 2.1 Search docs for `get_cache_size` and replace usages with supported cache APIs (`cache_size`, `sync_cache`) or remove them when unnecessary.
- [x] 2.2 Update Quickstart/API guide filesystem examples to describe `filesystem()` behavior, including:
  - DirFileSystem wrapping (`dirfs=True` default).
  - Base-path enforcement and relative path resolution when `base_fs` is provided.
  - Interaction between `cached=True`, `cache_storage`, and `MonitoredSimpleCacheFileSystem`.

## 3. Parallelism and Sync Helper Docs

- [x] 3.1 Update all `run_parallel` examples in `docs/utils.md`, `docs/api/fsspeckit.common.md`, and `docs/api-guide.md` to match the current signature and semantics (iterables, generators, `backend`, `n_jobs`, `verbose`).
- [x] 3.2 Rewrite `sync_files` / `sync_dir` examples to use `(src_fs, dst_fs, src_path, dst_path, ...)` and show both sequential and parallel patterns.
- [x] 3.3 Ensure optional dependency constraints (for example requiring `fsspeckit[datasets]` for `run_parallel`) are documented alongside these helpers.

## 4. Domain vs Utils Messaging

- [x] 4.1 Update `docs/api/index.md` to clearly present domain packages as the primary API surface and describe `fsspeckit.utils` as a compatibility façade only.
- [x] 4.2 Refresh `docs/utils.md` so each documented helper lists its canonical module (for example `DuckDBParquetHandler` from `fsspeckit.datasets`, `merge_parquet_dataset_pyarrow` from `fsspeckit.datasets.pyarrow`).
- [x] 4.3 Verify MkDocs navigation (`mkdocs.yml`) still reflects the "Domain Packages first, Utils compatibility section" structure required by `project-docs`.

## 5. Validation

- [x] 5.1 Run `uv run mkdocs build` (or equivalent) to ensure there are no broken links or import errors in API pages.
- [x] 5.2 Run `openspec validate --strict` to confirm `project-docs` requirements remain satisfied after the documentation edits.

