## Why

- Documentation has drifted from the current 0.5.x implementation in several core areas:
  - Logging pages mix legacy `fsspeckit_LOG_LEVEL` guidance with the new `setup_logging()` behavior wired through the top-level package and `FSSPECKIT_LOG_LEVEL`.
  - Filesystem and caching examples reference helpers that do not exist (for example `get_cache_size`) and do not fully describe the current `filesystem()` / `DirFileSystem` path-safety semantics.
  - Parallelism and sync helpers (`run_parallel`, `sync_dir`, `sync_files`) are documented with outdated signatures that do not match the refactored implementations (generator support, keyword iterables, and new parameter names).
  - The API index and utilities guides still under-emphasize the domain packages (`core`, `storage_options`, `datasets`, `sql`, `common`) relative to the `fsspeckit.utils` façade, despite the `project-docs` spec requiring domain packages to be primary.
- This drift violates existing `project-docs` requirements (for example avoiding non-existent helpers like `get_cache_size`) and makes it harder for users to rely on the docs as a source of truth.

## What Changes

- Align core guides and reference pages with the current implementation while staying within the existing `project-docs` specification (no new requirements in this change).
- Logging:
  - Update logging examples to use the top-level `setup_logging()` entrypoint and environment variable `FSSPECKIT_LOG_LEVEL`, while still documenting `fsspeckit.common.logging.get_logger()` for application-level loggers.
  - Fix or remove the stale `fsspeckit.utils.logging` API page, replacing it with a short compatibility note that points to `fsspeckit.common.logging`.
- Filesystem and caching:
  - Correct all examples that call `fs.get_cache_size()` to use supported cache APIs on `MonitoredSimpleCacheFileSystem` (for example `cache_size` and `sync_cache()`), and describe path-preserving behavior of the cache.
  - Ensure Quickstart/API guide examples for `filesystem()` mention `dirfs=True`, base directory confinement, and the behavior when combining `base_fs` with relative paths.
- Parallelism and sync:
  - Update `run_parallel` examples to match the real signature (`func, *args, n_jobs=-1, backend="threading", verbose=True, **kwargs`) and demonstrate generator/iterable keyword argument support.
  - Rewrite `sync_files` / `sync_dir` examples to use the actual `(src_fs, dst_fs, src_path, dst_path, ...)` APIs instead of legacy `fs_target` patterns.
- Domain vs utils messaging:
  - Refresh `docs/api/index.md`, `docs/utils.md`, and the API guide so that:
    - Domain packages (`fsspeckit.core`, `fsspeckit.storage_options`, `fsspeckit.datasets`, `fsspeckit.sql.filters`, `fsspeckit.common`) are clearly presented as the canonical imports.
    - `fsspeckit.utils` is consistently described as a backwards-compatible façade and each documented helper mentions its canonical module.

## Impact

- Affected specs:
  - `project-docs` (this change implements existing requirements; no spec delta is required).
- Affected documentation (non-exhaustive):
  - `README.md`
  - `docs/index.md`
  - `docs/quickstart.md`
  - `docs/api-guide.md`
  - `docs/advanced.md` (core sections only; deeper refactors are covered in a separate change)
  - `docs/utils.md`
  - `docs/api/index.md`
  - `docs/api/fsspeckit.common.md`
  - `docs/api/fsspeckit.utils.logging.md`
- No runtime behavior changes; all code changes are confined to documentation and MkDocs configuration.

