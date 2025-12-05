## Context

The `project-docs` specification already defines the high-level expectations for documentation accuracy, domain package prioritisation, and the `fsspeckit.utils` façade. Recent refactors (logging standardisation, filesystem/cache refactor, `run_parallel` and sync updates, utils façade) have landed in code and partially in docs, but some key guides are now out of sync:

- Logging docs mix legacy environment variable names and modules with the new `setup_logging()` entrypoint and `FSSPECKIT_LOG_LEVEL`.
- Filesystem and caching examples still show non-existent helpers like `get_cache_size` and do not fully describe DirFileSystem path-safety or the monitored cache behaviour.
- Parallelism and sync helpers are documented with pre-refactor signatures (simple `items` lists, old `sync_files`/`sync_dir` parameter shapes).
- The API index and utils docs underplay the domain packages and still treat `fsspeckit.utils` as if it were the primary surface in some places.

This change is intentionally *implementation-only* for docs: it does not add or modify requirements in `project-docs`, but brings the content back into alignment with the existing spec and the current code.

## Design Goals

- **Accuracy first:** All examples and narrative in the affected docs MUST match implemented APIs (names, signatures, behaviour).
- **Domain-centric:** Domain packages (`core`, `storage_options`, `datasets`, `sql`, `common`) MUST be presented as the canonical imports, with `fsspeckit.utils` clearly positioned as a façade.
- **Minimal disruption:** Preserve URLs, headings, and overall page structure where possible to avoid breaking external links and bookmarks.
- **Reduced duplication:** Where multiple pages cover the same helper, ensure there is one “canonical” example and other pages link to it instead of duplicating slightly different snippets.
- **No runtime change:** Limit edits to Markdown and MkDocs configuration; do not modify Python modules as part of this change.

## Scope and Boundaries

**In scope**

- Top-level docs:
  - `README.md`
  - `docs/index.md`
- Core guides:
  - `docs/quickstart.md`
  - `docs/api-guide.md`
  - `docs/advanced.md` (sections that touch logging, filesystem, caching, parallelism, sync, utils imports)
  - `docs/utils.md`
- API index and entry pages:
  - `docs/api/index.md`
  - `docs/api/fsspeckit.common.md`
  - `docs/api/fsspeckit.utils.logging.md`
- MkDocs configuration:
  - `mkdocs.yml` (only if needed to clarify labels for utils vs domain packages; the current nav structure is already spec-compliant).

**Out of scope**

- Changes to `project-docs` spec (handled by a separate change).
- Introducing new public Python APIs.
- Large-scale restructuring of navigation (already done in earlier IA refactors).

## Content Design

### Logging

- **Canonical entrypoint:** Document `from fsspeckit import setup_logging` as the primary package-level logging configuration, wired through `fsspeckit.common.logging_config`.
- **Environment variables:** Prefer `FSSPECKIT_LOG_LEVEL` in examples; mention `fsspeckit_LOG_LEVEL` only where `loguru`-based helpers explicitly support it and clarify the distinction.
- **Per-module loggers:** Show `from fsspeckit.common.logging import get_logger` in advanced examples and explain that it integrates with `setup_logging`.
- **Utils logging page:** Replace the `fsspeckit.utils.logging` API page with a short explanation that logging is now provided via `fsspeckit.common.logging` and `setup_logging`, keeping the page as a compatibility pointer rather than a full reference.

### Filesystem and Caching

- **Filesystem examples:** Use:
  - `from fsspeckit import filesystem` for high-level usage; and/or
  - `from fsspeckit.core.filesystem import filesystem` for advanced patterns.
- **Path safety:** Explain DirFileSystem behaviour:
  - Default `dirfs=True` when appropriate.
  - Base-path confinement when `base_fs` is provided.
  - Validation rules for relative paths (no escaping the base directory).
- **Caching:** Describe:
  - `cached=True` wrapping the underlying filesystem in `MonitoredSimpleCacheFileSystem`.
  - Path-preserving cache layout and `cache_storage` semantics.
  - Supported APIs like `sync_cache()` and `cache_size`.
- **Removal of fictitious helpers:** Replace any `get_cache_size` references with supported patterns or remove them if not essential to the narrative.

### Parallelism and Sync

- **`run_parallel`:**
  - Reflect the real signature `run_parallel(func, *args, n_jobs=-1, backend="threading", verbose=True, **kwargs)`.
  - Demonstrate:
    - Single and multiple iterable arguments.
    - Iterable keyword arguments.
    - Generator inputs.
  - Call out the optional dependency requirement (`fsspeckit[datasets]` / `joblib`).
- **`sync_files` / `sync_dir`:**
  - Replace legacy examples with the current form:
    - `sync_dir(src_fs, dst_fs, src_path=..., dst_path=..., ...)`
    - `sync_files(add_files, delete_files, src_fs, dst_fs, ...)` for more advanced usage.
  - Show both sequential and parallel patterns (and, where relevant, server-side copy behaviour).

### Domain vs Utils Messaging

- **API index:** Ensure `docs/api/index.md` states that domain packages form the primary surface and that utils modules are compatibility façades.
- **Utils reference:** In `docs/utils.md` and utils API pages:
  - For each documented helper, list its canonical module (for example `DuckDBParquetHandler` from `fsspeckit.datasets`, `merge_parquet_dataset_pyarrow` from `fsspeckit.datasets.pyarrow`).
  - Emphasise that new code should import from domain packages.

## Risks and Mitigations

- **Risk:** Inconsistent updates across multiple pages could introduce new contradictions.
  - **Mitigation:** Treat one page as canonical per topic (for example, `docs/utils.md` for utility examples) and cross-link from others; perform a focused review after editing to check for mismatched signatures.

- **Risk:** Removing or renaming sections may break deep links or user bookmarks.
  - **Mitigation:** Avoid changing filenames or top-level headings; where content must move, leave short stubs or update the text in place instead of relocating it.

- **Risk:** Future refactors could reintroduce drift.
  - **Mitigation:** This change pairs with `add-docs-security-migration-style`, which adds stronger guardrails in `project-docs` and encourages CI checks to detect common drift markers.

