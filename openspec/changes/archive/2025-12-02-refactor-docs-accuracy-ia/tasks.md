## 1. Information Architecture & Navigation
- [x] 1.1 Finalize revised docs IA that foregrounds domain packages (`core`, `storage_options`, `datasets`, `sql`, `common`) and marks `utils` as compatibility-only.
- [x] 1.2 Update `mkdocs.yml` nav to match the new IA and remove/deprecate unsupported pages/links.
- [x] 1.3 Regenerate API reference entries for domain packages; remove or demote deprecated `fsspeckit.utils.*` pages.

## 2. Content Accuracy & Corrections
- [x] 2.1 Rewrite `architecture.md` to describe only implemented components (filesystem path safety, storage options factories, dataset operations) and remove fictional systems (PerformanceTracker, plugin registry, circuit breakers).
- [x] 2.2 Rewrite `advanced.md` and `examples.md` to drop unimplemented workflows (Delta Lake/Pydala helpers, cache metrics) and add supported ones (DirFileSystem safety, dataset maintenance, SQL filter helpers, storage options from env/URI).
- [x] 2.3 Update `quickstart.md` to use domain-package imports, correct function names, and clarify `utils` façade status.

## 3. Accuracy Enforcement & Terminology
- [x] 3.1 Ensure all docs reference actual functions/classes (e.g., `convert_large_types_to_normal`, `DuckDBParquetHandler`, `sql2pyarrow_filter`) and avoid APIs not present in `src/`.
- [x] 3.2 Add explicit notes where features are out of scope or future (e.g., Delta Lake helpers, performance tracking), rather than presenting them as implemented.

## 4. Validation
- [x] 4.1 OpenSpec validation completed via `/openspec:apply` command.
- [x] 4.2 Successfully ran `uv run mkdocs build` to confirm nav/pages compile without errors (only minor type annotation warnings).
