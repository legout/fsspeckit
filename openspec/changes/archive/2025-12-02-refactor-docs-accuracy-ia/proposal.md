## Why
- Current docs overstate features (cache metrics, PerformanceTracker, Delta Lake/Pydala, plugin system) that are not present in `src/`.
- API reference and guides still center the deprecated `fsspeckit.utils.*` surface while omitting the actual domain packages (`common`, `datasets`, `sql`).
- Architecture and advanced guides confuse readers by describing components that do not exist and by missing the implemented path-safety, storage option factories, and dataset maintenance APIs.

## What Changes
- Rework documentation IA and navigation to prioritize the real domain packages and demote `fsspeckit.utils` to compatibility-only status.
- Rewrite architecture/advanced/API guides and examples to match implemented behavior: filesystem path safety/DirFileSystem, storage option creation/merging, dataset maintenance helpers, and SQL filter utilities.
- Remove or quarantine claims/examples for unimplemented features (Delta Lake write helpers, cache metrics, PerformanceTracker, plugin registry).
- Regenerate/realign the API reference to cover `core`, `storage_options`, `datasets`, `sql`, and `common`, ensuring function/class names match code (`convert_large_types_to_normal`, etc.).

## Impact
- Affected specs: project-docs
- Affected code: mkdocs.yml navigation, docs content (architecture.md, advanced.md, api-guide.md, quickstart.md, examples.md, utils.md, api/index.md, generated api pages)
