## 1. Information Architecture and Mapping

- [x] Inventory current top-level documentation pages (`index`, `quickstart`, `installation`, `api-guide`, `advanced`, `examples`, `utils`, `migration-0.5`, `architecture`) and confirm their current roles (tutorial, how-to, reference, explanation, or mixed).
- [x] Finalize a role-aligned target structure for `docs/` (tutorials, how-to, reference, explanation) and map each existing page/section to its new home.
- [x] Identify any external links or likely bookmarks (e.g., references to `quickstart.md`, `advanced.md`, `examples.md`, `utils.md`) that may be affected by path changes.

## 2. Tutorials: Getting Started

- [x] Create `docs/tutorials/getting-started.md` that:
  - [x] Introduces fsspeckit at a high level and describes the end-to-end "happy path" workflow.
  - [x] Walks through a minimal installation, local filesystem usage, one remote backend example, and a simple dataset operation.
  - [x] Links to installation, how-to guides, and API reference for deeper usage instead of embedding extensive reference material.
- [x] Trim `docs/quickstart.md` down to either:
  - [x] A thin wrapper that forwards readers to `tutorials/getting-started.md`, or
  - [x] A simple redirect-style stub with clear links to the new tutorial and how-to guides.
- [x] Ensure Quickstart/tutorial content no longer duplicates detailed parameter documentation that belongs in reference pages.

## 3. How-to Guides: Task-Oriented Recipes

- [x] Create `docs/how-to/configure-cloud-storage.md` with focused recipes for:
  - [x] Configuring AWS/GCP/Azure using environment variables via `storage_options_from_env`.
  - [x] Configuring providers using `StorageOptions` classes and `.to_filesystem()`.
  - [x] Creating storage options from URIs via `storage_options_from_uri`.
- [x] Create `docs/how-to/work-with-filesystems.md` covering:
  - [x] Creating local and remote filesystems with `filesystem()`.
  - [x] Path safety and `DirFileSystem` behavior (base directory confinement, `base_fs` hierarchy).
  - [x] Enabling and understanding caching at the usage level (without re-documenting cache internals).
- [x] Create `docs/how-to/read-and-write-datasets.md` covering:
  - [x] Reading JSON/CSV/Parquet and using batch options.
  - [x] Writing Parquet/CSV/JSON from DataFrames or tables.
  - [x] Basic `DuckDBParquetHandler` usage for reading/writing datasets and simple analytics.
- [x] Create `docs/how-to/use-sql-filters.md` with:
  - [x] Examples of `sql2pyarrow_filter` and `sql2polars_filter`.
  - [x] Applying translated filters to PyArrow datasets and Polars dataframes using realistic schemas.
- [x] Create `docs/how-to/sync-and-manage-files.md` with:
  - [x] Recipes for `sync_files` and `sync_dir` in local → cloud, cloud → cloud, and test-safe scenarios.
  - [x] Partition extraction utilities (e.g., `get_partitions_from_path` / `extract_partitions`) and typical patterns.
- [x] Create `docs/how-to/optimize-performance.md` with:
  - [x] Guidance on when to enable caching, choose backends, and tune batch sizes.
  - [x] Dataset compaction/optimization helpers (`optimize_parquet_dataset_pyarrow`, `compact_parquet_dataset_pyarrow`, etc.) with clear constraints.
- [x] For each new how-to guide, move or copy relevant examples from `advanced.md`, `api-guide.md`, `examples.md`, and `utils.md` and remove or slim down the original sections to avoid duplication.

## 4. Reference Layer: API Guide and Utils

- [x] Create `docs/reference/api-guide.md` as a light-weight capability index that:
  - [x] Describes major capability areas (filesystem factory, storage options, extended I/O, datasets, SQL filters, common utilities).
  - [x] Links to mkdocstrings-generated API pages (`docs/api/*.md`) instead of re-documenting function signatures.
  - [x] Cross-references how-to guides for concrete recipes.
- [x] Reshape `docs/utils.md` into `docs/reference/utils.md` (or equivalent) that:
  - [x] Presents `fsspeckit.utils` as a backwards-compatible façade.
  - [x] Provides a clear mapping table from legacy utility imports to canonical domain package imports.
  - [x] Links to domain package API reference pages for detailed behavior.
- [x] Ensure `docs/installation.md` is kept as a concise reference page:
  - [x] Requirements and supported Python versions.
  - [x] Installation/upgrade commands for `pip`, extras, and optional tools (`uv`, `pixi`).
  - [x] Optional dependencies and extras mapping for providers and datasets.

## 5. Explanation Layer: Architecture, Concepts, Migration

- [x] Move `docs/architecture.md` content into `docs/explanation/architecture.md` and:
  - [x] Trim large code blocks that belong in how-to guides.
  - [x] Emphasize domain boundaries, key design decisions, and integration patterns.
  - [x] Reference security and error-handling helpers at the conceptual level and link to the security/migration docs as needed.
- [x] Create `docs/explanation/concepts.md` by extracting conceptual sections from `advanced.md` and `architecture.md`, including:
  - [x] Why `DirFileSystem` and path safety matter.
  - [x] How fsspeckit extends fsspec I/O.
  - [x] Conceptual differences between datasets and raw files.
  - [x] High-level description of SQL filter translation.
- [x] Move `docs/migration-0.5.md` to `docs/explanation/migration-0.5.md` (or update navigation to treat it as an explanation page) and:
  - [x] Ensure it is linked from the homepage, architecture, and utils/reference docs as a discoverable upgrade resource.

## 6. MkDocs Navigation and URL Strategy

- [x] Update `mkdocs.yml` navigation to:
  - [x] Group tutorials, how-to guides, reference, and explanation sections under clearly labeled headings.
  - [x] Reduce the number of top-level nav entries and promote "Getting Started", "How-to Guides", "API Reference", and "Architecture & Concepts".
  - [x] Ensure "Installation", "Contributing", and "Migration" remain discoverable.
- [x] Decide on a URL and redirect strategy for existing pages:
  - [x] Where possible, keep existing filenames and adjust content to point into the new structure.
  - [x] Where paths must change, document the mapping in release notes and optionally add stub pages with prominent links to the new locations.

## 7. Validation and Spec Alignment

- [x] Perform a pass over the updated docs to ensure each page clearly fits one documentation role and does not mix tutorial/how-to/reference/explanation content.
- [x] Verify that narrative guides no longer duplicate detailed API semantics that are maintained in mkdocstrings-generated reference pages.
- [x] Check that domain packages (`core`, `storage_options`, `datasets`, `sql`, `common`) remain the primary API surface in examples and navigation, with `fsspeckit.utils` clearly labeled as a façade.
- [x] Update or add `project-docs` spec requirements (via this change) as needed to capture navigation and layout conventions without over-constraining future documentation evolution.
- [x] Run `openspec validate refactor-docs-diataxis-structure --strict` and address any reported issues.
