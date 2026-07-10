# Documentation Drift Audit for fsspeckit 0.22.x

## Purpose

This research asset resolves **Inventory refactor-induced documentation drift**. It compares active user-facing documentation with the current source tree, public tests, package metadata, OpenSpec requirements, and the refactor commits that established the 0.22.x layout.

It is an evidence inventory, not an implementation plan. The later information-architecture, migration, API-reference, validation, and OpenSpec-boundary decisions use these findings.

## Audit Basis

- **Authority:** current supported code and public tests win when prose conflicts with them.
- **Audience:** library users.
- **Current target:** fsspeckit 0.22.x and later.
- **User journey baseline:** an offline local dataset lifecycle tutorial, not a cloud-first walkthrough.
- **Legacy rule:** canonical imports belong in active tutorials, how-to guides, and migration routes. Legacy imports and renamed APIs belong only in reference deprecation notes.
- **Reference rule:** every supported public API must be discoverable through generated API pages plus curated reference guidance.

### Evidence sources

- Public exports and compatibility boundaries in `src/fsspeckit/__init__.py`, `storage_options/__init__.py`, `datasets/__init__.py`, and `utils/__init__.py`.
- Public/integration tests including `tests/test_basic.py`, `tests/test_backend_integration.py`, `tests/test_backend_consistency_validation.py`, and `tests/test_dataset_handler_protocol.py`.
- Current package metadata and extras in `pyproject.toml`.
- Current documentation navigation in `mkdocs.yml`.
- Current documentation requirements in `openspec/specs/project-docs/spec.md`.
- Refactor commits `9edf71c`, `9948e93`, `12a24f2`, `936af30`, and `3dc5989`.

## Confirmed Blockers and High-Severity Drift

### 1. Active documentation teaches deleted or nonexistent imports

The following active pages use imports or symbols that the current source does not provide. This prevents users from following the documented workflows and violates the requirement that documentation mirror implemented capabilities.

| Affected paths | Stale documentation | Current evidence | Required disposition |
| --- | --- | --- | --- |
| `docs/tutorials/getting-started.md` | Cloud-first setup; `storage_options_from_env`; `common.misc`; `common.types` | `storage_options` exports `from_env`; `common.misc` and `common.types` are absent; helpers now live in `common.parallel`, `common.sync`, `common.partitions`, and `datasets.types` | Rewrite as the approved offline local dataset lifecycle using canonical imports. |
| `docs/how-to/configure-cloud-storage.md` | `storage_options_from_env` in AWS, GCS, and Azure examples | Current API exports `from_env` and provider option classes’ `.from_env()` methods, but not `storage_options_from_env` | Rewrite all provider examples and add accurate extras-install guidance. |
| `docs/how-to/optimize-performance.md` | `common.misc.run_parallel`, `common.types.convert_large_types_to_normal`, `common.polars.opt_dtype_pl` | Those modules are absent after the refactor | Rewrite with canonical `common` and `datasets` module locations. |
| `docs/how-to/read-and-write-datasets.md` | `common.misc.run_parallel` | `common.misc` is absent | Rewrite the custom-processing example. |
| `docs/how-to/sync-and-manage-files.md` | `common.misc` paths for sync, partition, and parallel helpers | Implementations live in `common.sync`, `common.partitions`, and `common.parallel` | Rewrite imports and ensure examples use current public entry points. |
| `docs/explanation/architecture.md`, `docs/explanation/concepts.md`, `docs/reference/api-guide.md` | Repeated `storage_options_from_env`, `common.misc`, `common.types`, and `common.polars` usage | Current export/module layout conflicts with each of these claims | Rewrite current-layout examples and import-selection tables. |
| `docs/api/fsspeckit.utils.misc.md`, `docs/api/fsspeckit.utils.polars.md`, `docs/api/fsspeckit.utils.types.md` | Incorrect canonical target modules | Canonical implementations are split across `common.parallel`/`sync`/`partitions` and `datasets.polars`/`types` | Retain only if rewritten as accurate compatibility/deprecation reference. |

### 2. The primary tutorial contradicts the agreed user journey

`docs/tutorials/getting-started.md` leads with cloud/S3 operations and stale imports. The approved primary learning path is a narrative, copyable offline local dataset lifecycle for a Python user who can manage a virtual environment:

1. install fsspeckit;
2. configure a local filesystem;
3. create a minimal explicit schema;
4. write a dataset to a named tutorial sandbox;
5. read it back;
6. assert expected contents and show concise output;
7. explain when a simple write is sufficient and link to a merge how-to;
8. link to diagnosis, conceptual explanation, and optional-integration next steps.

**Disposition:** replace the current tutorial rather than incrementally patching its cloud-first flow.

### 3. README and example landing pages present invalid or obsolete onboarding

| Path | Confirmed problem | Required disposition |
| --- | --- | --- |
| `README.md` | Uses `common.types`; presents a `DuckDBParquetHandler` example without the required connection; references a placeholder migration version and missing `docs/migration-0.5.md` | Rewrite installation, quick-start, and migration links to the 0.22.x public surface. Use canonical dataset handler/connection APIs. |
| `examples/README.md` | Requests undeclared `common` and `storage_options` extras; refers to nonexistent `optimize_sql_query`; links to missing root `CONTRIBUTING.md` | Rewrite installation and links; remove or replace unsupported example claims. |

### 4. Active migration guidance contains false parity and removed-API claims

| Path | Confirmed problem | Required disposition |
| --- | --- | --- |
| `docs/migration/dataset-module-refactor.md` | Correctly identifies much of the class-handler migration, but claims DuckDB write operations accept `partition_by`; current `DuckDBDatasetIO.write_dataset` does not. It also overstates deduplication parity between PyArrow and DuckDB optimization methods. | Retain as the primary immediately-preceding-layout migration guide, but make backend differences explicit and correct claims against signatures/tests. |
| `docs/how-to/migrate-package-layout.md` | Has actionable layout moves, but uses deleted `datasets.pyarrow.schema`, claims old paths continue indefinitely, and presents compatibility aliases as primary | Retain only for the immediately preceding public pre-refactor layout; revise version scope, before/after mappings, and canonical imports. |
| `docs/migration/append-mode-default.md` | All examples use nonexistent `write_parquet_dataset`; current handler API is `write_dataset` | Revise to the current API or relocate it as historical-only material. |
| `docs/migration/enhanced-memory-monitoring.md` | Explicitly targets 0.8.3, unrelated to the current refactor | Relocate/archive as historical material unless a supported legacy audience is intentionally restored. |
| `docs/how-to/multi-key-examples.md`, `docs/how-to/multi-key-performance.md` | Present legacy module-level `merge_parquet_dataset_pyarrow` as an active workflow | Rewrite to class-handler APIs or clearly fence the material as legacy/deprecation reference. |

### 5. Architecture and concept pages describe unsupported systems

`docs/explanation/architecture.md` claims or implies systems not present in the current implementation, including Performance Tracking, Plugin Registry, Delta Lake integration, and advanced monitoring. The project-docs specification expressly prohibits documentation of fictitious components.

The same page also presents the deleted `common.misc`/`common.types` layout and includes ADR-style historical decision records that overlap `docs/architecture/0001-layering-rules.md` and `docs/adr/`.

**Disposition:** rewrite Explanation content around the current domain package architecture and retain only user-facing rationale. Relocate or clearly label historical decisions. Do not document unsupported systems as shipped features.

### 6. Public API discoverability is incomplete and occasionally leaks private implementation

The generated/reference surface does not provide discoverable coverage for many public or test-used modules. Missing or unexposed areas include backend dataset APIs and a broad set of common/core submodules:

- `datasets.duckdb`, `datasets.pyarrow.io`, `datasets.pyarrow.dataset`, `datasets.schema`, `datasets.types`, dataset interfaces/results;
- `common.parallel`, `common.partitions`, `common.sync`, `common.optional`, `common.datetime`, and security helpers;
- `core.incremental`, `core.filesystem` submodules, path/cache/GitLab modules, and core extension submodules;
- root-level exports and an accurate storage-options page.

`docs/api/fsspeckit.core.base.md` instead presents private `_check_cache` and `_check_file` methods with user-facing examples. Private implementation must not occupy the public reference path.

**Disposition:** define a public-API inventory from exports and public tests; generate or curate reference for that inventory; remove private methods from user-facing API documentation; demote `utils` to its compatibility-facade role.

### 7. Optional integration installation guidance is incomplete

`pyproject.toml` declares `aws`, `gcp`, `azure`, `polars`, `datasets`, `monitoring`, and `sql` extras. Current provider guides use stale APIs and do not tell users which supported extra enables each integration. Backend and monitoring/SQL docs likewise lack a clear extras-to-workflow mapping.

**Disposition:** the information-architecture decision must assign an installation matrix to an authoritative location and link provider/how-to/reference pages to it. The core local lifecycle remains free of cloud/SQL setup, with only brief integration previews.

## Confirmed Medium-Severity Drift

| Path | Finding | Required disposition |
| --- | --- | --- |
| `docs/how-to/multi-key-examples.md` | Invalid Python in `key_columns=["tenant_id", customer_id", "order_id"]` | Correct the snippet while revising the legacy merge API. |
| `docs/architecture/0001-layering-rules.md` | Historical ADR describes prior schema placement and `datasets.pyarrow.schema` duplication | Retain as history but add a current-layout status note; do not route it as active reference. |
| `docs/reference/utils.md` | Correctly calls `utils` a compatibility facade, but recommends nonexistent target paths and is too prominent in the active reference journey | Rewrite mappings and relocate/demote according to the API-reference decision. |
| Historical ADR/plan pages | `docs/adr/0003-common-layer-independence.md`, `docs/adr/0004-maintenance-execution-template.md`, and plan pages link to missing ADR `0002-merge-planning-seam.md` | Repair links or explicitly remove references while organizing historical material. |

## Navigation and Link Findings

### Confirmed

- All MkDocs Markdown targets exist on disk.
- A read-only resolver found no broken relative Markdown links across the 30 active user-facing documentation pages.
- The broken-link cluster is README/example/historical material: README → missing `docs/migration-0.5.md`, examples README → missing root `CONTRIBUTING.md`, and ADR/plan links → missing ADR `0002-merge-planning-seam.md`.
- The current site has recognizable tutorial, how-to, reference, explanation, and migration sections. The problem is accuracy, discoverability, and role discipline rather than absence of a Diátaxis-shaped directory layout.

### Suspected; requires a later build/navigation check

`mkdocs.yml` has inconsistent indentation around the memory-monitoring entry and the API Reference hierarchy. The paths themselves exist, but the indentation may misnest or invalidate the rendered navigation. This is a suspected blocker, not a confirmed build failure, because this research ticket did not run a documentation build.

## Decisions Enabled by This Audit

1. **Information architecture:** replace the cloud-first tutorial with the local dataset lifecycle and make it the homepage’s primary action; keep concept, merge, diagnosis, migration, API, and integrations as deliberate handoffs.
2. **Migration boundary:** retain only the immediately preceding public pre-refactor layout’s actionable workflow migration; scope and correct existing guides; keep legacy imports/API names only as reference deprecation notes.
3. **API reference contract:** derive supported reference coverage from public exports and public tests; combine generated symbol pages with curated import/selection/configuration/result guidance; exclude private methods.
4. **Validation/audit policy:** manually review tutorial prose, supported imports, navigation, links, buildability, API discoverability, and migration accuracy before minor releases. The team has chosen no mandatory automated documentation checks.
5. **OpenSpec scope:** the later change proposal must modify existing `project-docs` requirements rather than create a duplicate documentation capability. It must cover canonical imports, refactor-accurate examples, migration boundaries, navigation, public reference coverage, extras discoverability, and removal of unsupported architecture claims.

## Recommended Sequencing for Later Implementation Planning

1. Establish the public API inventory and canonical import matrix.
2. Establish the Diátaxis page map and homepage/nav layout, including the local lifecycle tutorial and handoffs.
3. Correct README, installation, and primary tutorial content because they are first-contact blockers.
4. Correct active how-to/reference/explanation imports and remove unsupported claims.
5. Rework migration pages, legacy compatibility/reference notes, ADR/plan placement, and broken historical links.
6. Regenerate and curate public API reference coverage; remove private API exposure.
7. Manually verify the final site’s buildability, navigation, links, and refactor accuracy before the next minor release.

## Verified Non-Issues

- Diátaxis directories already exist and can remain the organizing framework.
- The active user-facing docs do not have a broad relative-link failure; repairs can focus on the identified historical/landing-page links.
- Existing navigation targets exist on disk; the core navigation task is hierarchy and role correction, followed by a build check.
- `docs/api/index.md` correctly states that domain packages are primary and `fsspeckit.utils` is backwards compatibility, although the surrounding API pages and navigation do not consistently implement that principle.
