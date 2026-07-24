# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Maintenance planning accepts pre-collected `file_stats=` on every coordinator `plan_*` method and the shared `_prepare_plan_inputs` builder, letting callers with a Parquet `_metadata` sidecar (e.g. pydala2) supply per-file `{path, size_bytes, num_rows}` plus an optional `schema_arrow`/`codecs` snapshot and skip the filesystem walk (`fs.ls`) and footer scan entirely. The partition filter, source-snapshot capture, schema reconciliation, and grouping still run; a caller that also supplies a schema/codec snapshot plans with **zero** footer reads. The source snapshot records the true on-disk file size (via `fs.info`) so advisory sidecar sizes do not break drift detection. Planning without `file_stats=` is unchanged. (#67)

### Changed

- Maintenance planning reads each in-scope Parquet footer once instead of up to three times: the row count, Arrow schema, and per-column codec set are harvested from a single footer open and reused by schema reconciliation and codec selection, cutting planning from ~3N to ~N footer opens (biggest win on object storage). The public `collect_dataset_stats` contract is unchanged. (#66)

## [0.27.2] - 2026-07-24

### Fixed

- Maintenance reconciles compatible Parquet schemas during compaction, coordinated optimization, and best-effort execution: `string`/`large_string` and other offset-width, integer-widening, and float-widening variants now promote to a lossless common target schema (`SchemaOutcome.LOSSLESS_PROMOTED`) instead of rejecting the plan, with each input cast to the target schema before concatenation. Genuinely incompatible types and schema/field metadata conflicts still invalidate the plan before any file is mutated. (#65)

## [0.27.1] - 2026-07-23

### Added

- Null-safe (`IS NOT DISTINCT FROM`) equality for nullable merge key columns across the PyArrow and DuckDB backends: NULL matches NULL, NULL never matches a non-null value, and composite keys match only when every component matches; native typed fast paths for non-null keys are unchanged, and the former null-key rejection helpers are now backward-compatible no-ops. (#64)

### Fixed

- Null-safe merge key edge cases.

## [0.27.0] - 2026-07-20

### Added

- Partition-ordered compaction (`OrderedCompactionPlan`, `SortKey`, `plan_ordered_compaction`) producing one globally ordered output sequence per physical partition, split into contiguous `max_rows_per_file`-bounded chunks, with an external merge sort bounded by `memory_budget_mb` and an optional spill directory. Adjacent output files form a single sorted run; ordinary compaction remains explicitly unordered. (#61)
- Caller-directed schema rewrite (`SchemaRewritePlan`, `CastPolicy`, `plan_schema_rewrite`) that publishes an explicitly supplied target schema under a typed cast policy (STRICT/SAFE/LOOSE), validates every cast across the full rewrite scope before publication, and never invokes dtype inference. `opt_dtype` helpers remain proposal-only. (#62)

## [0.26.0] - 2026-07-17

### Added

- Coordinated global repartitioning can derive timestamp-based hive partition keys with explicit timezone and validation rules.

### Changed

- Python 3.12 is now the minimum supported version; CI covers Python 3.12, 3.13, and 3.14.
- Compaction plans expose singleton skip reasons and honor explicit codec-change intent.

### Fixed

- Global repartitioning keeps destination partition columns as path-only metadata, including integer-valued hive partitions.
- Best-effort object-store compaction preserves hive partition subtrees.
- PyArrow reads accept DNF tuple filters, including conjunctions and disjunctions.
- Multi-file writes concatenate Polars DataFrame inputs for CSV, JSON, and Parquet outputs.

## [0.25.0] - 2026-07-15

### Added

- Coordinator-backed filesystem maintenance façades for planning and one-call compaction, deduplication, repartitioning, and optimization.

### Changed

- Maintenance now returns typed plans and results through the coordinator workflow.

### Removed

- Legacy dictionary-returning PyArrow and DuckDB maintenance helpers and their `dry_run` execution mode. See the maintenance migration guide for replacements.

## [0.24.1] - 2026-07-13

### Fixed

- Import PyArrow and `pyarrow.dataset` at runtime for the filesystem dataset helpers, preventing `NameError: name 'pds' is not defined`.

## [0.24.0] - 2026-07-13

### Added

- Coordinated maintenance execution for local and object-store datasets, including atomic-local compaction and best-effort object-store compaction.
- Partition-preserving local compaction and partition-local deduplication with typed plans, results, validation, rollback, and recovery reporting.
- Object-store partition-local and global-repartitioning deduplication through generic fsspec filesystems, with explicit destination partitioning and source-drift protection.

## [0.23.0] - 2026-07-13

### Added

- `MergeStrategy` is now re-exported from `fsspeckit.datasets.duckdb` for DuckDB-oriented callers.

### Changed

- Example validation now resolves paths from the examples directory and imports discovered modules during `run_examples.py --validate`.
- The standalone `examples/test_examples.py` CLI now supports opt-in runtime checks with configurable and smoke-test timeouts.
- Directory filesystem examples detect whether `s3fs` is installed without importing it, so their local demonstrations remain available when the AWS extra is absent.

## [0.22.0] - 2026-07-10

### Fixed

- **Package-level merge import recovered.** `MergeTargetMetadata`, `plan_merge_operation`, and `resolve_merge_plan_early_exit` added to `core.merge`, fixing the `ImportError` that blocked both dataset backends. (#19)
- **Missing-target UPDATE parity preserved.** UPDATE against a non-existent target now fails before any no-op result, regardless of source size, with the established `non-existent target` error wording across both backends. (#20)
- **Accurate UPDATE plan validation.** Strategy compatibility is now evaluated from the actual prepared source-row count instead of a hardcoded sentinel, so valid nonempty UPDATE plans report truthful compatibility metadata. (#21)

## [0.21.0] - 2026-07-08

### Added

- `execute_compaction_template` in `core.maintenance` — shared template for compaction execution lifecycle (dry-run, output path generation, group iteration, file removal, stats return). Backends supply a single `compact_group_fn` callback. (#14)
- ADR-0003 (common layer independence) and ADR-0004 (maintenance execution template).

### Changed

- **Common layer is now dependency-free.** `schema`, `types`, and `polars` modules moved from `common` to `datasets`. (#8, #9)
- **`core/ext` carved as its own tier** in the layering rules. The `ALLOWLIST` for common→core imports is removed; the `check_layering.py` script enforces the new rules. (#7)
- **`common/logging_config.py` collapsed into `common.logging` package** — single import path. (#10)
- **Optional dependency seam consolidated.** All inline `try/except` guards and bare top-level imports of optional packages in `common` are routed through `common.optional`. (#11)
- **`common/misc.py` split** into `common.parallel` (parallelism), `common.sync` (filesystem sync), and `common.partitions` (partition helpers). The `misc` catch-all is deleted. (#17)
- DuckDB and PyArrow compaction backends now consume `execute_compaction_template` instead of inlining execution boilerplate. (#15, #16)
- Merge planning seam completed with parity tests. (#5)

### Removed

- `common/misc.py` (split into `parallel`, `sync`, `partitions`)
- `common/logging_config.py` (collapsed into `common.logging`)
- `ALLOWLIST` mechanism from `check_layering.py` (replaced by strict layering rules)

## [0.20.1] - 2026-02-14

### Changed

- Hive-partitioned merges no longer inject partition columns into rewritten file schemas.
- DuckDB merge no longer uses legacy `use_merge` routing helpers (removed unused code paths).
- PyArrow write_dataset defaults to hive partitioning when partition_by is set.

### Fixed

- Partitioned merge inserts now write to hive partition directories in DuckDB and PyArrow backends.
- PyArrow merge enforces partition immutability for existing keys.
- DuckDB `compact_parquet_dataset_duckdb` executes for non-dry runs.
- Dataset write path validation now auto-creates missing parent directories.
- Fixed partition pruning bug that could cause partition immutability validation to be skipped.

## [0.9.1] - 2026-01-08

### Added

- **DuckDB MERGE Statement Support**: Native MERGE statement support for DuckDB 1.4.0+
  - MERGE implementation for INSERT, UPDATE, and UPSERT strategies
  - Version detection and automatic fallback to UNION ALL for older DuckDB versions
  - `use_merge` parameter to explicitly control merge strategy
  - Expected 20-40% performance improvement over UNION ALL approach
  - Full ACID compliance for merge operations

### Changed

- Enhanced merge operation routing to support both MERGE and UNION ALL implementations
- Improved error handling for merge operations with better error messages

### Fixed

- Fixed MERGE implementation to properly handle all three merge strategies separately
- Added accurate insert/update count tracking for MERGE operations

### Technical Details

- MERGE is automatically enabled for DuckDB >= 1.4.0
- Automatic fallback to UNION ALL for DuckDB < 1.4.0
- `use_merge=True` forces MERGE (requires DuckDB >= 1.4.0)
- `use_merge=False` forces UNION ALL fallback
- `use_merge=None` (default) auto-detects based on DuckDB version

### Migration Notes

No migration required - existing code works unchanged with improved performance when DuckDB >= 1.4.0 is available.

## [0.9.0] - Previous Release

### Added

- Initial release with multi-cloud storage configuration
- Enhanced caching with monitoring
- Multi-format I/O operations (JSON, CSV, Parquet)
- Dataset operations for PyArrow and DuckDB
- SQL-to-filter translation helpers
- Domain-specific packages for better discoverability
