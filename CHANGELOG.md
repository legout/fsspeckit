# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
