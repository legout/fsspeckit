# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
