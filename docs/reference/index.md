# Reference

Reference is the authoritative surface for fsspeckit's public API. It splits
into two layers: curated guidance that helps you pick and configure the right
API, and generated pages that give the exact signatures and docstrings.

## Where to start

- [API Guide](api-guide.md) - how to choose imports, backends, and configuration; how to interpret results.
- [Public API Inventory](public-api-inventory.md) - the explicit list of supported public symbols, canonical imports, extras, and compatibility status.
- [Generated API](../api/index.md) - authoritative signatures and docstrings for every supported module.

## Topical reference

- [Dataset Handlers](../dataset-handlers.md) - the shared `write_dataset` / `merge` / maintenance interface across DuckDB and PyArrow.
- [Storage Options](storage-options.md) - how to configure local, cloud, and Git providers.
- [Adaptive Key Tracking](adaptive-key-tracking.md) - tiered memory management for large-key deduplication.
- [Multi-Key API](multi-key-api.md) - vectorized multi-column key helpers for PyArrow.
- [Legacy Imports](legacy-imports.md) - deprecation mappings from old import paths to canonical domain imports.

## Integration

- [Installation and extras matrix](../installation.md) - the single authoritative mapping of extras (`aws`, `gcp`, `azure`, `polars`, `datasets`, `monitoring`, `sql`) to the workflows and providers they enable.

## Compatibility

- [Utils compatibility](utils.md) - why `fsspeckit.utils` is a deprecated facade and where to import instead.
