# Change: Add comprehensive documentation for PyArrow merge-aware writes

## Why
The `add-pyarrow-merge-aware-write` feature has been implemented but lacks comprehensive documentation. Users cannot discover the new merge-aware write functionality, understand the different strategies, or find practical examples. This reduces the feature's usability and adoption.

## What Changes
- Update API reference to include new `strategy` and `key_columns` parameters and convenience helpers
- Add merge-aware write examples to existing how-to guides
- Create comprehensive merge-specific documentation
- Provide working code examples for all merge strategies and use cases

## Impact
- Improves feature discoverability and usability
- Provides clear guidance on when to use each merge strategy
- Enables users to leverage merge-aware writes effectively
- Maintains documentation consistency with existing DuckDB merge functionality

