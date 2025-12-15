# Change: Add comprehensive documentation for PyArrow merge-aware writes

## Why
The `add-pyarrow-merge-aware-write` feature has been implemented but lacks comprehensive documentation. Users cannot discover new merge-aware write functionality, understand different strategies, or find practical examples. This reduces the feature's usability and adoption despite its complete implementation.

## What Changes
- Update API reference to include new `strategy` and `key_columns` parameters and convenience helpers
- Add merge-aware write examples to existing how-to guides
- Create comprehensive merge-specific documentation  
- Provide working code examples for all merge strategies and use cases

## Impact
- Improves feature discoverability and usability for PyArrow merge-aware writes
- Provides clear guidance on when to use each merge strategy
- Enables users to leverage merge-aware writes effectively without trial-and-error
- Maintains documentation consistency with existing DuckDB merge functionality
- Reduces support burden by providing comprehensive self-service documentation

## Files Changed
- `docs/api/fsspeckit.core.ext.md` - Updated with new parameters and functions
- `docs/how-to/read-and-write-datasets.md` - Added merge-aware write section
- `docs/how-to/merge-datasets.md` - New comprehensive merge guide
- `docs/how-to/index.md` - Added link to merge guide
- `examples/pyarrow/pyarrow_merge_example.py` - Comprehensive merge examples
- `examples/datasets/getting_started/04_pyarrow_merges.py` - Beginner merge examples
- `examples/datasets/getting_started/03_simple_merges.py` - Added merge-aware section
- `examples/README.md` - Updated with new examples

## Testing
- All code examples tested and verified to work with current implementation
- Documentation build process validated with new content
- Cross-references and links verified to work correctly