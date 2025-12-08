## Context
The `add-pyarrow-merge-aware-write` feature is fully implemented with comprehensive test coverage, but documentation is incomplete. Users cannot easily discover the new merge-aware write functionality or understand how to use different strategies effectively.

## Goals / Non-Goals
- Goals:
  - Document new `strategy` and `key_columns` parameters for `write_pyarrow_dataset`
  - Document convenience helpers (`insert_dataset`, `upsert_dataset`, `update_dataset`, `deduplicate_dataset`)
  - Provide practical examples for all merge strategies
  - Create comprehensive merge-specific how-to guide
  - Ensure consistency with existing DuckDB merge documentation
- Non-Goals:
  - Re-document existing PyArrow dataset functionality
  - Change merge semantics or behavior
  - Create new merge strategies beyond existing implementation

## Decisions
- Update API reference to include new parameters and functions
- Add merge-aware write section to existing dataset how-to guide
- Create dedicated merge datasets guide for comprehensive coverage
- Provide working examples for all strategies and use cases
- Follow existing documentation patterns and style

## Risks / Trade-offs
- Documentation maintenance overhead for new content
- Risk of documentation drift if implementation changes
- Trade-off: More comprehensive documentation improves usability but requires ongoing maintenance

## Migration Plan
1. Update API reference with new parameters and functions
2. Add merge-aware examples to existing how-to guide
3. Create comprehensive merge datasets guide
4. Add working code examples for all strategies
5. Update documentation index and cross-references

## Open Questions
- Should merge-aware write examples include performance benchmarks comparing traditional vs merge-aware approaches?
- Should we create a separate "Advanced Merges" guide or keep everything in one comprehensive guide?