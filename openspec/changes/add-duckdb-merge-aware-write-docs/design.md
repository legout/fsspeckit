## Context
The `add-duckdb-merge-aware-write` feature is fully implemented with comprehensive test coverage and excellent examples, but documentation is incomplete. Users cannot easily discover new merge-aware write functionality or understand how to use different strategies effectively. OpenSpec tasks incorrectly mark documentation as complete when substantial gaps remain.

## Goals / Non-Goals
- Goals:
  - Document new `strategy` and `key_columns` parameters for `write_parquet_dataset`
  - Document convenience helpers (`insert_dataset`, `upsert_dataset`, `update_dataset`, `deduplicate_dataset`) on DuckDB backends
  - Integrate existing comprehensive examples into documentation structure
  - Provide guidance on when to use DuckDB vs PyArrow for merge operations
  - Correct OpenSpec task status to reflect actual documentation state
- Non-Goals:
  - Re-document existing DuckDB dataset functionality
  - Change merge semantics or behavior
  - Create new merge strategies beyond existing implementation

## Decisions
- Update API reference to include new parameters and functions for DuckDB
- Add merge-aware write examples to existing how-to guides
- Create unified merge datasets guide covering both DuckDB and PyArrow
- Integrate existing excellent examples rather than creating new ones
- Provide backend selection guidance for different use cases

## Risks / Trade-offs
- Documentation maintenance overhead for new content
- Risk of documentation drift if implementation changes
- Trade-off: More comprehensive documentation improves usability but requires ongoing maintenance
- Challenge: Maintaining consistency between DuckDB and PyArrow documentation

## Migration Plan
1. Update API reference with DuckDB merge-aware write parameters and functions
2. Add DuckDB merge-aware examples to existing how-to guide
3. Create unified merge datasets guide covering both backends
4. Integrate existing comprehensive examples into documentation structure
5. Add backend comparison and selection guidance
6. Update OpenSpec task status to reflect actual completion state

## Open Questions
- Should we create separate DuckDB and PyArrow merge guides or keep unified approach?
- How should we handle version differences in merge capabilities between backends?
- Should performance benchmarks be included to help users choose backends?