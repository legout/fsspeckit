## Context
The `add-pyarrow-dataset-handler` implementation is complete with comprehensive class-based API (`PyarrowDatasetIO`, `PyarrowDatasetHandler`) that provides full parity with DuckDB handler. However, documentation only covers DuckDB classes and function-based PyArrow approaches, creating a documentation gap that hinders discoverability and adoption of the new class-based PyArrow interface.

## Goals / Non-Goals
- Goals:
  - Provide complete documentation coverage for PyArrow class-based interface
  - Maintain consistency with existing DuckDB documentation format and structure
  - Present both function-based and class-based PyArrow approaches as valid options
  - Enable easy backend switching between DuckDB and PyArrow with similar ergonomics
  - Provide clear guidance on when to use each PyArrow approach
- Non-Goals:
  - Remove or deprecate existing function-based PyArrow documentation
  - Change existing DuckDB documentation
  - Modify implementation code (documentation-only change)

## Decisions
- Documentation will follow same format and structure as existing DuckDB documentation for consistency
- Both function-based and class-based PyArrow approaches will be presented as valid options
- Clear migration guidance will be provided between approaches
- All examples will be working and tested
- Documentation will emphasize API parity between DuckDB and PyArrow handlers

## Risks / Trade-offs
- Documentation volume increases significantly, but improves completeness
- Users may face choice paralysis between two PyArrow approaches, but guidance will clarify use cases
- Maintenance burden increases, but provides better user experience

## Migration Plan
1) Update API reference with complete PyArrow class documentation
2) Update dataset handlers documentation with class-based PyArrow section
3) Update getting started tutorial with PyArrow examples
4) Update API guide capability overview
5) Update how-to guides with class-based PyArrow examples
6) Validate all examples and cross-references

## Open Questions
- Should we create a dedicated "Choosing Your PyArrow Approach" section to help users decide between function-based vs class-based?
- How much detail should we provide in migration guidance from function-based to class-based PyArrow?