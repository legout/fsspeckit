## Context
The dataset module has parallel PyArrow and DuckDB implementations with diverging parameter sets, defaults, and error behaviors. Several shared responsibilities (validation, planning, error handling, and result shaping) are duplicated, which complicates maintenance and parity guarantees.

## Goals / Non-Goals
- **Goals**:
  - Provide a single canonical dataset handler API with aligned signatures and defaults.
  - Centralize shared validation/normalization logic for consistent behavior.
  - Preserve existing functionality while reducing duplication.
  - Normalize optional dependency loading and error messaging.
- **Non-Goals**:
  - Introduce new storage backends or file formats.
  - Change core merge semantics beyond parity alignment.
  - Add new external dependencies.

## Decisions
- **Decision**: Use class-based handlers (PyarrowDatasetIO, DuckDBDatasetIO) as the canonical API.
  - **Why**: Enables consistent signatures, encapsulated state, and parity across backends.
- **Decision**: Consolidate shared validation and normalization in datasets/base.py and/or core helpers.
  - **Why**: Eliminates duplicate code and enforces consistent error messages.
- **Decision**: Standardize result types (WriteDatasetResult, MergeResult) across backends.
  - **Why**: Improves downstream interoperability and parity tests.
- **Decision**: Enforce dependency checks at handler construction or call sites with actionable ImportError messages.
  - **Why**: Aligns with datasets-conditional-loading requirements and prevents late failures.

## Risks / Trade-offs
- **Risk**: Hidden backend-specific behaviors may regress during unification.
  - **Mitigation**: Add parity contract tests and keep changes incremental.
- **Risk**: API alignment may surface previously undocumented differences.
  - **Mitigation**: Document any unavoidable differences explicitly in docs.

## Migration Plan
1. Inventory parity gaps and document current differences.
2. Align signatures/defaults and consolidate shared logic.
3. Remove unused code after evidence and tests.
4. Add parity + dependency loading tests.
5. Update docs and migration notes.

## Open Questions
- Are there any unavoidable backend-specific parameters that must remain optional in the shared API?
- Should dependency checks occur in __init__ or per-method call for each handler?
