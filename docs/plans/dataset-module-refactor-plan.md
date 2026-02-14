# Dataset Module Simplification and Refactor Plan (2026)

## Objective
Simplify and refactor the dataset module to achieve **feature parity** and **near-identical syntax** between the PyArrow and DuckDB backends while keeping the module fully functional, reducing duplication, and removing unused code.

## Scope
In-scope modules and key references:
- `/Users/volker/coding/libs/fsspeckit/src/fsspeckit/datasets/__init__.py`
- `/Users/volker/coding/libs/fsspeckit/src/fsspeckit/datasets/base.py`
- `/Users/volker/coding/libs/fsspeckit/src/fsspeckit/datasets/interfaces.py`
- `/Users/volker/coding/libs/fsspeckit/src/fsspeckit/datasets/duckdb/*.py`
- `/Users/volker/coding/libs/fsspeckit/src/fsspeckit/datasets/pyarrow/*.py`
- `/Users/volker/coding/libs/fsspeckit/src/fsspeckit/core/*`
- `/Users/volker/coding/libs/fsspeckit/docs/dataset-handlers.md`
- `/Users/volker/coding/libs/fsspeckit/openspec/specs/datasets-conditional-loading/spec.md`
- `/Users/volker/coding/libs/fsspeckit/openspec/specs/datasets-parquet-io/spec.md`

Out of scope unless required for parity or correctness:
- Non-dataset modules unrelated to dataset I/O and merge operations
- New storage backends or new file formats

## Definition of Feature Parity
Parity means **the same public API surface and semantics** for PyArrow and DuckDB, with only clearly documented, unavoidable backend differences.

Minimum parity set:
- `write_dataset` with identical parameters and defaults
- `merge` with identical parameters and defaults (including strategy, key/partition columns)
- `compact_parquet_dataset` and `optimize_parquet_dataset` with identical parameters
- Consistent return types (same dataclasses or dict schema)
- Consistent error types and messages for validation failures
- Consistent conditional import behavior and dependency error messaging

## 2026 Data Engineering Best Practices to Enforce
- **Schema contracts**: explicit schema support, evolution policies, and schema validation hooks
- **Idempotent writes**: safe re-runs without unexpected duplication when mode/strategy allows
- **Atomicity**: staging + atomic replace for file-level updates
- **Partition invariants**: enforce partition columns immutability for existing keys
- **Observability**: structured stats/metrics from writes/merges/compactions
- **Determinism**: deterministic file naming/ordering for repeatable outputs when possible
- **Performance safety**: avoid full dataset materialization for metadata-only operations
- **Optional dependencies**: dependency validation prior to executing backend-specific logic

## Rules
- Follow OpenSpec process before implementing changes.
- Preserve Python 3.11+ typing rules and optional dependency import patterns.
- Do not add new dependencies unless necessary for parity or correctness.
- Keep all public APIs fully typed and documented.
- Avoid backend-specific behavior changes unless explicitly documented and tested.
- Prefer shared core logic in `core/` or shared helpers over duplicated backend code.

## Dos and Don’ts
Dos:
- Do create a single, canonical API surface for dataset operations.
- Do normalize parameter names and defaults across backends.
- Do centralize validation, normalization, and error handling.
- Do remove dead code only after proving it is unused (static search + tests).
- Do add contract tests that enforce parity.

Don’ts:
- Don’t introduce new public APIs without updating docs and specs.
- Don’t break existing imports without deprecation and migration notes.
- Don’t keep parallel implementations of the same logic unless justified.
- Don’t let backend-specific code leak into the shared API surface.

## Plan

### Phase 0: OpenSpec and Tasking
1. Review existing OpenSpec specs and active changes.
2. Create a new OpenSpec change for this refactor (verb-led change id).
3. Create `proposal.md`, `tasks.md`, and `design.md` (design is required due to cross-cutting changes).
4. Add spec deltas for datasets parity, API surface, and dependency loading.

Acceptance criteria:
- `openspec/changes/<change-id>/proposal.md` exists and is complete.
- `openspec/changes/<change-id>/tasks.md` contains 3–8 deliverable tasks.
- `openspec/changes/<change-id>/design.md` exists with decisions and risks.
- `openspec validate <change-id> --strict` passes.

### Phase 1: Baseline Inventory and Parity Matrix
1. Enumerate all public dataset APIs and their signatures for PyArrow and DuckDB.
2. Document current behavior differences (semantics, defaults, return values, error types).
3. Create a parity matrix (API x backend) with current coverage and gaps.
4. Identify unused code (modules, classes, functions) using:
   - `rg` for imports/usage
   - ruff unused checks
   - tests/examples/docs references

Acceptance criteria:
- A parity matrix exists in the plan or change docs.
- Each API has explicit signature + default alignment targets.
- A list of candidate unused code is produced with evidence.

### Phase 2: API Surface Unification
1. Choose a single canonical API style (class-based) and preserve any function wrappers only as thin delegates.
2. Standardize API names, parameters, and defaults across backends based on `DatasetHandler`.
3. Define shared dataclasses for results if not already unified (`WriteDatasetResult`, `MergeResult`).
4. Normalize error types and messages through shared validation utilities.
5. Ensure deprecation mappings in `/Users/volker/coding/libs/fsspeckit/src/fsspeckit/datasets/__init__.py` are correct and return the intended objects.

Acceptance criteria:
- `PyarrowDatasetIO` and `DuckDBDatasetIO` expose identical method signatures.
- Function-based helpers are thin wrappers with no duplicated logic.
- Deprecation imports resolve to correct classes (not modules).
- Documentation reflects the unified API.

### Phase 3: Shared Core Logic Extraction
1. Move shared validation and normalization (paths, keys, partition columns, schema) into `datasets/base.py` or `core/`.
2. Centralize file planning and metadata operations where possible (already in `core/*`).
3. Remove backend-specific copies of shared logic and replace with calls to shared functions.
4. Define a single shared “merge planning” pipeline that both backends consume.

Acceptance criteria:
- Redundant logic is removed from backend modules.
- Shared functions are used by both backends.
- Behavior remains consistent with pre-refactor behavior (backed by tests).

### Phase 4: Remove Unused Code
1. Confirm unused candidates with static search and test usage.
2. Remove dead modules/classes/functions.
3. Update docs and `__all__` exports to reflect removal.

Acceptance criteria:
- No dead or unreachable code remains in dataset modules.
- `ruff` unused checks are clean for dataset modules.
- All removals are reflected in docs and public exports.

### Phase 5: Parity Tests and Regression Coverage
1. Add contract tests that run the same operations against PyArrow and DuckDB and compare:
   - error types
   - default parameter behavior
   - result schema/structure
2. Add tests for conditional dependency loading and error messaging.
3. Add tests for deterministic behavior where expected (file counts, stats).

Acceptance criteria:
- Parity tests exist and pass on CI.
- Contract tests cover write/merge/compact/optimize.
- Dependency loading tests satisfy `datasets-conditional-loading` spec.

### Phase 6: Documentation and Migration
1. Update `/Users/volker/coding/libs/fsspeckit/docs/dataset-handlers.md` with the unified API.
2. Update API guide and any usage examples to match parity syntax.
3. Add migration notes for removed or renamed APIs.

Acceptance criteria:
- Docs and examples match the unified API.
- Deprecated APIs are documented with migration paths.
- `mkdocs build` succeeds.

### Phase 7: Final Validation
1. Run full tests, type checks, and linting on dataset modules.
2. Verify that both backends are fully functional for core operations.
3. Confirm no behavior regressions for existing public interfaces.

Acceptance criteria:
- `pytest` passes.
- `ruff` and type checks pass.
- Public APIs and docs are consistent and correct.

## Deliverables
- OpenSpec change proposal with tasks and design
- Parity matrix and gap analysis
- Unified dataset API
- Shared core helpers
- Removed unused code
- Parity and regression tests
- Updated documentation and migration guidance

## Risks and Mitigations
- Risk: Hidden backend-specific behavior regressions.
  Mitigation: Parity contract tests and golden tests from current behavior.

- Risk: Over-aggressive removal of code used implicitly.
  Mitigation: Require evidence of non-use and run full test suite before removal.

- Risk: Optional dependency import errors.
  Mitigation: Centralize dependency checks and add explicit tests.

## Acceptance Criteria Summary (Must All Be True)
- PyArrow and DuckDB dataset APIs share the same public surface and defaults.
- Core operations behave consistently across backends.
- No unused code remains in dataset modules.
- Tests and documentation are updated and passing.
- OpenSpec change is validated and approved before implementation.
