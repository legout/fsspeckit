## Why

While many functions and modules in `fsspeckit` are already annotated and tested, several reviews highlight:

- Gaps in type annotations, especially for internal helpers.
- Large, complex functions where types could help clarify intent.
- The absence of a clear policy around type-checking new or changed code.
- The need to treat the test suite as a first-class specification and to ensure new structural changes remain well covered.

Strengthening type and test discipline will reduce regressions as the codebase evolves, particularly as larger structural refactors land.

## What Changes

- Introduce a baseline type-checking configuration (e.g. mypy or a similar tool) that:
  - Runs in CI.
  - Enforces at least “no untyped defs” for new/changed code in key modules.
- Incrementally improve type coverage in:
  - Core modules (`core.filesystem`, `core.ext` and their new submodules).
  - Dataset modules (`datasets.pyarrow`, `datasets.duckdb` and their new submodules).
- Strengthen the testing discipline by:
  - Ensuring that new submodules introduced in refactors have targeted tests.
  - Documenting expectations for test coverage for new features and refactors.

## Impact

- **Behaviour:**  
  - No direct runtime behavioural changes; this is a tooling and quality effort.

- **Quality:**  
  - Better static feedback during development.
  - Greater confidence in refactors and cross-cutting changes.

- **Specs affected:**
  - `project-architecture` (project-wide quality and tooling expectations).

