## 1. Type-checking

- [ ] 1.1 Add a baseline type-checking configuration (e.g. `mypy.ini` or equivalent tool config) that:
  - [ ] 1.1.1 Targets the `src/fsspeckit` package.
  - [ ] 1.1.2 Enables `--disallow-untyped-defs` (or similar) for selected key modules and gradually expands coverage.
- [ ] 1.2 Mark the package as typed (e.g. via a `py.typed` marker file) once baseline coverage is in place.
- [ ] 1.3 Update CI to run the type-checking step as part of the standard pipeline.

## 2. Annotation improvements

- [ ] 2.1 Identify key modules for early annotation improvements (e.g. newly split submodules from `core.ext`, `datasets.pyarrow`, `datasets.duckdb`).
- [ ] 2.2 Add or refine type hints for public and internal helper functions in those modules, focusing on:
  - [ ] 2.2.1 Clear parameter and return types.
  - [ ] 2.2.2 Avoiding overly broad unions where a simpler contract is possible.

## 3. Testing discipline

- [ ] 3.1 Define a minimal testing expectation for refactors:
  - [ ] 3.1.1 New submodules should have at least a small set of direct unit or integration tests.
  - [ ] 3.1.2 Behaviour previously covered implicitly by integration tests should be preserved via a mix of integration and unit tests post-refactor.
- [ ] 3.2 Update contributor documentation to describe:
  - [ ] 3.2.1 The expectation that new code comes with corresponding tests.
  - [ ] 3.2.2 The expectation that high-risk changes (e.g. touching core IO or datasets modules) trigger type-checking and updated tests.

