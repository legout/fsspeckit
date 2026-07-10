## 1. Spec and Guidelines

- [ ] 1.1 Add cross-cutting error-handling requirements to `openspec/specs/project-architecture/spec.md`:
  - [ ] 1.1.1 Define a requirement for non-silencing error handling in core code paths.
  - [ ] 1.1.2 Define a requirement to route error reporting through the logging infrastructure instead of `print()`.
- [ ] 1.2 Add a short section to `openspec/project.md` (or contributor docs) that:
  - [ ] 1.2.1 Describes preferred error-handling patterns (no bare `except Exception`, log-and-rethrow for unexpected errors, etc.).
  - [ ] 1.2.2 Explains expectations around using `fsspeckit` logging utilities instead of `print()`.

## 2. Sub-Proposals

- [ ] 2.1 Ensure there are dedicated change proposals that implement these patterns for major modules:
  - [ ] 2.1.1 `fix-duckdb-error-handling` for DuckDB dataset helpers and cleanup helpers.
  - [ ] 2.1.2 `fix-pyarrow-error-handling` for PyArrow datasets and utilities.
  - [ ] 2.1.3 `fix-core-io-error-handling` for core filesystem and I/O helpers.
  - [ ] 2.1.4 `fix-common-modules-error-handling` for shared common utilities.
  - [ ] 2.1.5 `fix-maintenance-error-handling` for backend-neutral maintenance workflows.
  - [ ] 2.1.6 `fix-storage-options-error-handling` for storage option classes.
- [ ] 2.2 For each sub-proposal:
  - [ ] 2.2.1 Provide OpenSpec-compliant deltas that reference the relevant capability.
  - [ ] 2.2.2 Ensure tasks and tests in each proposal align with the global requirements from this change.

## 3. Validation

- [ ] 3.1 Run `openspec validate standardize-error-handling-logging --strict` and resolve any issues.
- [ ] 3.2 Run `openspec validate` for each sub-proposal once their deltas are updated, ensuring they remain compatible with this umbrella change.
