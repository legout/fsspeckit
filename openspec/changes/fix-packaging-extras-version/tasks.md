## 1. Implementation

- [ ] 1.1 Restructure `[project.optional-dependencies]` in `pyproject.toml` so that:
  - [ ] 1.1.1 All extras are declared as keys under the same table (e.g. `aws`, `gcp`, `azure`, `polars`, `datasets`, `sql`).
  - [ ] 1.1.2 The extras names match those referenced in error messages and documentation (for example, `fsspeckit[datasets]`).
- [ ] 1.2 Add a robust version initialisation in `src/fsspeckit/__init__.py`:
  - [ ] 1.2.1 Attempt to read `importlib.metadata.version("fsspeckit")`.
  - [ ] 1.2.2 On `PackageNotFoundError` (or similar), assign a safe default version.

## 2. Testing

- [ ] 2.1 Verify `pip install .[datasets]` and `. [sql]` work in a clean environment and pull the expected libraries.
- [ ] 2.2 Add a lightweight test that imports `fsspeckit` from a source checkout without installation and observes a non-crashing version initialisation behaviour (for example, by mocking `importlib.metadata.version` to raise).

## 3. Documentation

- [ ] 3.1 Ensure README and other docs use extras names that match the corrected `pyproject.toml`.
- [ ] 3.2 Optionally note in contributor docs how the version is determined in development vs installed environments.

