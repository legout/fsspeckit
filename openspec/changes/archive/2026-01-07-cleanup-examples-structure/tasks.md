## 1. Inventory and Removal Decisions

- [x] 1.1 Confirm the final "kept" example set (directories + scripts) and document the decision in this change
- [x] 1.2 Delete the obsolete example directories listed in the proposal (or explicitly migrate specific files to `docs/` if needed)
- [x] 1.3 Remove/update references to deleted examples from `examples/README.md` and any per-folder READMEs

## 2. Dependency Guidance

- [x] 2.1 Replace `examples/requirements.txt` with a minimal, examples-focused dependency list (or update it to align with `pyproject.toml` extras)
- [x] 2.2 Ensure example install instructions do not require network credentials or running external services by default

## 3. Validation

- [x] 3.1 Ensure `find examples -name '*.py'` contains only the kept example scripts
- [x] 3.2 Run the example runner (after it is updated in a separate change) against the remaining examples and confirm the suite is coherent

