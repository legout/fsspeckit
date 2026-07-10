## 1. Documentation updates

- [x] 1.1 Replace `fsspec-utils` with `fsspeckit` in markdown docs
  (`docs/installation.md`, `docs/contributing.md`, and related API docs).
- [x] 1.2 Update example READMEs and Python example headers under `examples/`
  to use the `fsspeckit` name and current install instructions.
- [x] 1.3 Adjust core docstrings in `src/fsspeckit/__init__.py`,
  `src/fsspeckit/core/__init__.py`, and `src/fsspeckit/utils/*` that mention
  `fsspec-utils` so that generated API docs are consistent.
- [x] 1.4 Skim docs for any remaining `fsspec-utils` mentions and clean them
  up where appropriate.

## 2. Validation

- [x] 2.1 Run `openspec validate update-docs-fsspeckit-branding --strict` and
  ensure it passes.
- [x] 2.2 Optionally run `uv run mkdocs build` (or `mkdocs build`) to verify
  the documentation site builds successfully after changes.

