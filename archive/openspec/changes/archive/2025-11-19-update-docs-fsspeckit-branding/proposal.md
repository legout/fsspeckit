## Why

After rebranding the library from `fsspec-utils` to `fsspeckit` and refactoring
internal modules, parts of the documentation (installation, contributing guide,
examples, and API docs) still use the old package name and outdated install
instructions. This creates confusion for users and makes the docs inconsistent
with the published package on PyPI.

## What Changes

- Replace remaining `fsspec-utils` references in public markdown docs and
  example READMEs with `fsspeckit`, including install commands and
  troubleshooting notes.
- Align installation instructions in `docs/installation.md` with the extras
  defined in `pyproject.toml` (`aws`, `gcp`, `azure`) and the current minimum
  Python version.
- Update API and logging docs so that descriptions refer to the `fsspeckit`
  package and top-level imports, not the old namespace.
- Refresh example scripts' top-level comments and docstrings to use
  `fsspeckit` naming where they are intended as user-facing documentation.

## Impact

- Affected specs: `project-docs` (new capability for user-facing documentation
  consistency).
- Affected code:
  - Markdown docs under `docs/` (especially `installation.md`,
    `contributing.md`, and logging API pages).
  - Example scripts and READMEs under `examples/`.
  - Selected docstrings in `src/fsspeckit/__init__.py`,
    `src/fsspeckit/core/__init__.py`, and `src/fsspeckit/utils/*` that surface
    in API docs.
- No behavioral or API changes; this change is documentation-only but should
  be validated via `mkdocs build` and `openspec validate`.

