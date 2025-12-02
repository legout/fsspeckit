## Why

The packaging configuration and version handling have a few issues that make development and installation more fragile than necessary:

- The `pyproject.toml` optional dependency sections are malformed, using additional tables (`[polars]`, `[datasets]`, `[sql]`) instead of entries under `[project.optional-dependencies]`.
- The `__version__` attribute in `fsspeckit.__init__` is obtained via `importlib.metadata.version("fsspeckit")`, which fails when the package is not installed (e.g. local development or editable installs).

These issues can break installation of extras (like `fsspeckit[datasets]`) and make development workflows brittle.

## What Changes

- Correct the layout of optional dependencies in `pyproject.toml` so that all extras (e.g. `aws`, `gcp`, `azure`, `polars`, `datasets`, `sql`) are declared under `[project.optional-dependencies]` with valid TOML syntax.
- Update version handling in `src/fsspeckit/__init__.py` to:
  - Try to obtain the installed version via `importlib.metadata.version("fsspeckit")`.
  - Fall back to a safe default (e.g. `"0.0.0"` or a marker string) when the distribution metadata is not available, so local development imports still work.

## Impact

- **Installation:**  
  - Extras like `fsspeckit[datasets]` and `fsspeckit[sql]` will install the intended dependency sets consistently.

- **Development:**  
  - Importing `fsspeckit` in an editable/local checkout will no longer fail purely due to missing installed metadata for the version.

- **Specs affected:**  
  - `project-architecture` (project-level layout, packaging, and deployment conventions).

- **Files affected:**
  - `pyproject.toml`
  - `src/fsspeckit/__init__.py`

