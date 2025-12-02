## 1. Planning

- [ ] 1.1 For each large module (`core.ext`, `datasets.pyarrow`, `datasets.duckdb`, `core.filesystem`), sketch a target submodule layout that:
  - [ ] 1.1.1 Groups related functions and classes by concern (e.g., IO helpers vs schema utilities vs connection management).
  - [ ] 1.1.2 Identifies the minimal “public entrypoint” surface that needs to be preserved.

## 2. Implementation

- [ ] 2.1 `core.ext` decomposition:
  - [ ] 2.1.1 Introduce submodules such as `core.ext_json`, `core.ext_csv`, `core.ext_parquet` (names to be finalised in design).
  - [ ] 2.1.2 Move format-specific helpers into these submodules.
  - [ ] 2.1.3 Keep a thin `core.ext` (or a dedicated registration module) that attaches the helpers to `AbstractFileSystem` and maintains existing import paths.

- [ ] 2.2 `datasets.pyarrow` decomposition:
  - [ ] 2.2.1 Extract schema/type inference and unification logic into a dedicated submodule (e.g., `datasets.pyarrow_schema`).
  - [ ] 2.2.2 Keep merge/maintenance helpers in a focused module that delegates to `core.merge` and `core.maintenance`.
  - [ ] 2.2.3 Add appropriate re-exports to keep `fsspeckit.datasets.pyarrow` public surface stable.

- [ ] 2.3 `datasets.duckdb` decomposition:
  - [ ] 2.3.1 Factor out DuckDB connection/registration and filesystem bridging into a dedicated helper module.
  - [ ] 2.3.2 Keep dataset IO and maintenance helpers in a focused module, reusing the shared core merge/maintenance logic.
  - [ ] 2.3.3 Maintain the `DuckDBParquetHandler` API and other public entrypoints via imports/re-exports.

- [ ] 2.4 `core.filesystem` decomposition:
  - [ ] 2.4.1 Extract path-normalisation, protocol parsing, and cache-mapper helpers into smaller modules (or a dedicated internal module).
  - [ ] 2.4.2 Keep the high-level `filesystem`/`get_filesystem` factory logic in a clearer, smaller core module.
  - [ ] 2.4.3 Ensure existing public imports from `fsspeckit.core` and `fsspeckit.__init__` remain valid.

## 3. Testing

- [ ] 3.1 Add tests to assert that:
  - [ ] 3.1.1 Public imports from `fsspeckit`, `fsspeckit.core`, and `fsspeckit.datasets` continue to work unchanged.
  - [ ] 3.1.2 The refactored modules can be imported individually without pulling in unrelated optional dependencies.
- [ ] 3.2 Run the full test suite after each decomposition step to detect any behavioural regressions.

## 4. Documentation

- [ ] 4.1 Update architecture documentation (including `openspec/project.md` and any relevant docs pages) to:
  - [ ] 4.1.1 Reflect the new internal submodule structure.
  - [ ] 4.1.2 Clarify where new functionality in each domain should be added.

