## Context

The project has evolved from a flatter module layout toward more domain-driven structure. Many modules are now logically
grouped (core ext helpers, filesystem helpers, DuckDB/PyArrow datasets, logging), but the physical layout still uses
flat, underscored filenames. This change moves those domains into packages while preserving backwards compatibility.

## Goals / Non-Goals

- Goals:
  - Introduce package-based layouts (`ext/`, `filesystem/`, `datasets/duckdb/`, `datasets/pyarrow/`, `common/logging/`).
  - Keep existing import paths working via shims and deprecation warnings.
  - Update internal imports and docs to prefer the new layout.
- Non-Goals:
  - Changing existing behaviour or semantics of ext/filesystem/datasets/logging helpers.
  - Removing legacy modules immediately; deprecation will be gradual.

## Decisions

- Use shim modules rather than hard renames:
  - Each old module imports from the new package location and re-exports public names.
  - Optionally emit `DeprecationWarning` at import time or on attribute access.
- Internals move to the new paths as part of this change; the shims exist for external consumers and tests.
- Avoid over-nesting for now: keep packages shallow and focused (e.g. `filesystem/paths.py`, not deeply nested subtrees).

## Risks / Trade-offs

- Touch-heavy change: many files and imports are updated, which can conflict with parallel work if not sequenced carefully.
- Shims add a small maintenance burden but are critical for non-breaking migration.

## Migration Plan

1. Create new packages and move implementation code.
2. Add shims for old modules, with clear deprecation warnings.
3. Update internal imports and docs to prefer the new layout.
4. After at least one stable release, consider archiving/removing shims in a separate breaking-change proposal.

## Open Questions

- Exact deprecation policy (timeline and version) can be decided when preparing release notes; this change will only
  introduce the shims and warnings.

