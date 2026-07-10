# Migration and Historical-Content Boundaries for fsspeckit 0.22.x

## Purpose

This research asset resolves **Define actionable migration and
historical-content boundaries**. It sets the user-facing boundary for the
refactor-aware documentation refresh; it does not rewrite the pages.

## Decision

The supported migration source is **the immediately preceding publicly
supported pre-0.22 package layout**. The active documentation targets 0.22.x
and later. A migration page is active only when it tells that reader how to
reach a supported 0.22.x workflow using canonical imports and current
class-based handlers.

The active Migration lane stays below Explanation, discoverable from Home and
Architecture, rather than becoming a top-level documentation journey. It
contains exactly these workflow pages:

### Dataset Module Refactor Migration Notes

**Disposition:** retain and rewrite as the primary active migration.

Upgrade removed helper and handler APIs to `PyarrowDatasetIO` or
`DuckDBDatasetIO`; make real backend differences explicit; link current
signatures to Reference. Do not claim signature parity, DuckDB `partition_by`
support, or optimization parity unless the public-reference contract verifies
those claims.

### Migrate to the New Package Layout

**Disposition:** move from `docs/how-to/migrate-package-layout.md` to
`docs/migration/migrate-package-layout.md`, then redirect the old URL.

Map only the supported pre-0.22 imports to current canonical imports. State the
version boundary. Remove claims that compatibility imports work indefinitely or
are preferred. Route every renamed symbol or old import to the single Reference
deprecation page.

This is a bounded workflow migration, not an all-version compatibility promise.
Active tutorials, how-tos, and migration pages use only canonical imports.
Legacy names and paths appear only in **Reference: Deprecation & Legacy
Imports**.

## Redirect and removal policy

A published route is never retained as a stale content stub. When a successor
serves the same user intent, implementation supplies a permanent redirect and
removes the old source page.

### `how-to/migrate-package-layout/`

**Disposition:** redirect to `migration/migrate-package-layout/` after the
page moves into the Migration lane.

### `migration/append-mode-default/`

**Disposition:** redirect and remove the source page.

The page teaches the nonexistent `write_parquet_dataset` API. Direct users to
the primary dataset-module-refactor migration, whose rewritten content will
link to the current `write_dataset` reference.

### `migration/enhanced-memory-monitoring/`

**Disposition:** redirect and remove the source page.

It is a release-specific 0.8.3 migration, not a 0.22.x package-layout upgrade.
Direct users seeking supported memory guidance to
`how-to/memory-constrained-environments/`; that guide and the reference
contract own current memory APIs.

No current page may redirect to an unrelated homepage merely to avoid a 404. If
a legacy path has no intent-preserving successor, remove it and let repository
history remain the record. This decision identifies successors for all three
existing migration routes.

## Historical ADR and plan policy

ADRs and plans are retained as engineering history, not library-user
instructions:

1. Keep their stable source paths in `docs/adr/`, `docs/architecture/`, and
   `docs/plans/`; do not duplicate their bodies in user documentation.
2. Remove `docs/architecture/0001-layering-rules.md` from active navigation.
   Its claims about the old layout and CI enforcement must carry a brief
   **Historical** status note and link to current Architecture for the
   supported model.
3. Add one clearly labeled **Historical decisions and plans** handoff at the
   end of Explanation: Architecture. It may index selected ADRs and plans and
   explain that they are context, not current API or workflow guidance. It is
   not a top-level tab, tutorial handoff, how-to route, or Reference content.
4. Each published historical page gets a short status preamble when its content
   can be mistaken for current behavior. The preamble points to current
   Architecture, Reference, or active migration content rather than repeating
   API facts.
5. Repair or retire dangling historical links, notably references to ADR-0002,
   during implementation. A missing historical target must not be silently
   represented as current guidance.

## Evidence

- `pyproject.toml` identifies the current release as 0.22.0.
- `docs/migration/dataset-module-refactor.md` has the correct user intent but
  overclaims parity and DuckDB `partition_by` behavior. The documentation-drift
  audit records those conflicts.
- `docs/how-to/migrate-package-layout.md` uses removed
  `datasets.pyarrow.schema` and `common.schema` paths, treats compatibility
  aliases as primary, and promises indefinite legacy support.
  `src/fsspeckit/datasets/__init__.py` instead exposes deprecated aliases
  through a warning-producing compatibility boundary.
- The supported handler surface is `write_dataset` and `merge` on
  `PyarrowDatasetIO` and `DuckDBDatasetIO`; the append-mode migration's
  `write_parquet_dataset` calls do not exist in the current package.
- `MemoryMonitor` and `MemoryPressureLevel` still exist in
  `src/fsspeckit/datasets/pyarrow/memory.py`, but the migration page explicitly
  targets 0.8.3. `docs/how-to/memory-constrained-environments.md` is the
  appropriate current-user destination.
- The agreed information architecture places Migration under Explanation,
  quarantines legacy imports in Reference, and makes historical ADRs and plans
  a labeled Architecture handoff.

## Implementation handoff

The OpenSpec-boundary work should specify the redirect mechanism, the two
rewritten active migration pages, the legacy-import reference page, the
Architecture history handoff, and historical-status and link repairs. The
public-reference work owns verification of every canonical import and API claim
used by those pages.
