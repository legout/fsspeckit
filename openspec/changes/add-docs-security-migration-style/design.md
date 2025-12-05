## Context

The `project-docs` spec currently ensures that documentation:
- Mirrors implemented capabilities.
- Prioritises domain packages over `fsspeckit.utils`.
- Uses accurate imports and path-safety guidance.

However, several cross-cutting areas are under-specified:

- **Security and error handling:** The codebase now includes `fsspeckit.common.security` helpers (path validation, codec validation, credential scrubbing, safe error formatting), but the docs do not surface them clearly.
- **Versioned migration:** Major refactors (e.g. domain packages, error-handling unification, logging changes) lack an explicit migration story, so users must infer upgrade steps from scattered notes.
- **Documentation layering:** Quickstart, API guide, advanced docs, and architecture pages sometimes overlap in purpose, which encourages duplication and increases the risk of drift.

This change extends `project-docs` to close those gaps and give future doc work clearer guardrails.

## Design Goals

- **Make security visible:** Users should be able to discover and understand the built-in safety measures (path validation, codec validation, credential scrubbing) from the docs, not just from reading code.
- **Support safe upgrades:** For significant refactors, there should be a first-class migration guide that explains what changed and how to update existing codebases.
- **Clarify doc roles:** Each doc type (quickstart, how-to/API guide, reference, architecture) should have a defined purpose, so new content lands in the right place and avoids redundancy.
- **Remain flexible:** The spec should be precise enough to prevent drift but not so rigid that small doc reorgs or future refactors become difficult.

## Requirements Shape

The spec delta introduces three grouped requirements under `project-docs`:

1. **Security and Error Handling Documentation**
   - Docs must describe:
     - `validate_path`
     - `validate_compression_codec`
     - `scrub_credentials`, `scrub_exception`, and `safe_format_error`
   - At least one dedicated section (for example in `docs/advanced.md` or a separate security page) will show realistic examples of using these helpers with dataset operations and storage options.
   - Architecture docs reference these helpers in the context of production deployment, security, and operations.

2. **Versioned Migration Guides**
   - Significant refactors (starting with the 0.5.x domain-package/logging/security refactor) require a migration guide (for example `docs/migration-0.5.md`).
   - The guide explains:
     - Import changes (`fsspec-utils` → `fsspeckit`; `fsspeckit.utils` → domain packages).
     - Logging and error-handling changes.
     - Dataset maintenance and optional dependency changes.
   - The docs homepage and architecture overview link to relevant migration guides so users can find them easily.

3. **Documentation Layering and Roles**
   - Quickstart:
     - Focus on a concise, end-to-end “first project” experience.
     - Avoid exhaustive parameter lists or edge-case behaviours.
   - API guide / how-to:
     - Focus on task-oriented workflows (e.g. “Sync directories between clouds”, “Translate SQL filters”).
     - Delegate detailed parameter/return semantics to the API reference.
   - Reference/API:
     - Prefer mkdocstrings-generated content over hand-written parameter descriptions where feasible.
     - Serve as the authoritative source for signatures and detailed behaviour.
   - Architecture:
     - Focus on concepts, design decisions, and integration patterns.
     - Avoid re-defining API signatures or introducing behaviours that differ from the reference.

## Documentation Design

### Security and Error Handling

- **Placement:**
  - Add a “Security and Error Handling” section under `docs/advanced.md` or a dedicated `docs/security.md`, depending on where the existing structure best accommodates it.
  - Summarise the topic in `docs/architecture.md` and link to the detailed section.
- **Content:**
  - Short, realistic examples:
    - Validating paths/columns before dataset operations.
    - Validating compression codecs before passing them to DuckDB/PyArrow helpers.
    - Formatting and logging errors with credentials scrubbed.

### Migration Guide

- **Placement:**
  - Create `docs/migration-0.5.md` for the 0.5.x refactor and leave room for future guides (e.g. `migration-0.6.md`) without changing the spec.
- **Content:**
  - Import mapping tables (old → new).
  - Side-by-side code snippets showing pre- vs post-refactor patterns.
  - Notes on behavioural changes (e.g. stricter path validation, logging defaults).

### Layering and Cross-linking

- **Quickstart:**
  - Keep it short and focused; link out to detailed guides for filesystem, storage options, dataset operations, and security.
- **API Guide:**
  - Provide “recipes” like “Set up multi-cloud storage”, “Maintain Parquet datasets”, “Translate SQL filters for PyArrow/Polars”.
  - Link into specific API reference pages for signatures.
- **Architecture:**
  - Emphasise domain boundaries, backend-neutral planning layers, security model, and deployment considerations.
  - Avoid embedding full function signatures or made-up pseudo-APIs.

## Risks and Mitigations

- **Risk:** Over-constraining documentation could make future reorganisations harder.
  - **Mitigation:** Requirements are framed in terms of *roles* and *presence* (e.g. “there is a migration guide and it is discoverable”) rather than dictating exact filenames or headings.

- **Risk:** Security docs become stale as new helpers are added.
  - **Mitigation:** The requirement focuses on documenting the current helpers and patterns; future security-related changes can extend the examples and link to any new helpers.

- **Risk:** Migration guides could drift from actual behaviour if not maintained.
  - **Mitigation:** By making migration guidance a first-class spec requirement and linking it from visible entry points, it becomes part of the normal review surface for future changes.

