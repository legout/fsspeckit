## 1. Extend project-docs Specification

- [ ] 1.1 Add a `Security and Error Handling Documentation` requirement to `project-docs` that:
  - References `fsspeckit.common.security` helpers and their intended use.
  - Requires at least one dedicated section in the docs that demonstrates safe usage patterns.
- [ ] 1.2 Add a `Versioned Migration Guides` requirement that:
  - Requires a migration doc for significant refactors (starting with the 0.5.x architecture/logging/security changes).
  - Ensures the migration doc is linked from the homepage and architecture page.
- [ ] 1.3 Add a `Documentation Layering and Roles` requirement that:
  - Defines the responsibilities of quickstart/tutorial, how-to/API guide, reference/API, and architecture docs.
  - Explicitly discourages duplicating detailed API behavior in narrative guides when it is already covered by generated reference.

## 2. Implement Security and Error Handling Docs

- [ ] 2.1 Add or expand a “Security and Error Handling” section in `docs/advanced.md` (or a dedicated page) that:
  - Explains path validation, codec validation, credential scrubbing, and safe error formatting.
  - Shows short, realistic examples of using these helpers with DuckDB/PyArrow dataset operations and storage options.
- [ ] 2.2 Ensure architecture docs (`docs/architecture.md`) reference these helpers as part of the production deployment story (monitoring, observability, security).

## 3. Implement Migration Guide

- [ ] 3.1 Create `docs/migration-0.5.md` (or equivalent) documenting:
  - Migration from `fsspec-utils` to `fsspeckit`.
  - Migration from `fsspeckit.utils` imports to domain packages.
  - Behavioral changes in logging, error handling, dataset maintenance, and optional dependencies.
- [ ] 3.2 Link the migration guide from `README.md`, `docs/index.md`, and `docs/architecture.md` so that it is easy to discover.

## 4. Clarify Documentation Roles and Reduce Duplication

- [ ] 4.1 Review `docs/quickstart.md`, `docs/api-guide.md`, `docs/advanced.md`, and `docs/examples.md` to ensure each plays its intended role (tutorial, how-to, advanced patterns, example catalogue) and that overlapping sections point to a single canonical example instead of duplicating code.
- [ ] 4.2 Remove or consolidate duplicate examples where they cause drift, and rely on cross-linking to canonical examples instead.

## 5. Validation and Guardrails

- [ ] 5.1 Run `openspec validate --strict` to confirm the updated `project-docs` spec parses correctly and that requirements and scenarios follow the expected format.
- [ ] 5.2 Add or update lightweight CI checks (for example simple `rg`-based scripts) to catch obvious documentation drift markers called out in `project-docs` (for example references to non-existent helpers).
- [ ] 5.3 Run `uv run mkdocs build` to ensure the docs compile successfully after these changes.

