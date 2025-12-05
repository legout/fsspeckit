## Why

- The current documentation specification (`project-docs`) enforces accuracy, domain-package prioritization, and utils façade messaging, but it does not explicitly require:
  - User-facing documentation of the new security helpers (`validate_path`, `validate_compression_codec`, `scrub_credentials`, `scrub_exception`, `safe_format_error`, `validate_columns`).
  - Versioned migration guides that explain how to move between major refactors (for example the 0.5.x domain package and error-handling/logging changes).
  - Clear separation of concerns between tutorials, how-to guides, reference, and architecture docs to reduce duplication and drift.
- As a result, security- and operations-critical behavior lives only in code and tests, and readers must infer migration steps and documentation structure from scattered hints.

## What Changes

- Extend the `project-docs` specification with three new requirement areas:
  1. **Security and Error Handling Documentation**
     - Require that the docs describe how fsspeckit protects against common issues (path traversal, unsafe codecs, credential leakage in logs).
     - Require at least one dedicated section (for example “Security and Error Handling”) that introduces the helpers in `fsspeckit.common.security` and shows when to use them in custom integrations.
  2. **Versioned Migration Guides**
     - Require migration guides for significant refactors (for example from pre–domain package layouts to the current architecture).
     - Ensure these guides are discoverable from the homepage and architecture docs.
  3. **Documentation Layering and Roles**
     - Codify that:
       - Quickstart/tutorial docs focus on end-to-end “getting started”.
       - API guide/how-to docs focus on task-oriented patterns.
       - Reference/API pages are generated from code where possible and do not introduce novel behaviors.
       - Architecture docs focus on design rationale and high-level patterns, avoiding API details that are better expressed in reference.
- Implement these requirements by:
  - Adding or updating pages such as `docs/advanced.md`, `docs/architecture.md`, and a new `docs/migration-0.5.md` (or similar), and by tightening the content scope of existing guides.

## Impact

- Affected specs:
  - `project-docs` (new requirements defining security/migration/style guardrails).
- Affected documentation:
  - `docs/advanced.md` (add concrete “Security and Error Handling” section with examples).
  - `docs/architecture.md` (reference the security helpers and migration story at a high level).
  - `docs/index.md` and/or `README.md` (link to the migration guide).
  - New file: `docs/migration-0.5.md` (or similar name documenting the 0.5.x refactors).
- No runtime behavior changes; this is a documentation and specification-only change that clarifies expectations for future doc updates.

