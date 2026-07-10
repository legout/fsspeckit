# Change: Refactor documentation into a role-based structure

## Why

- The current documentation surface exposes many top-level pages (`Quickstart`, `Advanced Usage`, `Examples`, `API Guide`, `Utils Reference`, `Architecture`, `Migration`, `Installation`), which makes it hard for users to know where to start and where to look for a specific kind of information.
- Several pages mix roles (tutorial, how-to, reference, explanation) in a single document; for example `quickstart.md` includes installation, end-to-end onboarding, and advanced configuration patterns, while `api-guide.md`, `advanced.md`, `examples.md`, and `utils.md` all repeat overlapping content with different levels of detail.
- The `project-docs` spec already requires clear separation of tutorial, how-to, reference, and architecture content, but the current file layout and MkDocs navigation do not yet reflect that separation in a discoverable, opinionated way.
- New users struggle to pick an entrypoint (Quickstart vs Advanced vs Examples), while more advanced users must scan several guides to find the right recipe or API reference for a concrete task.

## What Changes

- Introduce a documentation structure and navigation that makes content roles explicit:
  - Tutorials under `docs/tutorials/` (for example `tutorials/getting-started.md`).
  - How-to guides under `docs/how-to/` (task-oriented recipes such as configuring cloud storage, working with filesystems, reading/writing datasets, using SQL filters, syncing files, and optimizing performance).
  - Reference pages under `docs/reference/` (for example a slimmed-down `reference/api-guide.md` and a compatibility-focused `reference/utils.md`), alongside existing mkdocstrings-generated API pages under `docs/api/`.
  - Explanation pages under `docs/explanation/` (for example `explanation/architecture.md`, `explanation/concepts.md`, and `explanation/migration-0.5.md`).
- Simplify the landing page and top-level MkDocs navigation:
  - Reduce the number of first-level nav items.
  - Promote a small set of opinionated entrypoints: “Getting Started”, “How-to Guides”, “API Reference”, and “Architecture & Concepts”, plus “Installation” and “Contributing”.
  - Ensure the homepage briefly explains what fsspeckit is and routes users clearly to the correct doc layer based on their goal.
- Normalize and de-duplicate content across guides:
  - Move end-to-end onboarding content into the Getting Started tutorial and keep it focused.
  - Move concrete task recipes into how-to guides and remove duplicated examples from `advanced.md`, `examples.md`, and `utils.md`.
  - Trim reference-like sections in narrative guides and link to mkdocstrings-generated API pages instead of re-describing parameters and return types.
- Keep existing URLs as stable as practical by:
  - Retaining core topic pages (Quickstart, Advanced, Utils, Examples) as thin stubs or redirect-style pointers where feasible, or
  - Providing a clear migration/redirect matrix in the docs and release notes when a path must change.

## Impact

- Affected specs:
  - `project-docs`
    - This change primarily implements existing requirements about documentation layering and content roles.
    - It may add a small number of clarifying requirements around navigation and file layout to make the structure enforceable.
- Affected documentation (non-exhaustive):
  - `docs/index.md`
  - `docs/quickstart.md`
  - `docs/installation.md`
  - `docs/api-guide.md`
  - `docs/advanced.md`
  - `docs/examples.md`
  - `docs/utils.md`
  - `docs/migration-0.5.md`
  - `docs/architecture.md`
  - MkDocs configuration in `mkdocs.yml` (navigation structure).
- Affected API reference:
  - High-level index and descriptive text in `docs/api/index.md` and any navigation labels that need to reflect the new structure (domain modules vs utils façade remain governed by existing specs).
- No runtime behavior changes; all work is limited to documentation and site configuration.
