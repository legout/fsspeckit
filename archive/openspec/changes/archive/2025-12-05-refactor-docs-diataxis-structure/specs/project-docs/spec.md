## ADDED Requirements

### Requirement: Navigation Exposes Documentation Roles

The documentation navigation and homepage SHALL make the primary documentation roles (tutorials, how-to guides, reference, and architecture/explanation) explicit so that users can quickly choose the right entrypoint for their current task.

#### Scenario: Homepage routes by documentation role

- **WHEN** a user opens the documentation landing page (`docs/index.md`)
- **THEN** they see clearly labeled links to at least:
  - A getting started/tutorial entrypoint
  - A collection of how-to guides
  - API/reference documentation
  - Architecture and/or conceptual explanations
- **AND** the page briefly describes the purpose of each link so users understand which one to choose.

#### Scenario: MkDocs navigation reflects documentation roles

- **WHEN** a user opens the documentation site navigation
- **THEN** tutorials, how-to guides, reference, and explanation/architecture content are grouped under distinguishable headings
- **AND** the number of top-level navigation items is kept small and descriptive
- **AND** domain package API reference pages remain organized under an API Reference section that treats `fsspeckit.utils` as a compatibility façade.

### Requirement: File Layout Aligns with Documentation Roles

The documentation file layout SHALL align with the defined documentation roles (tutorials, how-to guides, reference, and explanation/architecture) to reduce ambiguity and duplication, using consistent directory names where practical, while still allowing future evolution of the structure.

#### Scenario: Tutorials live under a dedicated path

- **WHEN** a contributor adds or updates a tutorial
- **THEN** the tutorial pages live under a dedicated tutorial-oriented path (for example `docs/tutorials/`)
- **AND** the navigation entry for those pages is clearly labeled as a tutorial or getting started guide.

#### Scenario: How-to guides are grouped by task

- **WHEN** a contributor adds or updates a task-focused how-to guide
- **THEN** the pages are grouped under a how-to-oriented path (for example `docs/how-to/`) or an equivalent logical grouping
- **AND** the navigation labels emphasize the task being accomplished (for example “Configure Cloud Storage”, “Read and Write Datasets”) rather than internal module names.

#### Scenario: Reference and explanation content are clearly separated

- **WHEN** a reader navigates to reference or architecture pages
- **THEN** reference-style content (API details, installation instructions) is located in reference-oriented files and sections
- **AND** explanation-style content (architecture, concepts, migration rationale) is located in explanation-oriented files and sections
- **AND** narrative guides link to reference pages instead of re-stating detailed API signatures.
