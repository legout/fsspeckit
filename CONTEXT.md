# fsspeckit Documentation Context

The terms in this context define the user-facing documentation concepts used to plan and maintain fsspeckit documentation.

## Language

**Local dataset lifecycle**:
The offline-first user journey in which a Python user who can manage a virtual environment installs fsspeckit, configures a local filesystem, writes a dataset, reads it back, and learns when merge is the appropriate next operation.
_Avoid_: Quickstart, basic workflow, local demo

**Merge handoff**:
The tutorial boundary that explains when a simple dataset write is sufficient and routes users who need to reconcile existing data to a dedicated merge how-to guide.
_Avoid_: Merge tutorial, basic merge

**Minimal explicit schema**:
The smallest readable field definition used in the local dataset lifecycle to make a dataset’s stored shape deterministic without teaching schema design, evolution, or validation in depth.
_Avoid_: Inferred schema, schema tutorial

**Tutorial sandbox**:
A clearly named temporary directory that contains every file created by the local dataset lifecycle and is the only location the example explicitly cleans up.
_Avoid_: Working directory cleanup, project-root data

**Canonical tutorial**:
A narrative documentation page that explains the local dataset lifecycle step by step and includes a complete copyable script; repository examples support it rather than replacing it.
_Avoid_: Example-first documentation, notebook-first tutorial

**Integration preview**:
A brief next-steps pointer from the local dataset lifecycle to cloud, SQL, monitoring, or other advanced integrations without using them in its copyable script.
_Avoid_: Integration setup, optional-dependency tutorial

**Lifecycle success signal**:
The read-back assertion and concise human-readable output that demonstrate the local dataset lifecycle produced the expected stored data.
_Avoid_: Visual check, write-only success

**Tutorial diagnosis handoff**:
The short failure-oriented section following a happy-path tutorial that routes path, schema, and dependency problems to installation, filesystem, and troubleshooting references.
_Avoid_: Inline troubleshooting branches, error-free tutorial

**Concept handoff**:
The division in which a tutorial defines filesystem, dataset, write, and merge terms only when needed, then links to an Explanation page for their durable conceptual model and rationale.
_Avoid_: Tutorial-as-reference, explanation prerequisite

**Primary documentation entry point**:
The homepage call to action that starts a library user on the local dataset lifecycle while retaining how-to, reference, explanation, migration, and integration routes as visible secondary choices.
_Avoid_: API-first homepage, role-selection gate

**Migration entry point**:
The visible homepage and architecture-route link to refactor migration guidance, reinforced by contextual notices wherever retained legacy imports or workflows may still be encountered.
_Avoid_: Redirect-only migration, hidden migration section

**Hybrid API reference**:
Reference presentation in which generated pages provide authoritative public symbols and docstrings while curated pages explain API selection, imports, configuration, and result interpretation.
_Avoid_: Generated-only reference, hand-written symbol inventory

**Manual tutorial review**:
The pre-minor-release review of the canonical tutorial’s copyable script against the supported public API, without extracting or continuously executing the script in CI.
_Avoid_: Snippet-execution framework, unreviewed tutorial code

**Manual documentation audit**:
The pre-minor-release human review that checks site buildability, navigation, links, refactor accuracy, API discoverability, and tutorial prose without making any automated documentation check mandatory.
_Avoid_: Required documentation CI, build-only validation

**Bounded-impact documentation review**:
The manual content-review scope for each minor release: always check the canonical
tutorial, then check modified user-facing pages and every reference or migration
page affected by a public-API, extras, or navigation change against the
supported-public-API inventory.
_Avoid_: Full-site reread, changed-files-only review

**Legacy import policy**:
The rule that current tutorials, how-to guides, and migration routes use canonical imports, while legacy import paths and renamed APIs appear only as deprecation notes in reference.
_Avoid_: Contextual legacy notices, side-by-side imports

**Supported migration source**:
The immediately preceding publicly supported pre-refactor layout from which documentation provides an actionable workflow upgrade path to fsspeckit 0.22.x.
_Avoid_: All-version migration, versionless migration

**Shared documentation audit**:
The pre-minor-release manual documentation audit performed as a best-effort team convention without a designated accountable owner or release-blocking effect.
_Avoid_: Release-owner audit, documentation-maintainer gate

**Supported public API**:
A stable export or explicitly inventoried direct-import module intended for library
users. Public-test imports provide audit evidence but do not alone create a
support commitment, and underscore-prefixed helpers remain internal.
_Avoid_: Test-defined API, all-imports public surface

**Integration and extras matrix**:
The authoritative curated reference that maps each optional workflow or provider
to its `fsspeckit` extra. Individual API pages state their requirement and link
back to this matrix.
_Avoid_: Per-page-only installation guidance, integration setup in the
canonical tutorial

**Compatibility façade**:
The legacy `fsspeckit.utils` import surface, documented only through concise
migration/deprecation mappings to canonical domain modules and omitted from
generated API reference pages.
_Avoid_: Primary API surface, generated compatibility reference

**Composite-key dataset workflow**:
An offline example workflow in which a persisted local dataset identifies a
record by two or more fields, then demonstrates reconciliation and duplicate
removal against that dataset.
_Avoid_: In-memory compatibility demo, single-key-only example

**Import-safe example**:
An example whose module can load with the base development environment; optional
provider setup is reached only through an explicitly selected integration path.
_Avoid_: Import-time provider construction, hidden extra requirement
