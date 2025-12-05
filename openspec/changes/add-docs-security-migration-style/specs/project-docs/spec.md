## ADDED Requirements

### Requirement: Security and Error Handling Documentation

The documentation SHALL explain how fsspeckit protects against common security and error-handling issues and SHALL surface the security helpers exposed in `fsspeckit.common.security`.

#### Scenario: Security helpers are documented for users

- **WHEN** a developer searches the documentation for security or error handling guidance
- **THEN** they find a dedicated section (for example in `docs/advanced.md` or a dedicated security page) that describes:
  - Path validation via `validate_path`
  - Compression codec validation via `validate_compression_codec`
  - Credential scrubbing helpers (`scrub_credentials`, `scrub_exception`, `safe_format_error`)
- **AND** the examples show how to apply these helpers in the context of dataset operations and storage configuration.

#### Scenario: Architecture docs reference security model

- **WHEN** a DevOps engineer reviews `docs/architecture.md` for production deployment guidance
- **THEN** the page describes how path validation, codec validation, and credential scrubbing contribute to safe usage
- **AND** it links to the detailed security/error-handling section for implementation examples.

### Requirement: Versioned Migration Guides

The project SHALL provide versioned migration guides for significant refactors so that users can understand how to upgrade existing codebases to newer fsspeckit releases.

#### Scenario: Migration guide for 0.5.x refactor exists

- **WHEN** a user upgrades from a pre–0.5.x version to the domain-package-based 0.5.x release
- **THEN** they can open a migration guide (for example `docs/migration-0.5.md`)
- **AND** it explains:
  - How to move imports from `fsspec-utils` and `fsspeckit.utils` to the new domain packages
  - How logging and error-handling behavior changed (including environment variables)
  - How dataset maintenance and optional dependency handling changed.

#### Scenario: Migration guides are discoverable

- **WHEN** a user visits the documentation homepage or architecture overview
- **THEN** they see clear links to relevant migration guides for their version
- **AND** the links are kept up to date as new major refactors are introduced.

### Requirement: Documentation Layering and Roles

The documentation SHALL clearly separate tutorial, how-to, reference, and architecture content so that readers know where to look for each type of information and so that API behavior is primarily described in reference pages generated from code.

#### Scenario: Quickstart focuses on end-to-end onboarding

- **WHEN** a new user opens `docs/quickstart.md`
- **THEN** the page walks through a concise, end-to-end example (installation, filesystem creation, basic storage options, simple dataset operations)
- **AND** it does not attempt to document every parameter or advanced behavior of individual helpers (those are linked from API reference pages instead).

#### Scenario: API guide and reference avoid duplication

- **WHEN** a user reads `docs/api-guide.md` and the API reference under `docs/api/`
- **THEN** the API guide focuses on task-oriented patterns and cross-cutting examples
- **AND** detailed parameter/return semantics are delegated to mkdocstrings-generated reference pages
- **AND** the two sources do not contradict each other about supported parameters or behaviors.

#### Scenario: Architecture docs focus on concepts and rationale

- **WHEN** a reader opens `docs/architecture.md`
- **THEN** the page explains design decisions, domain boundaries, and integration patterns
- **AND** does not introduce new API behaviors or function signatures that differ from those in the reference
- **AND** it links to the relevant how-to and reference sections for concrete usage details.

