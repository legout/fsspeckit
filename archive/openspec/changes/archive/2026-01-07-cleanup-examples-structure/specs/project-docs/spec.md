## ADDED Requirements

### Requirement: Examples Are Curated and Reference Only Supported APIs

The repository SHALL maintain an `examples/` directory whose runnable scripts
reference only APIs that exist in `src/` and do not require private credentials
or network access by default.

#### Scenario: Obsolete example trees are removed
- **WHEN** an example directory references non-existent helpers (for example `fs.pydala_dataset`) or out-of-date public APIs
- **THEN** the directory SHALL be removed from the runnable example suite (or migrated to docs-only narrative)
- **AND** the remaining `examples/README.md` SHALL not reference removed content.

#### Scenario: Examples install guidance matches maintained set
- **WHEN** a user follows installation instructions in `examples/README.md` or `examples/requirements.txt`
- **THEN** the documented dependencies SHALL be sufficient to run the maintained examples
- **AND** the guidance SHALL avoid requiring cloud credentials or external services by default.

