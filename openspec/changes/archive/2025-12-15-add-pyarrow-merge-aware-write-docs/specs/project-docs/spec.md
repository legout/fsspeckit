## ADDED Requirements

### Requirement: Provide comprehensive merge strategy guidance

Users SHALL have access to comprehensive documentation explaining when and how to use each merge strategy.

#### Scenario: Strategy selection guidance
- **WHEN** a user needs to perform merge operations
- **THEN** documentation SHALL provide clear guidance on selecting appropriate merge strategy
- **AND** SHALL include real-world use cases for each strategy
- **AND** SHALL explain the behavior of each strategy with existing and new data

#### Scenario: Practical examples for all strategies
- **WHEN** a user wants to implement merge operations
- **THEN** documentation SHALL include working examples for all merge strategies
- **AND** SHALL show before/after data states
- **AND** SHALL demonstrate common patterns like composite keys and custom ordering

### Requirement: Ensure discoverability of merge functionality

Merge-aware write functionality SHALL be easily discoverable through documentation structure.

#### Scenario: Documentation navigation
- **WHEN** a user browses dataset documentation
- **THEN** merge-aware write functionality SHALL be prominently featured
- **AND** SHALL be accessible from multiple entry points (API docs, how-to guides, examples)
- **AND** SHALL be cross-referenced with related functionality