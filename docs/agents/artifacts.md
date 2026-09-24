# Artifact mapping

Mapping: generated defaults. This file is declarative documentation, not executable configuration.

- docs/research/: investigations, design studies, and probe reports.
- docs/adr/: accepted architectural decisions.
- docs/specs/: behavioral contracts.
- docs/plans/: execution maps.
- docs/tickets/: local work items.
- docs/agents/: workflow configuration, including the tracker (docs/agents/issue-tracker.md) and the context layout (docs/agents/domain.md).
- CONTEXT.md: canonical domain vocabulary per the context layout declared in docs/agents/domain.md.

Explicit project mappings recorded here override these defaults. Planning artifact and handoff semantics are owned by the `planning-contract` skill from `legout/skills`.

Setup never moves existing documents and never fabricates glossaries, ADRs, or placeholder folders to match this map. A misplaced document is evidence of misclassification, not a mapping rule; resolve conflicts explicitly with the owner.
