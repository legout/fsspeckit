## Context
After adding PyArrow and DuckDB dataset handlers, consistency of method names and parameters becomes important for UX and
tooling. A small shared interface (informal or via typing protocol) will reduce drift and make documentation clearer.

## Goals / Non-Goals
- Goals:
  - Define the common surface (method names/params) that both handlers should expose.
  - Allow backend-specific extensions but keep the core consistent.
  - Optionally express this as a typing protocol for editor support.
- Non-Goals:
  - Forcing backends to implement operations they do not support.
  - Removing existing backwards-compatible aliases.

## Decisions
- Start with a documented interface; if practical, add a `typing.Protocol` to express the shared surface.
- Use aliases where needed to align names without breaking existing code.
- Keep differences explicit (e.g., operations only supported in one backend) in docs.

## Risks / Trade-offs
- Introducing aliases can add minor maintenance overhead, but reduces user confusion.
- A protocol that is too strict could limit backend-specific features; keep it minimal.

## Migration Plan
1) List current DuckDB and PyArrow handler methods; find the common subset.
2) Define the shared interface and add aliases where useful.
3) Update docs and, if added, ensure protocol is satisfied by both handlers.
