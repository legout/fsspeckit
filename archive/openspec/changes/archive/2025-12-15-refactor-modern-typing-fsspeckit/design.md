## Context

The project was originally written using pre-PEP 604 typing patterns and `typing.List`/`typing.Dict`. Subsequent work
introduced `from __future__ import annotations` and PEP 604-style unions in some modules, but usage is inconsistent.

There is at least one concrete bug where `Union[...]` is used in annotations without importing `Union`, causing
module import failures. This change standardises the entire codebase on modern typing conventions and removes the
remaining failure modes around missing `typing` imports.

## Goals / Non-Goals

- Goals:
  - Use PEP 604 union syntax (`X | Y`) consistently.
  - Use built-in generics (`list`, `dict`) consistently.
  - Keep all type semantics the same (no behavioural changes).
  - Maintain existing optional-dependency patterns (`TYPE_CHECKING`, `common.optional` helpers).
- Non-Goals:
  - Introducing new public APIs.
  - Changing any existing runtime logic around optional dependencies.

## Decisions

- Prefer `X | Y` and `X | None` over `Union[X, Y]` and `Optional[X]`.
- Prefer `list[T]` / `dict[K, V]` over `List[T]` / `Dict[K, V]`.
- Continue using `from __future__ import annotations` where already present, and add it to any modules that need
  forward references or to avoid evaluation-time issues after changing annotations.
- For optional dependencies:
  - Keep `if TYPE_CHECKING:` imports for type checkers.
  - Use `common.optional` helpers for runtime access, as already specified by `core-lazy-imports` and `utils-optional-separation`.

## Risks / Trade-offs

- Touches many files, so there is some merge-conflict risk with parallel work; however, the change is mechanical and
  can be rebased easily.
- Static type checking might surface existing latent issues once annotations are simplified; these should be treated
  as correctness improvements.

## Migration Plan

1. Apply mechanical changes module-by-module, keeping commits small and focused when possible.
2. Run tests and type checking after each logical group of modules (core, common, storage_options, datasets, etc.).
3. Update documentation with the new typing guidelines.

## Open Questions

- Should we enforce these typing conventions via a linter (e.g. ruff rules) in a later change?

