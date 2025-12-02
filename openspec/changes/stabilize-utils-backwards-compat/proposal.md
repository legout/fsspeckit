## Why

The `fsspeckit.utils` package is intended to serve as a backwards-compatible façade that re-exports helpers from domain packages (`datasets`, `sql`, `common`). Over time, implementation has moved into the domain packages, but:

- Some older code and tests still reference deeper import paths (e.g. `fsspeckit.utils.misc.Progress`).
- It is not fully clear which `utils` imports are considered stable vs deprecated.
- There is a risk that future changes will inadvertently break long-standing import paths without a plan.

To give users a predictable compatibility story, we should explicitly define the supported `utils` surface and, where necessary, add thin shims that maintain existing import paths.

## What Changes

- Define a small set of supported `fsspeckit.utils` imports (e.g. commonly used helpers like `run_parallel`, dataset handlers, dtype utilities, SQL filter re-exports).
- Add shim modules under `fsspeckit/utils/` (such as `misc.py`) where needed to re-export the canonical implementation from `common`/`datasets`/`sql`, specifically for import patterns already observed in tests or likely in user code.
- Document that new implementation code should not live in `utils`, and mark deeper `utils.*` imports as deprecated where appropriate, while keeping them functional for at least one major version.

## Impact

- **Behaviour:**
  - Existing user code and tests using established `fsspeckit.utils` import paths continue to work without modification.
  - New development is guided towards the canonical domain packages, with `utils` acting strictly as a façade.

- **Specs affected:**
  - `project-architecture` (domain-driven layout and `utils` façade responsibilities).

- **Code affected (non-exhaustive):**
  - `src/fsspeckit/utils/__init__.py`
  - New shim modules under `src/fsspeckit/utils/` as needed.

