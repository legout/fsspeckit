## Why

Several helpers have drifted from the behaviour expected by the test suite or from historical usage patterns, leading to:

- Changes in error message wording that break tests that assert on `match=...`.
- Loss of generator support in `run_parallel`, even though tests still use generators.
- Unreachable code and mutable default arguments in some helpers, which are both error-prone and contrary to established style.

To keep the tests as a reliable executable specification of behaviour, we should explicitly bring these APIs back into alignment with the expectations encoded in the tests, and remove patterns that are likely to cause bugs.

## What Changes

- Adjust `run_parallel` to:
  - Treat general non-string `Iterable`s (including generators) as valid iterables, materialising them as needed.
  - Restore or preserve the error message wording expected by existing tests for length mismatch and “no iterables” conditions.
- Clean up mutable default arguments and unreachable branches in core helpers where they are identified in the reviews as problematic, without changing intended behaviour.

## Impact

- **Behaviour:**
  - The semantics of `run_parallel` and related helpers will match tests and historical expectations more closely, reducing the risk of surprising regressions.
  - Code quality improves by removing patterns like mutable defaults and dead code, while keeping the external behaviour stable.

- **Specs affected:**
  - `project-architecture` (coding conventions and correctness).

- **Code affected (non-exhaustive):**
  - `src/fsspeckit/common/misc.py` (`run_parallel`).
  - Other helper modules where mutable defaults/unreachable code have been flagged.

