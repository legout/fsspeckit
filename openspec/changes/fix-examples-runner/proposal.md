# Change: Fix and Simplify the Examples Runner/Validator

## Why
`examples/run_examples.py` and `examples/test_examples.py` are currently out of sync with the real `examples/` tree and with common usage patterns:

- `run_examples.py` fails on relative paths due to incorrect `Path.relative_to(Path.cwd())` usage.
- The category list in `run_examples.py` does not match the actual example directories and filenames (missing/extra categories).
- Some examples are interactive (`input()`), which breaks automated runs.
- `test_examples.py` enforces style rules that do not match the repo and does not provide a reliable “smoke runnable” signal.

The project needs a small, reliable runner that can be used to validate that examples still execute after API changes.

## What Changes
- Fix `examples/run_examples.py` path handling so it can run examples by relative or absolute path.
- Align runner categories to the actual kept example set after cleanup.
- Add a non-interactive mode (default for CI) and skip/flag interactive prompts.
- Simplify or replace `examples/test_examples.py` to focus on:
  - Syntax validity
  - Import validity
  - Optional smoke-run (opt-in / bounded time)

## Impact
- Makes example validation repeatable and usable in CI.
- Provides a stable guardrail to prevent future example drift.

