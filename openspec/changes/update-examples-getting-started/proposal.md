# Change: Update `datasets/getting_started` Examples to Current APIs

## Why
The `examples/datasets/getting_started/*` scripts are intended as the first-run learning path, but they currently fail in a clean environment due to:

- PyArrow API drift: `pa.table(list_of_dicts)` no longer works on current PyArrow (>=22)
- Interactive prompts (`input()`) breaking non-interactive execution
- Assumptions about filesystem behavior that don’t hold (DirFileSystem base path vs absolute temp paths)

These scripts should be runnable by default, offline, and serve as the canonical beginner path.

## What Changes
- Replace deprecated/broken PyArrow table creation patterns with supported ones (e.g. `pa.Table.from_pylist` / `pa.Table.from_pydict`).
- Remove interactive `input()` prompts or gate them behind an explicit `--interactive` flag.
- Ensure each script can run offline and completes in a reasonable time budget.

## Impact
- Restores a working “Getting Started” path.
- Reduces support churn by keeping examples aligned with current dependencies.

