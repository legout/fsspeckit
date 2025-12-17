## 1. Runner Correctness

- [x] 1.1 Fix `examples/run_examples.py` to resolve example paths relative to `examples/` (script directory), not `cwd`
- [x] 1.2 Update `EXAMPLE_CATEGORIES` and learning path ordering to match the kept example tree
- [x] 1.3 Remove or gate interactive prompts (e.g. `input()`) behind an explicit `--interactive` flag; default to non-interactive

## 2. Validator Simplification

- [x] 2.1 Update `examples/test_examples.py` to avoid repo-incompatible "style" requirements, focusing on syntax/import checks
- [x] 2.2 Add an optional smoke-run mode with strict per-example timeouts

## 3. Validation

- [x] 3.1 Run `examples/run_examples.py --list` and confirm it reflects the on-disk examples
- [x] 3.2 Run a smoke pass for the kept examples (after updates land) and confirm non-interactive operation works end-to-end

