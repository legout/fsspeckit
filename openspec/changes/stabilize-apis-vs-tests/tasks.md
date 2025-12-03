## 1. Implementation

- [x] 1.1 Update `run_parallel` argument handling to:
  - [x] 1.1.1 Accept any non-string `Iterable` (including generators) as an iterable argument.
  - [x] 1.1.2 Materialise iterables into concrete sequences internally when required by joblib.
- [x] 1.2 Restore or minimally adjust error messages for:
  - [x] 1.2.1 Length mismatch between iterables (matching test expectations).
  - [x] 1.2.2 Missing iterable arguments (matching the `match=` patterns used in tests).
- [x] 1.3 Identify and replace mutable default arguments (`dict = {}`, `list = []`, etc.) with `None` + internal initialisation in the most commonly used helpers flagged by the reviews.
- [x] 1.4 Remove or refactor unreachable code branches that cannot be exercised and that refer to undefined state.

## 2. Testing

- [x] 2.1 Confirm that the existing tests for `run_parallel` generator input continue to pass and that new behaviour meets their expectations.
- [x] 2.2 Add or refine tests for error messages in `run_parallel` to ensure future changes do not inadvertently break the contract.
- [x] 2.3 Add tests (where appropriate) to ensure functions with previously mutable defaults behave identically after refactor.

## 3. Documentation

- [x] 3.1 Document the expected semantics of `run_parallel` (including generator support and error modes) in its docstring.
- [x] 3.2 Optionally add a brief coding-guidelines note (for contributors) about avoiding mutable default arguments and unreachable code.

