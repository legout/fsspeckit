# Implementation Tasks

## Ready

- [x] Create unified `normalize_path()` function in `core/filesystem/paths.py`
  - **Spec**: `core-filesystem-paths`
  - **Effort**: 2 hours
  - **Dependencies**: None

- [ ] Add tests for unified `normalize_path()`
  - **Spec**: `core-filesystem-paths`
  - **Effort**: 2 hours
  - **Dependencies**: Create unified `normalize_path()` function

- [ ] Update `datasets/path_utils.py` to use unified function
  - **Spec**: `datasets-path-utils`
  - **Effort**: 1 hour
  - **Dependencies**: Add tests for unified `normalize_path()`

- [ ] Update `PyarrowDatasetIO._normalize_path()` method
  - **Spec**: `datasets-pyarrow-io`
  - **Effort**: 1 hour
  - **Dependencies**: Update `datasets/path_utils.py` to use unified function

- [ ] Update all call sites in `core/ext/` modules
  - **Spec**: `core-ext`
  - **Effort**: 3 hours
  - **Dependencies**: Update `PyarrowDatasetIO._normalize_path()` method

- [ ] Update project-architecture spec for centralized path normalization
  - **Spec**: `project-architecture`
  - **Effort**: 1 hour
  - **Dependencies**: Create unified `normalize_path()` function

- [ ] Update documentation
  - **Spec**: `project-docs`
  - **Effort**: 2 hours
  - **Dependencies**: Update all call sites in `core/ext/` modules

## Blocked

_None_

## Pending

_None_

## In Progress

_None_

## Done

_None_
