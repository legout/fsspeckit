## 1. Implementation

- [x] 1.1 Update `fsspeckit.core.*` modules to use PEP 604 unions and built-in generics.
- [x] 1.2 Update `fsspeckit.common.*` modules to use PEP 604 unions and built-in generics.
- [x] 1.3 Update `fsspeckit.storage_options.*` modules to use PEP 604 unions and built-in generics.
- [x] 1.4 Update `fsspeckit.datasets.*`, `fsspeckit.sql.*`, and `fsspeckit.utils.*` modules to the same typing style.
- [x] 1.5 Remove now-unused `typing.Union`, `Optional`, `List`, and `Dict` imports where no longer required.
- [x] 1.6 Ensure optional-dependency types continue to use `TYPE_CHECKING` and/or `common.optional` helpers as per existing specs.

## 2. Testing

- [x] 2.1 Run the full test suite (`pytest`) and confirm there are no regressions.
- [x] 2.2 Run static type checking (if configured, e.g. `mypy` or `pyright`) to ensure new annotations are accepted.

## 3. Documentation

- [x] 3.1 Add or update a short "Typing conventions" section in `openspec/project.md` or contributing documentation to describe:
  - Use of PEP 604 unions (`X | Y`) instead of `Union[X, Y]`.
  - Use of built-in generics (`list[str]`, `dict[str, Any]`) instead of `List[str]`, `Dict[str, Any]`.
  - Expectations around optional-dependency types and `TYPE_CHECKING` imports.

