## ADDED Requirements

### Requirement: Modern typing conventions across modules

The project SHALL use modern Python typing conventions consistently across all modules.

#### Scenario: PEP 604 unions are used instead of typing.Union
- **WHEN** reviewing type annotations in core, common, datasets, storage_options, sql, and utils modules
- **THEN** union types are expressed using `X | Y` and `X | None`
- **AND** `typing.Union` and `typing.Optional` are not used in new or updated code.

#### Scenario: Built-in generics replace typing.List and typing.Dict
- **WHEN** reviewing collection type annotations
- **THEN** list and dict types use `list[T]` and `dict[K, V]`
- **AND** `typing.List` and `typing.Dict` are not used in new or updated code.

#### Scenario: Optional dependency types respect lazy-import rules
- **WHEN** optional-dependency types such as Polars, Pandas, PyArrow, DuckDB, sqlglot, or orjson are used in annotations
- **THEN** those types are imported only under `if TYPE_CHECKING:` or referenced via shared helpers in `fsspeckit.common.optional`
- **AND** importing modules does not require those optional packages at runtime.

