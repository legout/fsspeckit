# `fsspeckit.utils.types` (legacy documentation)

There is no standalone `fsspeckit.utils.types` module in the current
codebase. Type-conversion helpers live in the canonical
`fsspeckit.common.types` module and are also exposed via the top-level
`fsspeckit.utils` façade.

This page exists to document the compatibility mapping and to help migrate
older code.

## Canonical helpers

The primary helpers are:

- `dict_to_dataframe`
- `to_pyarrow_table`

They live in `fsspeckit.common.types`.

## Compatibility mapping

| Usage style                  | Import path                                 |
|------------------------------|---------------------------------------------|
| Canonical (recommended)      | `from fsspeckit.common.types import dict_to_dataframe, to_pyarrow_table` |
| Façade (backwards compatible)| `from fsspeckit.utils import dict_to_dataframe, to_pyarrow_table`        |

> For full parameter/return documentation and examples, see the
> `fsspeckit.common` API reference. This page is intentionally minimal to
> avoid duplicating and drifting from the canonical docs.
