# `fsspeckit.utils.datetime` (legacy documentation)

There is no longer a dedicated `fsspeckit.utils.datetime` module in the
codebase. Datetime helpers live in the canonical domain package
`fsspeckit.common.datetime` and are **not** re-exported via the
`fsspeckit.utils` façade.

This page exists only to help migrate older code that previously relied on
`fsspec-utils`/`utils` datetime helpers.

## Canonical datetime helpers

The current, supported helpers are:

- `get_timestamp_column`
- `get_timedelta_str`
- `timestamp_from_string`

All of them live in `fsspeckit.common.datetime`.

## Recommended imports

**Preferred (domain package):**

```python
from fsspeckit.common.datetime import (
    get_timestamp_column,
    get_timedelta_str,
    timestamp_from_string,
)
```

If you have legacy code importing from a non-existent
`fsspeckit.utils.datetime` module, you should update it to use the canonical
imports above.
