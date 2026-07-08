# `fsspeckit.utils.polars` (compatibility façade)

The `fsspeckit.utils.polars` module is a **backwards-compatibility façade**.
It re-exports a small set of helpers from `fsspeckit.common.polars` so that
older code can continue to work after the domain refactor.

New code SHOULD import directly from `fsspeckit.common.polars`.

## Exposed helpers

| Compat name      | Canonical helper | Canonical module           |
|------------------|------------------|----------------------------|
| `opt_dtype_pl`   | `opt_dtype`      | `fsspeckit.common.polars` |
| `pl`             | `pl`             | `fsspeckit.common.polars` |
| `explode_all`    | `explode_all`    | `fsspeckit.common.polars` |
| `drop_null_columns` | `drop_null_columns` | `fsspeckit.common.polars` |

> Full API documentation for these helpers is available under the
> `fsspeckit.common` domain package reference. This page focuses on the
> compatibility layer only.

## Recommended imports

**Preferred (domain package):**

```python
from fsspeckit.common.polars import opt_dtype, explode_all, drop_null_columns
import polars as pl
```

**Backwards-compatible (façade):**

```python
from fsspeckit.utils.polars import opt_dtype_pl, explode_all, drop_null_columns
```

Use the façade only when maintaining legacy code. For new development, prefer
the canonical imports from `fsspeckit.common.polars`.

