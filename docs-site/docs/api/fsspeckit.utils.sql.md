# `fsspeckit.utils.sql` (compatibility façade)

The `fsspeckit.utils.sql` module is a **backwards-compatibility façade**.
It exposes a single helper from the canonical `fsspeckit.sql` package.

New code SHOULD import from `fsspeckit.sql` directly.

## Exposed helper

| Compat name       | Canonical helper | Canonical module |
|-------------------|------------------|------------------|
| `get_table_names` | `get_table_names`| `fsspeckit.sql`  |

## Example

**Preferred (domain package):**

```python
from fsspeckit.sql import get_table_names

query = "SELECT a FROM my_table WHERE b > 10"
tables = get_table_names(query)
print(tables)  # ['my_table']
```

**Backwards-compatible (façade):**

```python
from fsspeckit.utils.sql import get_table_names
```

> Parameter and return-type details for `get_table_names` are documented in the
> `fsspeckit.sql` API reference. This page focuses on the compatibility layer
> only.
