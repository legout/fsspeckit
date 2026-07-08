# `fsspeckit.utils.pyarrow` (compatibility façade)

The `fsspeckit.utils.pyarrow` module is a **backwards-compatibility façade**.
It re-exports a subset of PyArrow-related helpers from the canonical
`fsspeckit.datasets.pyarrow` module so that older code can continue to work.

New code SHOULD import directly from `fsspeckit.datasets.pyarrow`.

## Exposed helpers

| Compat name                          | Canonical helper                        | Canonical module                 |
|--------------------------------------|-----------------------------------------|----------------------------------|
| `cast_schema`                        | `cast_schema`                           | `fsspeckit.datasets.pyarrow`    |
| `convert_large_types_to_normal`      | `convert_large_types_to_normal`         | `fsspeckit.datasets.pyarrow`    |
| `dominant_timezone_per_column`       | `dominant_timezone_per_column`          | `fsspeckit.datasets.pyarrow`    |
| `merge_parquet_dataset_pyarrow`      | `merge_parquet_dataset_pyarrow`         | `fsspeckit.datasets.pyarrow`    |
| `opt_dtype_pa`                       | `opt_dtype`                             | `fsspeckit.datasets.pyarrow`    |
| `standardize_schema_timezones`       | `standardize_schema_timezones`          | `fsspeckit.datasets.pyarrow`    |
| `standardize_schema_timezones_by_majority` | `standardize_schema_timezones_by_majority` | `fsspeckit.datasets.pyarrow` |

> Detailed parameter and return-type documentation for these helpers is
> available in the `fsspeckit.datasets` API reference. This page documents only
> the legacy compatibility surface.

## Recommended imports

**Preferred (domain packages):**

```python
from fsspeckit.datasets.pyarrow import merge_parquet_dataset_pyarrow
```

**Backwards-compatible (façade):**

```python
from fsspeckit.utils.pyarrow import merge_parquet_dataset_pyarrow
```

Use the façade only to keep older code working while you migrate imports to
`fsspeckit.datasets.pyarrow`.

