# `fsspeckit.utils.pyarrow` API Reference

## `dominant_timezone_per_column()`

For each timestamp column (by name) across all schemas, detect the most frequent timezone (including None).

If None and a timezone are tied, prefer the timezone. Returns a dict: {column_name: dominant_timezone}

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `schemas` | `list[pyarrow.Schema]` | A list of PyArrow schemas to analyze. |

**Example:**

```python
import pyarrow as pa
from fsspeckit.utils.pyarrow import dominant_timezone_per_column

schema1 = pa.schema([("ts", pa.timestamp("ns", tz="UTC"))])
schema2 = pa.schema([("ts", pa.timestamp("ns", tz="Europe/Berlin"))])
schema3 = pa.schema([("ts", pa.timestamp("ns"))]) # naive
schemas = [schema1, schema2, schema3]

dominant_tz = dominant_timezone_per_column(schemas)
print(dominant_tz)
# Expected: {'ts': 'UTC'} (or 'Europe/Berlin' depending on logic)
```

**Returns:**

- `dict`: {column_name: dominant_timezone}

## `standardize_schema_timezones_by_majority()`

For each timestamp column (by name) across all schemas, set the timezone to the most frequent (with tie-breaking).

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `schemas` | `list[pyarrow.Schema]` | A list of PyArrow schemas to standardize. |

**Example:**

```python
import pyarrow as pa
from fsspeckit.utils.pyarrow import standardize_schema_timezones_by_majority

schema1 = pa.schema([("ts", pa.timestamp("ns", tz="UTC"))])
schema2 = pa.schema([("ts", pa.timestamp("ns", tz="Europe/Berlin"))])
schemas = [schema1, schema2]

standardized_schemas = standardize_schema_timezones_by_majority(schemas)
print(standardized_schemas[0].field("ts").type)
print(standardized_schemas[1].field("ts").type)
# Expected: timestamp[ns, tz=Europe/Berlin] (or UTC, depending on tie-breaking)
```

**Returns:**

- `list[pyarrow.Schema]`: A new list of schemas with updated timestamp timezones.

## `standardize_schema_timezones()`

Standardize timezone info for all timestamp columns in a list of PyArrow schemas.

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `schemas` | `list[pyarrow.Schema]` | The list of PyArrow schemas to process. |
| `timezone` | `str` or `None` | The target timezone to apply to timestamp columns. If None, timezones are removed. If "auto", the most frequent timezone across schemas is used. |

**Example:**

```python
import pyarrow as pa
from fsspeckit.utils.pyarrow import standardize_schema_timezones

schema1 = pa.schema([("ts", pa.timestamp("ns", tz="UTC"))])
schema2 = pa.schema([("ts", pa.timestamp("ns"))]) # naive
schemas = [schema1, schema2]

# Remove timezones
new_schemas_naive = standardize_schema_timezones(schemas, timezone=None)
print(new_schemas_naive[0].field("ts").type)
# Expected: timestamp[ns]

# Set a specific timezone
new_schemas_berlin = standardize_schema_timezones(schemas, timezone="Europe/Berlin")
print(new_schemas_berlin[0].field("ts").type)
# Expected: timestamp[ns, tz=Europe/Berlin]
```

**Returns:**

- `list[pyarrow.Schema]`: New schemas with standardized timezone info.

## `unify_schemas()`

Unify a list of PyArrow schemas into a single schema.

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `schemas` | `list[pyarrow.Schema]` | List of PyArrow schemas to unify. |
| `use_large_dtypes` | `bool` | If True, keep large types like large_string. |
| `timezone` | `str` or `None` | If specified, standardize all timestamp columns to this timezone. If "auto", use the most frequent timezone across schemas. If None, remove timezone from all timestamp columns. |
| `standardize_timezones` | `bool` | If True, standardize all timestamp columns to the most frequent timezone. |

**Returns:**

- `pyarrow.Schema`: A unified PyArrow schema.

## `cast_schema()`

Cast a PyArrow table to a given schema, updating the schema to match the table's columns.

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `table` | `pyarrow.Table` | The PyArrow table to cast. |
| `schema` | `pyarrow.Schema` | The target schema to cast the table to. |

**Returns:**

- `pyarrow.Table`: A new PyArrow table with the specified schema.

## `convert_large_types_to_normal()`

Convert large types in a PyArrow schema to their standard types.

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `schema` | `pyarrow.Schema` | The PyArrow schema to convert. |

**Returns:**

- `pyarrow.Schema`: A new PyArrow schema with large types converted to standard types.

## `opt_dtype()`

Optimize data types of a PyArrow Table for performance and memory efficiency.

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `table` | `pyarrow.Table` | |
| `include` | `list[str]`, optional | |
| `exclude` | `list[str]`, optional | |
| `time_zone` | `str`, optional | |
| `shrink_numerics` | `bool` | |
| `allow_unsigned` | `bool` | |
| `use_large_dtypes` | `bool` | |
| `strict` | `bool` | |
| `allow_null` | `bool` | If False, columns that only hold null-like values will not be converted to pyarrow.null(). |
| `sample_size` | `int` or `None` | Maximum number of cleaned values inspected during regex-based inference (`1024` by default). The inferred schema is derived solely from the samples before casting the complete column. |
| `sample_method` | `str` | Sampling strategy (`"first"` or `"random"`) for the inference subset. |

**Returns:**

- `pyarrow.Table`: A new table casted to the optimal schema.
