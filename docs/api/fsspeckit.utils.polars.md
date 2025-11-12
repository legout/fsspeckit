# `fsspeckit.utils.polars` API Reference

## `opt_dtype()`

Optimize data types of a Polars DataFrame for performance and memory efficiency.

This function analyzes each column and converts it to the most appropriate data type based on content, handling string-to-type conversions and numeric type downcasting.

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `df` | `polars.DataFrame` | The input Polars DataFrame to optimize. |
| `include` | `list[str]` or `None` | Optional list of column names to include in the optimization process. If None, all columns are considered. |
| `exclude` | `list[str]` or `None` | Optional list of column names to exclude from the optimization process. |
| `time_zone` | `str` or `None` | Optional time zone string for datetime parsing. |
| `shrink_numerics` | `bool` | If True, numeric columns will be downcasted to smaller data types if possible without losing precision. |
| `allow_unsigned` | `bool` | If True, unsigned integer types will be considered for numeric column optimization. |
| `allow_null` | `bool` | If True, columns containing only null values will be cast to the Null type. |
| `sample_size` | `int` or `None` | Maximum number of cleaned values inspected during regex-based inference (`1024` by default). The inferred schema is based solely on this sample before casting the full column. |
| `sample_method` | `str` | Which subset to inspect (`"first"` or `"random"`) when sampling values for inference. |
| `strict` | `bool` | If True, an error will be raised if any column cannot be optimized (e.g., due to type inference issues). |

**Example:**

```python
import polars as pl
from fsspeckit.utils.polars import opt_dtype

df = pl.DataFrame({
    "col_int": ["1", "2", "3"],
    "col_float": ["1.1", "2.2", "3.3"],
    "col_bool": ["True", "False", "True"],
    "col_date": ["2023-01-01", "2023-01-02", "2023-01-03"],
    "col_str": ["a", "b", "c"],
    "col_null": [None, None, None]
})
optimized_df = opt_dtype(df, shrink_numerics=True)
print(optimized_df.schema)
# Expected output similar to:
# Schema({
#     'col_int': Int8,
#     'col_float': Float32,
#     'col_bool': Boolean,
#     'col_date': Date,
#     'col_str': Utf8,
#     'col_null': Null
# })
```

**Returns:**

- `polars.DataFrame`: DataFrame with optimized data types

## `unnest_all()`

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `df` | `polars.DataFrame` | The input Polars DataFrame. |
| `seperator` | `str` | The separator used to flatten nested column names. Defaults to '_'. |
| `fields` | `list[str]` or `None` | Optional list of specific fields (structs) to unnest. If None, all struct columns will be unnested. |

**Example:**

```python
import polars as pl
from fsspeckit.utils.polars import explode_all

df = pl.DataFrame({
    "id": [1, 2],
    "values": [[10, 20], [30]]
})
exploded_df = explode_all(df)
print(exploded_df)
# shape: (3, 2)
# ┌─────┬────────┐
# │ id  ┆ values │
# │ --- ┆ ---    │
# │ i64 ┆ i64    │
# ╞═════╪════════╡
# │ 1   ┆ 10     │
# │ 1   ┆ 20     │
# │ 2   ┆ 30     │
# └─────┴────────┘
```

```python
import polars as pl
from fsspeckit.utils.polars import unnest_all

df = pl.DataFrame({
    "id": [1, 2],
    "data": [
        {"a": 1, "b": {"c": 3}},
        {"a": 4, "b": {"c": 6}}
    ]
})
unnested_df = unnest_all(df, seperator='__')
print(unnested_df)
# shape: (2, 3)
# ┌─────┬──────┬───────┐
# │ id  ┆ data__a ┆ data__b__c │
# │ --- ┆ ---  ┆ ---     │
# │ i64 ┆ i64  ┆ i64     │
# ╞═════╪══════╪═════════╡
# │ 1   ┆ 1    ┆ 3       │
# │ 2   ┆ 4    ┆ 6       │
# └─────┴──────┴─────────┘
```

**Returns:**

- `None`

## `explode_all()`

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `df` | `polars.DataFrame` | The input Polars DataFrame. |

**Example:**

```python
import polars as pl
from fsspeckit.utils.polars import drop_null_columns

df = pl.DataFrame({
    "col1": [1, 2, 3],
    "col2": [None, None, None],
    "col3": ["a", None, "c"]
})
df_cleaned = drop_null_columns(df)
print(df_cleaned)
# shape: (3, 2)
# ┌──────┬───────┐
# │ col1 ┆ col3  │
# │ ---  ┆ ---   │
# │ i64  ┆ str   │
# ╞══════╪═══════╡
# │ 1    ┆ a     │
# │ 2    ┆ null  │
# │ 3    ┆ c     │
# └──────┴───────┘
```

**Returns:**

- `None`

## `with_strftime_columns()`

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `df` | `polars.DataFrame` | The input Polars DataFrame. |
| `strftime` | `str` | The `strftime` format string (e.g., "%Y-%m-%d" for date, "%H" for hour). |
| `timestamp_column` | `str` | The name of the timestamp column to use. Defaults to 'auto' (attempts to infer). |
| `column_names` | `list[str]` or `None` | Optional list of new column names to use for the generated columns. If None, names are derived from the `strftime` format. |

**Returns:**

- `None`

## `with_truncated_columns()`

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `df` | `polars.DataFrame` | The input Polars DataFrame. |
| `truncate_by` | `str` | The duration string to truncate by (e.g., "1h", "1d", "1mo"). |
| `timestamp_column` | `str` | The name of the timestamp column to truncate. Defaults to 'auto' (attempts to infer). |
| `column_names` | `list[str]` or `None` | Optional list of new column names for the truncated columns. If None, names are derived automatically. |

**Returns:**

- `None`

## `with_datepart_columns()`

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `df` | `polars.DataFrame` | The input Polars DataFrame. |
| `timestamp_column` | `str` | The name of the timestamp column to extract date parts from. Defaults to 'auto' (attempts to infer). |
| `year` | `bool` | If True, extract the year as a new column. |
| `month` | `bool` | If True, extract the month as a new column. |
| `week` | `bool` | If True, extract the week of the year as a new column. |
| `yearday` | `bool` | If True, extract the day of the year as a new column. |
| `monthday` | `bool` | If True, extract the day of the month as a new column. |
| `day` | `bool` | If True, extract the day of the week (1-7, Monday=1) as a new column. |
| `weekday` | `bool` | If True, extract the weekday (0-6, Monday=0) as a new column. |
| `hour` | `bool` | If True, extract the hour as a new column. |
| `minute` | `bool` | If True, extract the minute as a new column. |
| `strftime` | `str` or `None` | Optional `strftime` format string to apply to the timestamp column before extracting parts. |

**Returns:**

- `None`

## `with_row_count()`

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `df` | `polars.DataFrame` | The input Polars DataFrame. |
| `over` | `list[str]` or `None` | Optional list of column names to partition the data by before adding row counts. If None, a global row count is added. |

**Returns:**

- `None`

## `drop_null_columns()`

Remove columns with all null values from the DataFrame.

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `df` | `polars.DataFrame` | The input Polars DataFrame. |

**Returns:**

- `None`

## `unify_schemas()`

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `dfs` | `list[polars.DataFrame]` | A list of Polars DataFrames to unify their schemas. |

**Returns:**

- `None`

## `cast_relaxed()`

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `df` | `polars.DataFrame` | The input Polars DataFrame to cast. |
| `schema` | `dict` or `polars.Schema` | The target schema to cast the DataFrame to. Can be a dictionary mapping column names to data types or a Polars Schema object. |

**Returns:**

- `None`

## `delta()`

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `df1` | `polars.DataFrame` | The first Polars DataFrame. |
| `df2` | `polars.DataFrame` | The second Polars DataFrame. |
| `subset` | `list[str]` or `None` | Optional list of column names to consider when calculating the delta. If None, all columns are used. |
| `eager` | `bool` | If True, the delta calculation is performed eagerly. Defaults to False (lazy). |

**Returns:**

- `None`

## `partition_by()`

**Parameters:**

| Name | Type | Description |
|:---|:---|:---|
| `df` | `polars.DataFrame` | The input Polars DataFrame to partition. |
| `timestamp_column` | `str` or `None` | The name of the timestamp column to use for time-based partitioning. Defaults to None. |
| `columns` | `list[str]` or `None` | Optional list of column names to partition by. Defaults to None. |
| `strftime` | `str` or `None` | Optional `strftime` format string for time-based partitioning. Defaults to None. |
| `timedelta` | `str` or `None` | Optional timedelta string (e.g., "1h", "1d") for time-based partitioning. Defaults to None. |
| `num_rows` | `int` or `None` | Optional number of rows per partition for row-based partitioning. Defaults to None. |

**Returns:**

- `None`
