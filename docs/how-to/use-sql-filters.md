# Use SQL Filters

fsspeckit translates a SQL `WHERE` clause into a PyArrow or Polars filter
expression, so you can write a predicate once and apply it across frameworks.
The translation is schema-aware: comparisons and literals are built from the
target column types.

SQL filter translation requires the `sql` extra. See the
[extras matrix](../installation.md#optional-extras).

## Basic translation

Pass the SQL string and a schema. The function returns a native filter
expression you can hand to a dataset scan or a DataFrame filter.

```python
import pyarrow as pa
from fsspeckit.sql import sql2pyarrow_filter

schema = pa.schema([
    ("id", pa.int64()),
    ("category", pa.string()),
    ("value", pa.float64()),
    ("timestamp", pa.timestamp("us")),
])

pa_filter = sql2pyarrow_filter("id > 100 AND category IN ('A', 'B', 'C')", schema)

import pyarrow.parquet as pq
dataset = pq.ParquetDataset("data/")
table = dataset.to_table(filter=pa_filter)
print(f"Filtered rows: {table.num_rows}")
```

For Polars, build a Polars schema and use `sql2polars_filter`:

```python
import polars as pl
from fsspeckit.sql import sql2polars_filter

schema = pl.Schema({
    "id": pl.Int64,
    "category": pl.String,
    "value": pl.Float64,
    "timestamp": pl.Datetime,
})

pl_filter = sql2polars_filter("value > 100 AND timestamp >= '2023-01-01'", schema)

df = pl.read_parquet("data.parquet")
filtered = df.filter(pl_filter)
```

## Cross-framework use

The same SQL string works for both frameworks against their respective schemas:

```python
sql = "category IN ('A', 'B') AND value > 100.0 AND timestamp >= '2023-01-01'"

pa_filter = sql2pyarrow_filter(sql, arrow_schema)
pl_filter = sql2polars_filter(sql, polars_schema)
```

This lets you keep filtering logic in one place (for example, in configuration)
and apply it to whichever engine processes the data.

## Supported SQL

The translator covers the common predicate surface. Each example below is a
valid input string:

- **Comparison**: `=`, `!=`, `>`, `>=`, `<`, `<=`, `BETWEEN`
- **Logical**: `AND`, `OR`, `NOT`, with parentheses
- **String**: `LIKE` (with `%` wildcards), `IN (...)`, `NOT IN (...)`
- **Nulls**: `IS NULL`, `IS NOT NULL`
- **Date and time**: comparison against timestamp literals, plus `YEAR()`,
  `MONTH()`, `DAY()`, `DATE()` functions

```python
filters = [
    "value BETWEEN 10 AND 100 OR category = 'SPECIAL'",
    "(name LIKE 'test%' OR name LIKE 'demo%') AND value > 50",
    "timestamp >= '2023-01-01' AND timestamp <= '2023-12-31'",
    "YEAR(timestamp) = 2023 AND MONTH(timestamp) = 6",
]
```

## Type-aware conversion

Literals are parsed and coerced to the schema's column type, so a comparison
against a timestamp column uses a timestamp value, and a comparison against an
integer column uses an integer. This avoids silent type-mismatch bugs.

## Related documentation

- [API Guide](../reference/api-guide.md) - SQL import selection.
- [Generated API: fsspeckit.sql.filters](../api/fsspeckit.sql.filters.md) -
  exact signatures.
- [Read and Write Datasets](read-and-write-datasets.md) - applying filters to
  dataset reads.
