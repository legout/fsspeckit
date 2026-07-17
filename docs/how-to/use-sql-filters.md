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
sql = "(category = 'A' OR category = 'B') AND value > 100.0 AND timestamp >= '2023-01-01'"

pa_filter = sql2pyarrow_filter(sql, arrow_schema)
pl_filter = sql2polars_filter(sql, polars_schema)
```

Note the `OR` chain instead of `IN`: `IN (...)` is only supported by the
PyArrow translator (see the support matrix below).

This lets you keep filtering logic in one place (for example, in configuration)
and apply it to whichever engine processes the data.

## Supported SQL

The translator covers a deliberately small predicate surface. Unsupported
constructs raise `ValueError` at translation time instead of silently
mis-filtering. The matrix below is verified against the shipped translators:

| Construct | PyArrow | Polars |
| --- | --- | --- |
| `=`, `!=`, `>`, `>=`, `<`, `<=` | ✅ | ✅ |
| `AND`, `OR`, `NOT`, parentheses | ✅ | ✅ |
| `IS NULL`, `IS NOT NULL` | ✅ | ✅ |
| Timestamp literals (`timestamp >= '2023-01-01'`) | ✅ | ✅ |
| `IN (...)`, `NOT IN (...)` | ✅ | ❌ |
| Boolean literals (`active = TRUE`) | ✅ | ❌ |
| `BETWEEN`, `LIKE` | ❌ | ❌ |
| Function calls (`YEAR()`, `DATE()`, `UPPER()`, ...) | ❌ | ❌ |
| Arithmetic (`value * 1.1 > 100`) | ❌ | ❌ |

```python
filters = [
    "value >= 10 AND value <= 100 OR category = 'SPECIAL'",  # BETWEEN rewritten
    "timestamp >= '2023-01-01' AND timestamp <= '2023-12-31'",
    "category IN ('a', 'b') AND value > 50",                  # PyArrow only
]
```

Rewrite unsupported constructs in terms of supported ones:

- `value BETWEEN 10 AND 100` -> `value >= 10 AND value <= 100`
- `category IN ('a', 'b')` -> `category = 'a' OR category = 'b'` (for Polars)
- `YEAR(ts) = 2023` -> `ts >= '2023-01-01' AND ts < '2024-01-01'`
- `name LIKE 'test%'` has no translator equivalent; filter natively in the
  engine after the scan.

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
