# Move Your Package Imports

This migration covers the import-path relocations introduced in fsspeckit
0.22.x. It targets code written against the immediately preceding public layout
(pre-0.22) and maps the supported legacy imports to their current canonical
locations.

For the dataset API migration (removed helper functions and handler method
changes), see [Upgrade from the Pre-refactor Dataset Module](dataset-module-refactor.md).
For the complete legacy-to-canonical mapping including renamed symbols, see
[Deprecation and Legacy Imports](../reference/legacy-imports.md).

## Scope

Pre-0.22 to 0.22.x. This is a bounded workflow migration for the immediately
preceding public layout, not an all-version compatibility promise.

## What moved

Several modules were relocated to reflect their true dependencies. The old
import paths still resolve through the `fsspeckit.utils` backwards-compatibility
facade, but they emit no long-term support promise. Use canonical imports in
all new and migrated code.

| Concept | Pre-0.22 location | 0.22.x canonical location |
|---------|-------------------|---------------------------|
| Schema utilities | `fsspeckit.common.schema` | `fsspeckit.datasets.schema` |
| Type conversion | `fsspeckit.common.types` | `fsspeckit.datasets.types` |
| Polars utilities | `fsspeckit.common.polars` | `fsspeckit.datasets.polars` |
| Parallel processing | `fsspeckit.common.misc` | `fsspeckit.common.parallel` |
| Filesystem sync | `fsspeckit.common.misc` | `fsspeckit.common.sync` |
| Partition helpers | `fsspeckit.common.misc` | `fsspeckit.common.partitions` |
| Logging | `fsspeckit.common` (misc) | `fsspeckit.common.logging` |

The `fsspeckit.common` package is now importable without any optional
dependencies installed. Schema, type, and polars utilities moved to
`fsspeckit.datasets`, where `pyarrow`, `numpy`, and `polars` are already
required.

## Migration examples

### Schema and type utilities

Before (pre-0.22):

```python
from fsspeckit.common.schema import cast_schema, opt_dtype
from fsspeckit.common.types import to_pyarrow_table, dict_to_dataframe
```

After (0.22.x):

```python
from fsspeckit.datasets.schema import cast_schema, opt_dtype as opt_dtype_pa
from fsspeckit.datasets.types import to_pyarrow_table, dict_to_dataframe
```

### Parallel processing and sync

Before (pre-0.22):

```python
from fsspeckit.common.misc import run_parallel, sync_dir, get_partitions_from_path
```

After (0.22.x):

```python
from fsspeckit.common.parallel import run_parallel
from fsspeckit.common.sync import sync_dir
from fsspeckit.common.partitions import get_partitions_from_path
```

### Logging

Before (pre-0.22):

```python
from fsspeckit.common import setup_logging
```

After (0.22.x):

```python
from fsspeckit.common.logging import setup_logging
```

### Dataset handlers

Before (pre-0.22):

```python
from fsspeckit.utils import DuckDBParquetHandler
handler = DuckDBParquetHandler()
```

After (0.22.x):

```python
from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection
conn = create_duckdb_connection()
io = DuckDBDatasetIO(conn)
```

For renamed dataset symbols (`DuckDBParquetHandler`,
`PyarrowDatasetHandler`, etc.), see the table in
[Deprecation and Legacy Imports](../reference/legacy-imports.md).

## Complete example

Before (pre-0.22):

```python
from fsspeckit.common.schema import cast_schema
from fsspeckit.common.misc import run_parallel, sync_dir
from fsspeckit.common import setup_logging
from fsspeckit.utils import DuckDBParquetHandler

setup_logging(level="INFO")
results = run_parallel(process_item, items)
```

After (0.22.x):

```python
from fsspeckit.datasets.schema import cast_schema
from fsspeckit.common.parallel import run_parallel
from fsspeckit.common.sync import sync_dir
from fsspeckit.common.logging import setup_logging
from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection

setup_logging(level="INFO")
results = run_parallel(process_item, items)
```

## Troubleshooting

**`ImportError` after migration**: verify the canonical path exists in the
table above. The most common cause is importing from a module that has been
split: `common.misc` is now three separate modules
(`common.parallel`, `common.sync`, `common.partitions`).

**`fsspeckit.common` fails to import**: in 0.22.x, `fsspeckit.common` has no
hard dependency on `pyarrow` or `numpy`. If you need schema or type utilities,
import from `fsspeckit.datasets` instead.

## Legacy imports still work

The `fsspeckit.utils` facade re-exports the old names so existing code keeps
running. The facade is not the canonical location; do not use it in new code.
For the full facade mapping, see
[Deprecation and Legacy Imports](../reference/legacy-imports.md).
