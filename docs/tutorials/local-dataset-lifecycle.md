# Local Dataset Lifecycle

This tutorial walks through the canonical fsspeckit workflow: configure a local
filesystem, write a small Parquet dataset, read it back, verify the result, and
clean up. It runs entirely offline. No cloud credentials, no remote services.

If you are new to fsspeckit, start here. Everything else in the documentation
builds on the three ideas in this script: a **filesystem**, a **dataset**, and a
**write**.

## Before you begin

You already manage a Python environment (a virtualenv, conda env, or similar)
with Python 3.11 or higher. Install fsspeckit with the `datasets` extra, which
provides the complete dataset workflow used in this tutorial:

```bash
pip install "fsspeckit[datasets]"
```

For the full extras matrix (cloud providers, DuckDB, SQL filters), see
[Installation and optional extras](../installation.md).

## The complete script

Copy this into a file and run it. It creates a single sandbox directory, writes
and reads a dataset inside it, asserts the round trip, then removes only that
sandbox.

```python
"""fsspeckit local dataset lifecycle: write, read back, verify, clean up.

Run offline. No cloud credentials required.
Requires: pip install "fsspeckit[datasets]"
"""
from __future__ import annotations

import shutil
from pathlib import Path

import pyarrow as pa

from fsspeckit import filesystem
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO

# 1. Named tutorial sandbox. We create it and remove only this directory.
SANDBOX = Path("fsspeckit_tutorial_sandbox")
DATASET_PATH = SANDBOX / "sensors"
SANDBOX.mkdir(parents=True, exist_ok=True)

# 2. Local filesystem. dirfs=False returns the raw LocalFileSystem that the
#    dataset I/O layer reads and writes through directly.
fs = filesystem("file", dirfs=False)

# 3. Minimal explicit schema. We fix column types up front so the written
#    Parquet is stable. This tutorial does not cover schema evolution.
schema = pa.schema([
    pa.field("sensor_id", pa.int64()),
    pa.field("reading", pa.float64()),
    pa.field("recorded_at", pa.string()),
])

# 4. Write the dataset. mode="overwrite" starts from a clean directory.
io = PyarrowDatasetIO(filesystem=fs)
records = pa.table(
    {
        "sensor_id": [1, 2, 3],
        "reading": [21.4, 22.1, 19.8],
        "recorded_at": [
            "2026-07-10T08:00",
            "2026-07-10T08:05",
            "2026-07-10T08:10",
        ],
    },
    schema=schema,
)
write_result = io.write_dataset(
    records, str(DATASET_PATH), schema=schema, mode="overwrite"
)
print(f"Wrote {write_result.total_rows} rows with the {write_result.backend} backend.")

# 5. Read the dataset back into a single PyArrow table.
table = io.read_parquet(str(DATASET_PATH))

# 6. Assert the read-back data matches what we wrote.
assert table.num_rows == write_result.total_rows
assert table.column_names == ["sensor_id", "reading", "recorded_at"]
assert table.column("sensor_id").to_pylist() == [1, 2, 3]
print(f"Read back {table.num_rows} rows. Local dataset lifecycle verified.")

# 7. Clean up ONLY the named sandbox directory.
shutil.rmtree(SANDBOX)
print(f"Removed sandbox: {SANDBOX}")
```

Expected output:

```text
Wrote 3 rows with the pyarrow backend.
Read back 3 rows. Local dataset lifecycle verified.
Removed sandbox: fsspeckit_tutorial_sandbox
```

## Walkthrough

### 1. The sandbox

The script writes into one named directory, `fsspeckit_tutorial_sandbox/`, and
removes exactly that directory at the end. Nothing outside the sandbox is
touched. Using a dedicated directory keeps the tutorial reproducible and makes
cleanup safe.

### 2. The filesystem

`filesystem("file", dirfs=False)` returns the raw local filesystem. The dataset
I/O layer reads and writes through this filesystem object, so the same code
path works unchanged when you later swap in a cloud backend. The `dirfs=False`
argument asks for the raw `LocalFileSystem` rather than a directory-confined
view, which is what the dataset handlers expect.

### 3. The schema

A `pa.schema` fixes the column names and types stored in the Parquet files.
Declaring it explicitly keeps the written data stable across runs. This
tutorial deliberately stops there: schema evolution, validation, and
unification are separate topics covered in the reference material.

### 4. Writing the dataset

`PyarrowDatasetIO` is the handler for this tutorial. You pass it the
filesystem, then call `write_dataset` with the data, the target directory, the
schema, and a mode. `mode="overwrite"` clears any existing Parquet files in the
target first, so the script is safe to re-run.

The call returns a `WriteDatasetResult` with the row count, the backend name,
and per-file metadata. For the exact fields, see
[Dataset Handlers](../dataset-handlers.md).

### 5. Reading it back

`read_parquet` reads the whole dataset directory into one PyArrow table. The
same method works on a single file path.

### 6. Verifying the round trip

The assertions check row count, column order, and one column's values. They
make the success of the lifecycle explicit rather than relying on a print
statement alone.

### 7. Cleanup

`shutil.rmtree(SANDBOX)` removes only the sandbox directory the script created.
It does not touch anything else on your machine.

## When to write versus when to merge

This tutorial writes a fresh dataset each run. That is the right starting point:
a plain `write_dataset` call is enough whenever you are creating or replacing a
dataset in full.

Merging is for a different situation: updating an existing dataset in place by
inserting, updating, or upserting only the rows that changed. That is an
incremental operation with its own strategies and tradeoffs. When you need it,
follow the [Merge Datasets](../how-to/merge-datasets.md) how-to instead of
adapting this script.

## Where to go next

Now that you have a working local lifecycle, these routes take you further:

- **Diagnose and reference**: [API Guide](../reference/api-guide.md) for
  choosing imports and backends; [Public API Inventory](../reference/public-api-inventory.md)
  for the full list of supported symbols; [Dataset Handlers](../dataset-handlers.md)
  for the shared write, read, and maintenance interface.
- **Concepts**: [Key Concepts](../explanation/concepts.md) for the import
  hierarchy, path safety, and the package architecture.
- **Incremental updates**: [Merge Datasets](../how-to/merge-datasets.md) for
  insert, update, and upsert strategies.
- **Migration**: [Upgrade from the Pre-refactor Layout](../migration/dataset-module-refactor.md)
  if you are moving older code to the current domain-package imports.
- **Optional integrations**: [Configure Cloud Storage](../how-to/configure-cloud-storage.md)
  to point the same handlers at S3, GCS, or Azure; [Use SQL Filters](../how-to/use-sql-filters.md)
  to push filters into reads; [Installation and optional extras](../installation.md)
  for the complete extras matrix.
