"""Micro-benchmark for DuckDBParquetHandler.merge_parquet_dataset().

Generates a multi-million-row dataset, exercises the merge strategies, and
prints wall-clock timings so Task 4.7 ("Performance test with large datasets")
can be satisfied reproducibly.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import random
import tempfile
import time

import numpy as np
import pyarrow as pa

from fsspeckit.datasets import DuckDBParquetHandler


random.seed(7)
np.random.seed(7)


def _table(num_rows: int, id_start: int = 0) -> pa.Table:
    """Generate a synthetic orders table."""

    ids = np.arange(id_start, id_start + num_rows, dtype=np.int64)
    customer_ids = np.random.randint(1, 50_000, size=num_rows, dtype=np.int64)
    amounts = np.random.uniform(1.0, 500.0, size=num_rows)
    statuses = np.random.choice(["new", "processing", "complete"], size=num_rows)
    updated_at = np.random.randint(1_700_000_000, 1_701_000_000, size=num_rows)

    return pa.table(
        {
            "order_id": pa.array(ids),
            "customer_id": pa.array(customer_ids),
            "amount": pa.array(amounts).cast(pa.float64()),
            "status": pa.array(statuses),
            "updated_at": pa.array(updated_at).cast(pa.int64()),
        }
    )


@dataclass
class BenchmarkResult:
    strategy: str
    seconds: float
    inserted: int
    updated: int
    deleted: int


def run_benchmark(target_rows: int = 1_000_000, source_rows: int = 300_000) -> list[BenchmarkResult]:
    results: list[BenchmarkResult] = []
    target_table = _table(target_rows)

    # Source mixes ~70% updates and 30% new inserts
    update_count = int(source_rows * 0.7)
    new_count = source_rows - update_count
    update_ids = np.random.choice(target_rows, size=update_count, replace=False)
    updates = target_table.take(pa.array(update_ids))
    updates = updates.set_column(2, "amount", pa.array(np.random.uniform(10.0, 800.0, size=update_count)))
    new_records = _table(new_count, id_start=target_rows)
    source_table = pa.concat_tables([updates, new_records])

    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = Path(temp_dir) / "orders_dataset"
        with DuckDBParquetHandler() as handler:
            for strategy in ["upsert", "insert", "full_merge"]:
                handler.write_parquet_dataset(target_table, str(dataset_path), mode="overwrite")
                start = time.perf_counter()
                stats = handler.merge_parquet_dataset(
                    source=source_table,
                    target_path=str(dataset_path),
                    key_columns="order_id",
                    strategy=strategy,
                )
                duration = time.perf_counter() - start
                results.append(
                    BenchmarkResult(
                        strategy=strategy,
                        seconds=duration,
                        inserted=stats.get("inserted", 0),
                        updated=stats.get("updated", 0),
                        deleted=stats.get("deleted", 0),
                    )
                )
                print(f"{strategy.upper():>10s}: {duration:.2f}s | stats={stats}")
    return results


if __name__ == "__main__":
    run_benchmark()
