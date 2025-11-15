"""DuckDB dataset maintenance: z-order style optimization workflows."""

from __future__ import annotations

import random
import tempfile
from pathlib import Path

import pyarrow as pa

from fsspeckit.utils import DuckDBParquetHandler


def _build_events_table(seed: int) -> pa.Table:
    random.seed(seed)
    return pa.table(
        {
            "user_id": [random.randint(1, 100) for _ in range(1000)],
            "event_date": [f"2025-11-{random.randint(1, 30):02d}" for _ in range(1000)],
            "event_type": [random.choice(["view", "click", "purchase"]) for _ in range(1000)],
            "amount": [round(random.random() * 250, 2) for _ in range(1000)],
        }
    )


def run_optimize_example() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        dataset_path = Path(temp_dir) / "events"
        with DuckDBParquetHandler() as handler:
            table = _build_events_table(seed=42)
            handler.write_parquet_dataset(
                table,
                str(dataset_path),
                max_rows_per_file=100,
                basename_template="events-{}.parquet",
            )

            print("Dataset seeded with", table.num_rows, "events")

            dry = handler.optimize_parquet_dataset(
                path=str(dataset_path),
                zorder_columns=["user_id", "event_date"],
                dry_run=True,
            )
            print("\nDry-run stats:")
            print(" before", dry["before"])
            print(" projected clustering sample", dry["plan"][0])

            stats = handler.optimize_parquet_dataset(
                path=str(dataset_path),
                zorder_columns=["user_id", "event_date"],
                target_mb_per_file=8,
            )
            print("\nOptimization complete:")
            print(stats["before"], "->", stats["after"])

            scoped = handler.optimize_parquet_dataset(
                path=str(dataset_path),
                zorder_columns=["user_id"],
                partition_filter=["event_date=2025-11-15"],
                dry_run=True,
            )
            print("\nPartition-scoped dry-run:")
            print(scoped["plan"][:1])

if __name__ == "__main__":
    run_optimize_example()
