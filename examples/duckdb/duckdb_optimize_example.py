"""DuckDB dataset maintenance: z-order style optimization workflows."""

from __future__ import annotations

import random
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc

from fsspeckit.datasets import DuckDBParquetHandler


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
            days = sorted(set(table.column("event_date").to_pylist()))[:3]
            for day in days:
                mask = pc.equal(table.column("event_date"), day)
                subset = table.filter(mask)
                handler.write_parquet_dataset(
                    subset,
                    str(dataset_path / f"event_date={day}"),
                    mode="overwrite",
                    max_rows_per_file=100,
                    basename_template=f"events-{day}-{{}}.parquet",
                )

            print("Dataset seeded with", table.num_rows, "events across", len(days), "partitions")

            dry = handler.optimize_parquet_dataset(
                path=str(dataset_path),
                zorder_columns=["user_id", "event_date"],
                dry_run=True,
            )
            print("\nDry-run stats:")
            print(" before_file_count", dry["before_file_count"])
            print(" projected clustering sample", dry["planned_groups"][0])

            scoped = handler.optimize_parquet_dataset(
                path=str(dataset_path),
                zorder_columns=["user_id"],
                partition_filter=[f"event_date={days[0]}"],
                dry_run=True,
            )
            print("\nPartition-scoped dry-run:")
            print(scoped["planned_groups"][:1])

            stats = handler.optimize_parquet_dataset(
                path=str(dataset_path),
                zorder_columns=["user_id", "event_date"],
                target_mb_per_file=8,
            )
            print("\nOptimization complete:")
            print(
                f"{stats['before_file_count']} files -> {stats['after_file_count']} files"
            )

if __name__ == "__main__":
    run_optimize_example()
