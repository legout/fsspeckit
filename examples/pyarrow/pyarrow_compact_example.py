"""Compact parquet datasets with PyArrow-only helpers.

This script mirrors the DuckDB maintenance examples but relies purely on
`collect_dataset_stats_pyarrow` and `compact_parquet_dataset_pyarrow`. It keeps
all reads filtered to the target partition so we never materialize the entire
dataset with `dataset.to_table()`.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from fsspeckit.datasets import (
    collect_dataset_stats_pyarrow,
    compact_parquet_dataset_pyarrow,
)


def bootstrap_dataset(dataset_path: Path) -> None:
    """Create a tiny partitioned dataset for demo purposes."""

    if any(dataset_path.rglob("*.parquet")):
        return

    for partition in ("date=2025-11-15", "date=2025-11-16"):
        part_dir = dataset_path / partition
        part_dir.mkdir(parents=True, exist_ok=True)
        for idx in range(3):
            table = pa.table(
                {
                    "date": [partition.split("=")[1]] * 20,
                    "user_id": list(range(idx * 20, idx * 20 + 20)),
                    "amount": [idx] * 20,
                }
            )
            pq.write_table(table, part_dir / f"part-{idx:02d}.parquet")


def main() -> None:
    dataset_path = Path("var/examples/pyarrow_compaction")
    dataset_path.mkdir(parents=True, exist_ok=True)
    bootstrap_dataset(dataset_path)

    hot_partition = "date=2025-11-15"
    stats = collect_dataset_stats_pyarrow(
        str(dataset_path), partition_filter=[hot_partition]
    )
    print(
        f"Hot partition has {len(stats['files'])} files totaling {stats['total_bytes']} bytes"
    )

    plan = compact_parquet_dataset_pyarrow(
        str(dataset_path),
        target_rows_per_file=60,
        compression="zstd",
        partition_filter=[hot_partition],
        dry_run=True,
    )
    print("Dry-run plan:", plan["planned_groups"])

    live = compact_parquet_dataset_pyarrow(
        str(dataset_path),
        target_rows_per_file=60,
        compression="zstd",
        partition_filter=[hot_partition],
    )
    print(
        "Compaction reduced file count from"
        f" {live['before_file_count']} to {live['after_file_count']}"
    )

    dataset = ds.dataset(dataset_path)
    filtered = dataset.to_table(filter=ds.field("date") == "2025-11-15")
    print("Filtered rows materialized:", filtered.num_rows)


if __name__ == "__main__":
    main()
