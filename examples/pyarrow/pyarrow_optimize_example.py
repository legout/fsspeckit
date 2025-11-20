"""Optimize parquet datasets with PyArrow-only helpers.

`optimize_parquet_dataset_pyarrow` can recluster a subset of files ordered by
`zorder_columns` without introducing a DuckDB dependency. This script keeps all
materialization scoped via filters so large datasets remain streaming-friendly.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from fsspeckit.datasets import optimize_parquet_dataset_pyarrow


def bootstrap_dataset(dataset_path: Path) -> None:
    """Create a sample dataset with lightly shuffled partitions."""

    if any(dataset_path.rglob("*.parquet")):
        return

    for partition in ("date=2025-11-15", "date=2025-11-16"):
        part_dir = dataset_path / partition
        part_dir.mkdir(parents=True, exist_ok=True)
        for idx in range(2):
            table = pa.table(
                {
                    "date": [partition.split("=")[1]] * 10,
                    "user_id": list(reversed(range(idx * 10, idx * 10 + 10))),
                    "event_id": list(range(idx * 10, idx * 10 + 10)),
                }
            )
            pq.write_table(table, part_dir / f"part-{idx:02d}.parquet")


def main() -> None:
    dataset_path = Path("var/examples/pyarrow_optimize")
    dataset_path.mkdir(parents=True, exist_ok=True)
    bootstrap_dataset(dataset_path)

    hot_partition = "date=2025-11-15"
    plan = optimize_parquet_dataset_pyarrow(
        str(dataset_path),
        zorder_columns=["date", "user_id"],
        target_rows_per_file=50,
        partition_filter=[hot_partition],
        dry_run=True,
    )
    print("Dry-run optimized groups:", plan["planned_groups"])

    live = optimize_parquet_dataset_pyarrow(
        str(dataset_path),
        zorder_columns=["date", "user_id"],
        target_rows_per_file=50,
        partition_filter=[hot_partition],
    )
    print(
        "Optimization rewrote",
        live["compacted_file_count"],
        "files ordered by",
        live["zorder_columns"],
    )

    dataset = ds.dataset(dataset_path)
    filtered = dataset.to_table(filter=ds.field("date") == "2025-11-15")
    values = list(
        zip(
            filtered.column("date").to_pylist(),
            filtered.column("user_id").to_pylist(),
        )
    )
    print("First 5 ordered rows:", values[:5])


if __name__ == "__main__":
    main()
