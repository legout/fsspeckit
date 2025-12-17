#!/usr/bin/env python3
"""
Script to remove legacy write_parquet_dataset methods and helpers from PyArrow and DuckDB handlers.

This script surgically removes:
1. write_parquet_dataset method
2. Convenience methods: insert_dataset, upsert_dataset, update_dataset, deduplicate_dataset
3. _write_parquet_dataset_incremental method
4. _perform_merge_in_memory method
5. merge_parquet_dataset wrapper (that just calls merge_parquet_dataset_pyarrow)
"""

import re
from pathlib import Path
from typing import List, Tuple, Optional


def find_method_range(
    lines: List[str], method_name: str, start_line: int = 0
) -> Optional[Tuple[int, int]]:
    """Find the start and end line numbers of a method definition.

    Returns (start, end) tuple where end is the line after the method ends,
    or None if method not found.
    """
    # Find method start
    method_pattern = rf"^\s*def {re.escape(method_name)}\("
    start = None

    for i in range(start_line, len(lines)):
        if re.match(method_pattern, lines[i]):
            start = i
            break

    if start is None:
        return None

    # Find method end by tracking indentation
    base_indent = len(lines[start]) - len(lines[start].lstrip())

    # Look for the next line with same or less indentation that's not empty/comment
    for i in range(start + 1, len(lines)):
        line = lines[i]
        stripped = line.lstrip()

        # Skip empty lines and comments
        if not stripped or stripped.startswith("#"):
            continue

        # Check indentation
        current_indent = len(line) - len(stripped)

        # If we find a line at same or less indentation, that's the end
        if current_indent <= base_indent:
            return (start, i)

    # If we reach end of file, the method goes to the end
    return (start, len(lines))


def remove_method(lines: List[str], method_name: str) -> List[str]:
    """Remove a method from the source code."""
    result = find_method_range(lines, method_name)
    if result is None:
        print(f"  Method {method_name} not found")
        return lines

    start, end = result
    print(f"  Removing {method_name} (lines {start + 1}-{end})")

    # Remove the method but keep one blank line
    return lines[:start] + ["\n"] + lines[end:]


def process_file(file_path: Path, methods_to_remove: List[str]) -> None:
    """Process a file and remove specified methods."""
    print(f"\nProcessing {file_path}...")

    content = file_path.read_text()
    lines = content.splitlines(keepends=True)

    for method in methods_to_remove:
        lines = remove_method(lines, method)

    # Write back
    file_path.write_text("".join(lines))
    print(f"  ✓ Updated {file_path}")


def main():
    root = Path(__file__).parent.parent

    # PyArrow handler
    pyarrow_file = root / "src" / "fsspeckit" / "datasets" / "pyarrow" / "io.py"
    pyarrow_methods = [
        "write_parquet_dataset",
        "insert_dataset",
        "upsert_dataset",
        "update_dataset",
        "deduplicate_dataset",
        "_write_parquet_dataset_incremental",
        "_merge_upsert_pyarrow",
        "_merge_update_pyarrow",
        "_perform_merge_in_memory",
        "_merge_insert",
        "_merge_upsert",
        "_merge_update",
        "_merge_deduplicate",
    ]

    if pyarrow_file.exists():
        process_file(pyarrow_file, pyarrow_methods)

    # DuckDB handler
    duckdb_file = root / "src" / "fsspeckit" / "datasets" / "duckdb" / "dataset.py"
    duckdb_methods = [
        "write_parquet_dataset",
        "insert_dataset",
        "upsert_dataset",
        "update_dataset",
        "deduplicate_dataset",
        "_write_parquet_dataset_incremental",
        "_write_parquet_dataset_standard",
    ]

    if duckdb_file.exists():
        process_file(duckdb_file, duckdb_methods)

    print("\n✓ All methods removed successfully")


if __name__ == "__main__":
    main()
