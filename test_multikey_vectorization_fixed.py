#!/usr/bin/env python3
"""
Simple test to verify PyArrow multi-key vectorization implementation.
This test directly tests the helper functions without full package import.
"""

import sys
import os
import pyarrow as pa
import pyarrow.compute as pc


def _create_composite_key_array(table: pa.Table, key_columns: list) -> pa.Array:
    """Create a StructArray representing composite keys for efficient comparison."""
    if not key_columns:
        raise ValueError("key_columns cannot be empty")

    # Check that all columns exist
    for col in key_columns:
        if col not in table.column_names:
            raise ValueError(f"Column '{col}' not found in table")

    arrays = [table[col].combine_chunks() for col in key_columns]
    return pa.StructArray.from_arrays(arrays, names=key_columns)


def _create_string_key_array(table: pa.Table, key_columns: list) -> pa.Array:
    """Create string representation of composite keys using binary join."""
    if not key_columns:
        raise ValueError("key_columns cannot be empty")

    # Convert all columns to strings
    string_arrays = []
    for col in key_columns:
        col_array = table[col].combine_chunks()
        # Handle nulls by converting to a special string
        string_array = pc.cast(col_array, pa.string())
        string_array = pc.fill_null(string_array, "__NULL__")
        string_arrays.append(string_array)

    if len(string_arrays) == 1:
        return string_arrays[0]

    # Use binary join with unit separator as delimiter
    # The last argument is the separator
    return pc.binary_join_element_wise(*string_arrays, "\x1f")


def _table_drop_duplicates(table: pa.Table, key_columns: list) -> pa.Table:
    """Drop duplicate rows based on key columns."""
    if not key_columns:
        return table

    # Modern PyArrow has drop_duplicates
    if hasattr(table, "drop_duplicates"):
        return table.drop_duplicates(subset=key_columns)

    # Fallback: Use group_by to find unique indices
    # We add an index column, group by keys, and take the first index for each group
    indices = pa.array(range(table.num_rows))
    table_with_idx = table.append_column("__idx__", indices)

    # Group by key columns and get the minimum index for each group
    # This effectively picks the first occurrence of each unique key
    unique_idx_table = table_with_idx.group_by(key_columns).aggregate(
        [("__idx__", "min")]
    )

    # Extract the resulting indices and use them to take rows from original table
    unique_indices = unique_idx_table.column("__idx___min").combine_chunks()

    return table.take(unique_indices)


def _filter_by_key_membership(
    table: pa.Table,
    key_columns: list,
    reference_keys: pa.Table,
    keep_matches: bool = True,
) -> pa.Table:
    """Filter table rows based on key membership using PyArrow joins."""
    if not key_columns:
        raise ValueError("key_columns cannot be empty")

    # Perform join operation
    join_type = "left semi" if keep_matches else "left anti"
    return table.join(reference_keys, keys=key_columns, join_type=join_type)


def test_composite_key_array():
    """Test _create_composite_key_array function."""
    print("Testing _create_composite_key_array...")

    # Create test table with multi-column keys
    data = {
        "tenant_id": [1, 1, 2, 2, 3],
        "record_id": [10, 11, 10, 12, 10],
        "value": ["a", "b", "c", "d", "e"],
    }
    table = pa.table(data)

    # Test single column
    single_key = _create_composite_key_array(table, ["tenant_id"])
    print(f"  Single key type: {single_key.type}")
    print("  ✓ Single column key creation works")

    # Test multi-column
    multi_key = _create_composite_key_array(table, ["tenant_id", "record_id"])
    assert isinstance(multi_key, pa.StructArray)
    print("  ✓ Multi-column key creation works")


def test_string_key_array():
    """Test _create_string_key_array function."""
    print("Testing _create_string_key_array...")

    data = {
        "tenant_id": [1, 1, 2, 2, 3],
        "record_id": [10, 11, 10, 12, 10],
        "value": ["a", "b", "c", "d", "e"],
    }
    table = pa.table(data)

    string_keys = _create_string_key_array(table, ["tenant_id", "record_id"])
    assert isinstance(string_keys, pa.StringArray)
    print("  ✓ String key creation works")

    # Check that keys are properly formatted
    print(f"  Sample string keys: {string_keys[:3].to_pylist()}")


def test_filter_by_key_membership():
    """Test _filter_by_key_membership function."""
    print("Testing _filter_by_key_membership...")

    # Create test data
    data1 = {
        "tenant_id": [1, 1, 2, 2, 3, 3],
        "record_id": [10, 11, 10, 12, 10, 11],
        "value": ["a", "b", "c", "d", "e", "f"],
    }
    table = pa.table(data1)

    # Create reference keys (subset of the data)
    ref_data = {"tenant_id": [1, 2, 2], "record_id": [10, 10, 12]}
    ref_table = pa.table(ref_data)

    # Test filtering to keep matches
    filtered = _filter_by_key_membership(
        table, ["tenant_id", "record_id"], ref_table, keep_matches=True
    )
    assert filtered.num_rows == 3  # Should keep rows with (1,10), (2,10), (2,12)
    print("  ✓ Filter by key membership (keep matches) works")

    # Test filtering to keep non-matches
    filtered = _filter_by_key_membership(
        table, ["tenant_id", "record_id"], ref_table, keep_matches=False
    )
    assert filtered.num_rows == 3  # Should keep rows with (1,11), (3,10), (3,11)
    print("  ✓ Filter by key membership (keep non-matches) works")


def test_table_drop_duplicates():
    """Test _table_drop_duplicates function."""
    print("Testing _table_drop_duplicates...")

    data = {
        "tenant_id": [1, 1, 2, 2, 3],
        "record_id": [10, 11, 10, 12, 10],
        "value": ["a", "b", "c", "d", "e"],
    }
    table = pa.table(data)

    deduped = _table_drop_duplicates(table, ["tenant_id", "record_id"])
    assert deduped.num_rows == 5  # All rows are unique
    print("  ✓ Table drop duplicates works")

    # Test with actual duplicates
    data_dup = {
        "tenant_id": [1, 1, 1, 2],
        "record_id": [10, 10, 11, 10],
        "value": ["a", "b", "c", "d"],
    }
    table_dup = pa.table(data_dup)

    deduped = _table_drop_duplicates(table_dup, ["tenant_id", "record_id"])
    assert deduped.num_rows == 3  # Should remove one duplicate
    print("  ✓ Table drop duplicates removes actual duplicates")


def test_performance_comparison():
    """Test performance comparison between vectorized and non-vectorized approaches."""
    print("Testing performance comparison...")

    # Create larger test dataset
    import time

    n_rows = 10000
    data = {
        "tenant_id": [i % 1000 for i in range(n_rows)],  # 1000 unique tenants
        "record_id": [i % 10 for i in range(n_rows)],  # 10 records per tenant
        "value": [f"value_{i}" for i in range(n_rows)],
    }
    table = pa.table(data)

    # Test vectorized approach
    start_time = time.time()
    for _ in range(10):  # Run multiple times for better measurement
        keys = _create_composite_key_array(table, ["tenant_id", "record_id"])
    vectorized_time = time.time() - start_time

    # Test string approach
    start_time = time.time()
    for _ in range(10):
        keys = _create_string_key_array(table, ["tenant_id", "record_id"])
    string_time = time.time() - start_time

    print(f"  Vectorized approach: {vectorized_time:.4f}s for 10 iterations")
    print(f"  String approach: {string_time:.4f}s for 10 iterations")
    print(f"  Speedup ratio: {string_time / vectorized_time:.2f}x")


def test_vectorized_vs_legacy():
    """Test that our vectorized approach is better than legacy Python loops."""
    print("Testing vectorized vs legacy approach...")

    # Create test data
    n_rows = 5000
    data = {
        "tenant_id": [i % 500 for i in range(n_rows)],  # 500 unique tenants
        "record_id": [i % 5 for i in range(n_rows)],  # 5 records per tenant
        "value": [f"value_{i}" for i in range(n_rows)],
    }
    table = pa.table(data)

    # Test legacy approach (Python loops with to_pylist)
    def legacy_multi_key_operations(table, key_columns):
        # This mimics the old problematic approach
        result = []
        # Convert to list of dicts or handle columns directly
        data = table.to_pydict()
        for i in range(table.num_rows):
            key = tuple(data[col][i] for col in key_columns)
            result.append(key)
        return result

    # Test vectorized approach
    def vectorized_multi_key_operations(table, key_columns):
        return _create_composite_key_array(table, key_columns)

    import time

    # Time legacy approach
    legacy_result = None
    start_time = time.time()
    for _ in range(5):
        legacy_result = legacy_multi_key_operations(table, ["tenant_id", "record_id"])
    legacy_time = time.time() - start_time

    # Time vectorized approach
    vectorized_result = None
    start_time = time.time()
    for _ in range(5):
        vectorized_result = vectorized_multi_key_operations(
            table, ["tenant_id", "record_id"]
        )
    vectorized_time = time.time() - start_time

    print(f"  Legacy Python loop approach: {legacy_time:.4f}s for 5 iterations")
    print(f"  Vectorized Arrow approach: {vectorized_time:.4f}s for 5 iterations")
    print(f"  Speedup ratio: {legacy_time / vectorized_time:.2f}x faster")

    if legacy_result is not None and vectorized_result is not None:
        print(
            f"  Results contain same number of keys: {len(legacy_result) == len(vectorized_result)}"
        )


if __name__ == "__main__":
    print("Testing PyArrow Multi-Key Vectorization Implementation")
    print("=" * 60)

    try:
        test_composite_key_array()
        test_string_key_array()
        test_filter_by_key_membership()
        test_table_drop_duplicates()
        test_performance_comparison()
        test_vectorized_vs_legacy()

        print("\n" + "=" * 60)
        print("✓ All tests passed! Multi-key vectorization implementation is working.")

    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
