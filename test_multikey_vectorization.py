#!/usr/bin/env python3
"""
Simple test to verify PyArrow multi-key vectorization implementation.
This test doesn't require the full package installation.
"""

import sys
import os

# Add the src directory to the path so we can import the modules directly
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

try:
    import pyarrow as pa
    import pyarrow.compute as pc
    from fsspeckit.datasets.pyarrow.dataset import (
        _create_composite_key_array,
        _filter_by_key_membership,
        _create_fallback_key_array,
        _table_drop_duplicates,
    )

    print("✓ Successfully imported PyArrow and helper functions")
except ImportError as e:
    print(f"✗ Import error: {e}")
    sys.exit(1)


def test_composite_key_array():
    """Test _create_composite_key_array function."""
    print("\nTesting _create_composite_key_array...")

    # Create test table with multi-column keys
    data = {
        "tenant_id": [1, 1, 2, 2, 3],
        "record_id": [10, 11, 10, 12, 10],
        "value": ["a", "b", "c", "d", "e"],
    }
    table = pa.table(data)

    # Test single column
    single_key = _create_composite_key_array(table, ["tenant_id"])
    assert single_key.type == pa.int64()
    print("✓ Single column key creation works")

    # Test multi-column
    multi_key = _create_composite_key_array(table, ["tenant_id", "record_id"])
    assert isinstance(multi_key, pa.StructArray)
    print("✓ Multi-column key creation works")


def test_fallback_key_array():
    """Test _create_fallback_key_array function."""
    print("\nTesting _create_fallback_key_array...")

    data = {
        "tenant_id": [1, 1, 2, 2, 3],
        "record_id": [10, 11, 10, 12, 10],
        "value": ["a", "b", "c", "d", "e"],
    }
    table = pa.table(data)

    fallback_keys = _create_fallback_key_array(table, ["tenant_id", "record_id"])
    assert isinstance(fallback_keys, (pa.BinaryArray, pa.StringArray))
    print("✓ Fallback key creation works")


def test_filter_by_key_membership():
    """Test _filter_by_key_membership function."""
    print("\nTesting _filter_by_key_membership...")

    # Create test data
    data1 = {
        "tenant_id": [1, 1, 2, 2, 3, 3],
        "record_id": [10, 11, 10, 12, 10, 11],
        "value": ["a", "b", "c", "d", "e", "f"],
    }
    table = pa.table(data1)

    # Create reference keys (subset of the data)
    ref_data = {"tenant_id": [1, 2], "record_id": [10, 12]}
    ref_table = pa.table(ref_data)

    # Test filtering to keep matches
    filtered = _filter_by_key_membership(
        table, ["tenant_id", "record_id"], ref_table, keep_matches=True
    )
    assert filtered.num_rows == 2  # Should keep rows with (1,10), (2,12)
    print("✓ Filter by key membership (keep matches) works")

    # Test filtering to keep non-matches
    filtered = _filter_by_key_membership(
        table, ["tenant_id", "record_id"], ref_table, keep_matches=False
    )
    assert (
        filtered.num_rows == 4
    )  # Should keep rows with (1,11), (2,10), (3,10), (3,11)
    print("✓ Filter by key membership (keep non-matches) works")


def test_table_drop_duplicates():
    """Test _table_drop_duplicates function."""
    print("\nTesting _table_drop_duplicates...")

    data = {
        "tenant_id": [1, 1, 2, 2, 3],
        "record_id": [10, 11, 10, 12, 10],
        "value": ["a", "b", "c", "d", "e"],
    }
    table = pa.table(data)

    deduped = _table_drop_duplicates(table, ["tenant_id", "record_id"])
    assert deduped.num_rows == 5  # All rows are unique
    print("✓ Table drop duplicates works")

    # Test with actual duplicates
    data_dup = {
        "tenant_id": [1, 1, 1, 2],
        "record_id": [10, 10, 11, 10],
        "value": ["a", "b", "c", "d"],
    }
    table_dup = pa.table(data_dup)

    deduped = _table_drop_duplicates(table_dup, ["tenant_id", "record_id"])
    assert deduped.num_rows == 3  # Should remove one duplicate
    print("✓ Table drop duplicates removes actual duplicates")


if __name__ == "__main__":
    print("Testing PyArrow Multi-Key Vectorization Implementation")
    print("=" * 60)

    try:
        test_composite_key_array()
        test_fallback_key_array()
        test_filter_by_key_membership()
        test_table_drop_duplicates()

        print("\n" + "=" * 60)
        print("✓ All tests passed! Multi-key vectorization implementation is working.")

    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
