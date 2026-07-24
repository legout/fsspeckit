"""Tests for ``canonical_keys_from_table`` fast-path equivalence.

The helper must produce exactly what per-row ``canonical_key`` would, but with a
vectorized fast path for single, non-nullable, non-floating key columns.
"""

import math

import pyarrow as pa
import pytest

from fsspeckit.core.merge import canonical_key, canonical_keys_from_table


def _reference(table: pa.Table, key_cols: list[str]) -> list:
    n = len(key_cols)
    if n == 1:
        return [canonical_key(v, 1) for v in table.column(key_cols[0]).to_pylist()]
    cols = [table.column(c).to_pylist() for c in key_cols]
    return [canonical_key(row, n) for row in zip(*cols, strict=True)]


CASES = [
    (
        "single_int_fast",
        pa.table({"id": pa.array([1, 2, 3, 2, 1], pa.int64())}),
        ["id"],
    ),
    ("single_string_fast", pa.table({"id": pa.array(["a", "b", "a"])}), ["id"]),
    ("single_bool_fast", pa.table({"id": pa.array([True, False, True])}), ["id"]),
    (
        "single_float_no_nan_fallback",
        pa.table({"id": pa.array([1.0, 2.0, 3.0])}),
        ["id"],
    ),
    (
        "single_float_nan_fallback",
        pa.table({"id": pa.array([1.0, math.nan, 2.0, math.nan])}),
        ["id"],
    ),
    ("nullable_int_null_fallback", pa.table({"id": pa.array([1, None, 2, 1])}), ["id"]),
    (
        "composite_non_null",
        pa.table({"a": pa.array([1, 2, 1]), "b": pa.array(["x", "y", "x"])}),
        ["a", "b"],
    ),
    (
        "composite_with_null",
        pa.table({"a": pa.array([1, None, 1]), "b": pa.array(["x", "y", None])}),
        ["a", "b"],
    ),
    ("empty_single", pa.table({"id": pa.array([], pa.int64())}), ["id"]),
    (
        "empty_composite",
        pa.table({"a": pa.array([], pa.int64()), "b": pa.array([], pa.string())}),
        ["a", "b"],
    ),
]


@pytest.mark.parametrize("label,table,keys", CASES, ids=[c[0] for c in CASES])
def test_canonical_keys_from_table_matches_reference(label, table, keys):
    assert canonical_keys_from_table(table, keys) == _reference(table, keys)
