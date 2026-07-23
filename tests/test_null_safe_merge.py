"""Null-safe (IS NOT DISTINCT FROM) merge key tests for both backends.

These tests verify the null-equal key identity contract described in #64:
NULL matches NULL, NULL does not match any non-null value, and composite keys
match only when every component matches. Both ``PyarrowDatasetIO`` and
``DuckDBDatasetIO`` must produce identical results.
"""

from __future__ import annotations

import pyarrow as pa
import math

import pytest

from fsspeckit.common.optional import _DUCKDB_AVAILABLE
from fsspeckit.datasets.pyarrow import PyarrowDatasetIO

# DuckDB is imported unconditionally; tests that need it are skipped via
# BACKENDS / pytestmark when the dependency is absent.
try:
    from fsspeckit.datasets.duckdb import DuckDBDatasetIO, create_duckdb_connection
except ImportError:  # pragma: no cover
    DuckDBDatasetIO = None  # type: ignore[assignment,misc]
    create_duckdb_connection = None  # type: ignore[assignment]


def _read_dataset(path: str) -> pa.Table:
    import pyarrow.dataset as ds

    return ds.dataset(path).to_table()


# ---------------------------------------------------------------------------
# Backend fixtures
# ---------------------------------------------------------------------------
BACKENDS = ["pyarrow"]
if _DUCKDB_AVAILABLE:
    BACKENDS.append("duckdb")


@pytest.fixture
def io_factory(tmp_path):
    """Return a factory producing an IO instance per backend."""

    def _make(backend: str):
        if backend == "pyarrow":
            return PyarrowDatasetIO()
        assert create_duckdb_connection is not None and DuckDBDatasetIO is not None
        conn = create_duckdb_connection()
        return DuckDBDatasetIO(conn)

    return _make


def _write_target(io, path: str, table: pa.Table) -> None:
    io.write_dataset(table, path, mode="overwrite")


# ---------------------------------------------------------------------------
# Single nullable key
# ---------------------------------------------------------------------------
class TestSingleNullableKey:
    """Single nullable key: null matches an existing null."""

    @pytest.mark.parametrize("backend", BACKENDS)
    def test_null_matches_existing_null_upsert(self, tmp_path, io_factory, backend):
        io = io_factory(backend)
        path = str(tmp_path / "ds")
        target = pa.table(
            {"id": pa.array([1, 2, None], type=pa.int64()), "v": ["a", "b", "n"]}
        )
        _write_target(io, path, target)

        source = pa.table(
            {"id": pa.array([None, 3], type=pa.int64()), "v": ["N2", "c"]}
        )
        result = io.merge(source, path, strategy="upsert", key_columns=["id"])

        assert result.updated == 1  # None matched None
        assert result.inserted == 1  # 3 is new

        final = _read_dataset(path).to_pydict()
        # Build lookup dict safely (None key)
        rows = dict(zip(final["id"], final["v"], strict=True))
        assert rows[None] == "N2"
        assert rows[3] == "c"
        assert rows[1] == "a"

    @pytest.mark.parametrize("backend", BACKENDS)
    def test_null_matches_existing_null_insert(self, tmp_path, io_factory, backend):
        io = io_factory(backend)
        path = str(tmp_path / "ds")
        target = pa.table({"id": pa.array([1, None], type=pa.int64()), "v": ["a", "n"]})
        _write_target(io, path, target)

        source = pa.table(
            {"id": pa.array([None, 5], type=pa.int64()), "v": ["N2", "e"]}
        )
        result = io.merge(source, path, strategy="insert", key_columns=["id"])

        # None already exists -> not inserted; 5 is new -> inserted
        assert result.inserted == 1
        rows = dict(
            zip(
                _read_dataset(path).column("id").to_pylist(),
                _read_dataset(path).column("v").to_pylist(),
                strict=True,
            )
        )
        assert rows[None] == "n"  # preserved original
        assert rows[5] == "e"

    @pytest.mark.parametrize("backend", BACKENDS)
    def test_null_matches_existing_null_update(self, tmp_path, io_factory, backend):
        io = io_factory(backend)
        path = str(tmp_path / "ds")
        target = pa.table({"id": pa.array([1, None], type=pa.int64()), "v": ["a", "n"]})
        _write_target(io, path, target)

        source = pa.table({"id": pa.array([None], type=pa.int64()), "v": ["UPDATED"]})
        result = io.merge(source, path, strategy="update", key_columns=["id"])

        assert result.updated == 1
        rows = dict(
            zip(
                _read_dataset(path).column("id").to_pylist(),
                _read_dataset(path).column("v").to_pylist(),
                strict=True,
            )
        )
        assert rows[None] == "UPDATED"


# ---------------------------------------------------------------------------
# Composite key with null component
# ---------------------------------------------------------------------------
class TestCompositeNullableKey:
    """(id, NULL) differs from (id, "abc") and is inserted."""

    @pytest.mark.parametrize("backend", BACKENDS)
    def test_null_component_distinct_from_value(self, tmp_path, io_factory, backend):
        io = io_factory(backend)
        path = str(tmp_path / "ds")
        target = pa.table(
            {
                "id": pa.array([121221, 121221], type=pa.int64()),
                "value": pa.array(["abc", None], type=pa.string()),
                "data": ["existing", "existing_null"],
            }
        )
        _write_target(io, path, target)

        # Source (121221, NULL) matches target (121221, NULL) -> update.
        # Source (121221, "xyz") is new -> insert.
        source = pa.table(
            {
                "id": pa.array([121221, 121221], type=pa.int64()),
                "value": pa.array([None, "xyz"], type=pa.string()),
                "data": ["updated_null", "new_value"],
            }
        )
        result = io.merge(source, path, strategy="upsert", key_columns=["id", "value"])

        assert result.updated == 1  # (121221, NULL) matched
        assert result.inserted == 1  # (121221, "xyz") is new

        final = _read_dataset(path).to_pylist()
        lookup = {(r["id"], r["value"]): r["data"] for r in final}
        null_key = (121221, None)
        new_key = (121221, "xyz")
        abc_key = (121221, "abc")
        assert lookup[null_key] == "updated_null"
        assert lookup[new_key] == "new_value"
        assert lookup[abc_key] == "existing"

    @pytest.mark.parametrize("backend", BACKENDS)
    def test_null_component_is_inserted_when_only_non_null_variant_exists(
        self, tmp_path, io_factory, backend
    ):
        """Issue #64 acceptance case: (id, NULL) differs from (id, "abc")."""
        io = io_factory(backend)
        path = str(tmp_path / "ds")
        _write_target(
            io,
            path,
            pa.table(
                {
                    "id": pa.array([121221], type=pa.int64()),
                    "value": pa.array(["abc"], type=pa.string()),
                    "data": ["existing"],
                }
            ),
        )

        source = pa.table(
            {
                "id": pa.array([121221], type=pa.int64()),
                "value": pa.array([None], type=pa.string()),
                "data": ["inserted_null"],
            }
        )
        result = io.merge(source, path, strategy="upsert", key_columns=["id", "value"])

        assert result.updated == 0
        assert result.inserted == 1
        lookup = {
            (row["id"], row["value"]): row["data"]
            for row in _read_dataset(path).to_pylist()
        }
        abc_key = (121221, "abc")
        null_key = (121221, None)
        assert lookup[abc_key] == "existing"
        assert lookup[null_key] == "inserted_null"

    @pytest.mark.parametrize("backend", BACKENDS)
    def test_repeated_null_key_not_inserted_twice(self, tmp_path, io_factory, backend):
        io = io_factory(backend)
        path = str(tmp_path / "ds")
        target = pa.table(
            {
                "id": pa.array([1], type=pa.int64()),
                "value": pa.array([None], type=pa.string()),
                "data": ["original"],
            }
        )
        _write_target(io, path, target)

        # Source has the same null key twice (dedup last-wins) + a new key.
        source = pa.table(
            {
                "id": pa.array([1, 1, 2], type=pa.int64()),
                "value": pa.array([None, None, None], type=pa.string()),
                "data": ["dup1", "dup2_last", "new"],
            }
        )
        result = io.merge(source, path, strategy="upsert", key_columns=["id", "value"])

        # (1, None) deduped to last ("dup2_last") -> update. (2, None) new -> insert.
        assert result.updated == 1
        assert result.inserted == 1

        final = _read_dataset(path).to_pylist()
        lookup = {(r["id"], r["value"]): r["data"] for r in final}
        key1 = (1, None)
        key2 = (2, None)
        assert lookup[key1] == "dup2_last"
        assert lookup[key2] == "new"
        # No duplicate (1, None) rows
        null_key_rows = [r for r in final if r["id"] == 1 and r["value"] is None]
        assert len(null_key_rows) == 1

    @pytest.mark.parametrize("backend", BACKENDS)
    def test_multiple_null_components(self, tmp_path, io_factory, backend):
        """Multiple nullable components in a composite key."""
        io = io_factory(backend)
        path = str(tmp_path / "ds")
        target = pa.table(
            {
                "a": pa.array([1, 1], type=pa.int64()),
                "b": pa.array([None, None], type=pa.int64()),
                "c": pa.array([None, 5], type=pa.int64()),
                "v": ["r1", "r2"],
            }
        )
        _write_target(io, path, target)

        # (1, None, None) matches -> update. (1, None, 5) matches -> update.
        source = pa.table(
            {
                "a": pa.array([1, 1], type=pa.int64()),
                "b": pa.array([None, None], type=pa.int64()),
                "c": pa.array([None, 5], type=pa.int64()),
                "v": ["U1", "U2"],
            }
        )
        result = io.merge(source, path, strategy="upsert", key_columns=["a", "b", "c"])
        assert result.updated == 2
        assert result.inserted == 0

        final = _read_dataset(path).to_pylist()
        lookup = {(r["a"], r["b"], r["c"]): r["v"] for r in final}
        k1 = (1, None, None)
        k2 = (1, None, 5)
        assert lookup[k1] == "U1"
        assert lookup[k2] == "U2"


# ---------------------------------------------------------------------------
# Mixed null and non-null keys in one batch
# ---------------------------------------------------------------------------
class TestMixedKeys:
    @pytest.mark.parametrize("backend", BACKENDS)
    def test_mixed_null_and_non_null(self, tmp_path, io_factory, backend):
        io = io_factory(backend)
        path = str(tmp_path / "ds")
        target = pa.table(
            {
                "id": pa.array([1, None, 3], type=pa.int64()),
                "v": ["a", "n", "c"],
            }
        )
        _write_target(io, path, target)

        source = pa.table(
            {
                "id": pa.array([1, None, 3, 4], type=pa.int64()),
                "v": ["A", "N", "C", "d"],
            }
        )
        result = io.merge(source, path, strategy="upsert", key_columns=["id"])
        assert result.updated == 3  # 1, None, 3 all matched
        assert result.inserted == 1  # 4 is new

        rows = dict(
            zip(
                _read_dataset(path).column("id").to_pylist(),
                _read_dataset(path).column("v").to_pylist(),
                strict=True,
            )
        )
        assert rows[1] == "A"
        assert rows[None] == "N"
        assert rows[3] == "C"
        assert rows[4] == "d"


# ---------------------------------------------------------------------------
# Source duplicate nullable keys resolve last-row-wins
# ---------------------------------------------------------------------------
class TestSourceDedupLastWins:
    @pytest.mark.parametrize("backend", BACKENDS)
    def test_duplicate_null_key_last_wins(self, tmp_path, io_factory, backend):
        io = io_factory(backend)
        path = str(tmp_path / "ds")
        _write_target(
            io, path, pa.table({"id": pa.array([1], type=pa.int64()), "v": ["a"]})
        )

        source = pa.table(
            {
                "id": pa.array([None, None], type=pa.int64()),
                "v": ["first", "second"],
            }
        )
        result = io.merge(source, path, strategy="upsert", key_columns=["id"])
        assert result.inserted == 1
        rows = dict(
            zip(
                _read_dataset(path).column("id").to_pylist(),
                _read_dataset(path).column("v").to_pylist(),
                strict=True,
            )
        )
        assert rows[None] == "second"  # last-row-wins


# ---------------------------------------------------------------------------
# Nullable keys in existing target files found and rewritten
# ---------------------------------------------------------------------------
class TestRewriteNullableTargetKeys:
    @pytest.mark.parametrize("backend", BACKENDS)
    def test_nullable_target_key_rewritten(self, tmp_path, io_factory, backend):
        io = io_factory(backend)
        path = str(tmp_path / "ds")
        # Target spread across multiple files, one with a null key.
        target = pa.table({"id": pa.array([1, None], type=pa.int64()), "v": ["a", "n"]})
        _write_target(io, path, target)

        source = pa.table({"id": pa.array([None], type=pa.int64()), "v": ["REWRITTEN"]})
        result = io.merge(source, path, strategy="upsert", key_columns=["id"])
        assert result.updated == 1
        assert len(result.rewritten_files) >= 1
        rows = dict(
            zip(
                _read_dataset(path).column("id").to_pylist(),
                _read_dataset(path).column("v").to_pylist(),
                strict=True,
            )
        )
        assert rows[None] == "REWRITTEN"

    @pytest.mark.parametrize("backend", BACKENDS)
    def test_partition_pruning_keeps_only_null_key_partition_candidate(
        self, tmp_path, io_factory, backend
    ):
        io = io_factory(backend)
        path = str(tmp_path / "ds")
        target = pa.table(
            {
                "id": pa.array([None, 2], type=pa.int64()),
                "region": ["A", "B"],
                "v": ["null-a", "two-b"],
            }
        )
        io.write_dataset(target, path, mode="overwrite", partition_by=["region"])
        source = pa.table(
            {
                "id": pa.array([None], type=pa.int64()),
                "region": ["A"],
                "v": ["updated-a"],
            }
        )

        result = io.merge(
            source,
            path,
            strategy="upsert",
            key_columns=["id"],
            partition_columns=["region"],
        )

        assert result.updated == 1
        assert result.inserted == 0
        assert result.rewritten_files
        assert all("region=A" in file_path for file_path in result.rewritten_files)
        assert any("region=B" in file_path for file_path in result.preserved_files)


# ---------------------------------------------------------------------------
# Collision: a real value resembling an internal null marker must not collide
# ---------------------------------------------------------------------------
class TestNoPlaceholderCollision:
    @pytest.mark.parametrize("backend", BACKENDS)
    def test_value_resembling_null_marker_no_collision(
        self, tmp_path, io_factory, backend
    ):
        io = io_factory(backend)
        path = str(tmp_path / "ds")
        # Target has a real value that could resemble an internal null marker.
        target = pa.table(
            {"id": pa.array(["__NULL__", "real"], type=pa.string()), "v": ["a", "b"]}
        )
        _write_target(io, path, target)

        # Source has an actual null key and the real "__NULL__" key.
        source = pa.table(
            {"id": pa.array([None, "__NULL__"], type=pa.string()), "v": ["NUL", "UPD"]}
        )
        result = io.merge(source, path, strategy="upsert", key_columns=["id"])
        # "__NULL__" matches existing -> update. None is new -> insert.
        assert result.updated == 1
        assert result.inserted == 1

        final = _read_dataset(path).to_pylist()
        lookup = {r["id"]: r["v"] for r in final}
        assert lookup["__NULL__"] == "UPD"
        assert lookup[None] == "NUL"
        assert lookup["real"] == "b"

    @pytest.mark.parametrize("backend", BACKENDS)
    def test_internal_helper_column_names_do_not_collide(
        self, tmp_path, io_factory, backend
    ):
        io = io_factory(backend)
        path = str(tmp_path / "ds")
        target = pa.table(
            {
                "id": pa.array([None, 1], type=pa.int64()),
                "__fsspeckit_nsk_f_id": ["user-null", "user-one"],
                "__fsspeckit_nsk_i_id": [False, True],
                "v": ["old-null", "old-one"],
            }
        )
        _write_target(io, path, target)
        source = pa.table(
            {
                "id": pa.array([None], type=pa.int64()),
                "__fsspeckit_nsk_f_id": ["updated-user-value"],
                "__fsspeckit_nsk_i_id": [True],
                "v": ["updated-null"],
            }
        )

        result = io.merge(source, path, strategy="upsert", key_columns=["id"])

        assert result.updated == 1
        assert result.inserted == 0
        null_row = next(
            row for row in _read_dataset(path).to_pylist() if row["id"] is None
        )
        assert null_row["__fsspeckit_nsk_f_id"] == "updated-user-value"
        assert bool(null_row["__fsspeckit_nsk_i_id"])
        assert null_row["v"] == "updated-null"

    @pytest.mark.parametrize("backend", BACKENDS)
    def test_nan_equality_is_preserved_in_nullable_batch(
        self, tmp_path, io_factory, backend
    ):
        io = io_factory(backend)
        path = str(tmp_path / "ds")
        target = pa.table({"id": pa.array([None, math.nan]), "v": ["null", "nan"]})
        _write_target(io, path, target)
        source = pa.table({"id": pa.array([None, math.nan]), "v": ["N", "NAN"]})

        result = io.merge(source, path, strategy="upsert", key_columns=["id"])

        assert result.updated == 2
        assert result.inserted == 0
        rows = _read_dataset(path).to_pylist()
        assert next(row["v"] for row in rows if row["id"] in [None]) == "N"
        assert next(row["v"] for row in rows if row["id"] != row["id"]) == "NAN"

    @pytest.mark.parametrize("backend", BACKENDS)
    def test_duplicate_nan_source_key_is_last_row_wins(
        self, tmp_path, io_factory, backend
    ):
        io = io_factory(backend)
        path = str(tmp_path / "ds")
        target = pa.table({"id": pa.array([None, math.nan]), "v": ["null", "old-nan"]})
        _write_target(io, path, target)
        source = pa.table(
            {
                "id": pa.array([None, math.nan, math.nan]),
                "v": ["N", "first-nan", "last-nan"],
            }
        )

        result = io.merge(source, path, strategy="upsert", key_columns=["id"])

        assert result.updated == 2
        assert result.inserted == 0
        rows = _read_dataset(path).to_pylist()
        assert len(rows) == 2
        assert next(row["v"] for row in rows if row["id"] != row["id"]) == "last-nan"


# ---------------------------------------------------------------------------
# Partition immutability enforced when matching key contains null
# ---------------------------------------------------------------------------
class TestPartitionImmutabilityNullKeyPyArrow:
    def test_partition_change_rejected_with_null_key_pyarrow(self, tmp_path):
        io = PyarrowDatasetIO()
        path = str(tmp_path / "ds")
        target = pa.table(
            {
                "id": pa.array([1, None], type=pa.int64()),
                "region": ["A", "A"],
                "v": ["a", "n"],
            }
        )
        io.write_dataset(target, path, mode="overwrite", partition_by=["region"])

        # Try to change region for the null key.
        source = pa.table(
            {
                "id": pa.array([None], type=pa.int64()),
                "region": ["B"],
                "v": ["changed"],
            }
        )
        with pytest.raises(ValueError, match="partition column"):
            io.merge(
                source,
                path,
                strategy="upsert",
                key_columns=["id"],
                partition_columns=["region"],
            )

    def test_partition_immutable_with_null_key_pyarrow(self, tmp_path):
        io = PyarrowDatasetIO()
        path = str(tmp_path / "ds")
        target = pa.table(
            {
                "id": pa.array([1, None], type=pa.int64()),
                "region": ["A", "A"],
                "v": ["a", "n"],
            }
        )
        io.write_dataset(target, path, mode="overwrite", partition_by=["region"])

        source = pa.table(
            {
                "id": pa.array([None], type=pa.int64()),
                "region": ["A"],
                "v": ["updated"],
            }
        )
        result = io.merge(
            source,
            path,
            strategy="upsert",
            key_columns=["id"],
            partition_columns=["region"],
        )
        assert result.updated == 1


@pytest.mark.skipif(not _DUCKDB_AVAILABLE, reason="DuckDB not available")
class TestPartitionImmutabilityNullKey:
    def test_partition_change_rejected_with_null_key(self, tmp_path):
        assert create_duckdb_connection is not None and DuckDBDatasetIO is not None
        conn = create_duckdb_connection()
        io = DuckDBDatasetIO(conn)
        path = str(tmp_path / "ds")
        target = pa.table(
            {
                "id": pa.array([1, None], type=pa.int64()),
                "region": ["A", "A"],
                "v": ["a", "n"],
            }
        )
        _write_target(io, path, target)

        # Try to change region for the null key.
        source = pa.table(
            {
                "id": pa.array([None], type=pa.int64()),
                "region": ["B"],
                "v": ["changed"],
            }
        )
        with pytest.raises(ValueError, match="partition column"):
            io.merge(
                source,
                path,
                strategy="upsert",
                key_columns=["id"],
                partition_columns=["region"],
            )

    def test_partition_immutable_with_null_key_same_partition(self, tmp_path):
        assert create_duckdb_connection is not None and DuckDBDatasetIO is not None
        conn = create_duckdb_connection()
        io = DuckDBDatasetIO(conn)
        path = str(tmp_path / "ds")
        target = pa.table(
            {
                "id": pa.array([1, None], type=pa.int64()),
                "region": ["A", "A"],
                "v": ["a", "n"],
            }
        )
        _write_target(io, path, target)

        source = pa.table(
            {
                "id": pa.array([None], type=pa.int64()),
                "region": ["A"],
                "v": ["updated"],
            }
        )
        result = io.merge(
            source,
            path,
            strategy="upsert",
            key_columns=["id"],
            partition_columns=["region"],
        )
        assert result.updated == 1
        rows = dict(
            zip(
                _read_dataset(path).column("id").to_pylist(),
                _read_dataset(path).column("v").to_pylist(),
                strict=True,
            )
        )
        assert rows[None] == "updated"


# ---------------------------------------------------------------------------
# Strategy semantics with nullable keys
# ---------------------------------------------------------------------------
class TestStrategySemanticsNullKeys:
    @pytest.mark.parametrize("backend", BACKENDS)
    @pytest.mark.parametrize("strategy", ["insert", "update", "upsert"])
    def test_strategy_with_null_key(self, tmp_path, io_factory, backend, strategy):
        io = io_factory(backend)
        path = str(tmp_path / "ds")
        target = pa.table({"id": pa.array([1, None], type=pa.int64()), "v": ["a", "n"]})
        _write_target(io, path, target)

        source = pa.table(
            {"id": pa.array([None, 2], type=pa.int64()), "v": ["N2", "b"]}
        )
        result = io.merge(source, path, strategy=strategy, key_columns=["id"])

        if strategy == "insert":
            # Only new keys inserted; None exists -> not inserted.
            assert result.inserted == 1
            assert result.updated == 0
        elif strategy == "update":
            # Only existing keys updated; 2 is new -> not inserted.
            assert result.updated == 1
            assert result.inserted == 0
        else:  # upsert
            assert result.updated == 1
            assert result.inserted == 1


# ---------------------------------------------------------------------------
# Partitioned datasets with conservative affected-file selection
# ---------------------------------------------------------------------------
class TestPartitionedNullableKeys:
    @pytest.mark.parametrize("backend", BACKENDS)
    def test_partitioned_null_key_merge(self, tmp_path, io_factory, backend):
        io = io_factory(backend)
        path = str(tmp_path / "ds")
        target = pa.table(
            {
                "id": pa.array([1, 2, None], type=pa.int64()),
                "region": ["A", "B", "A"],
                "v": ["a", "b", "n"],
            }
        )
        _write_target(io, path, target)

        source = pa.table(
            {
                "id": pa.array([None, 1], type=pa.int64()),
                "region": ["A", "A"],
                "v": ["N2", "A2"],
            }
        )
        result = io.merge(
            source,
            path,
            strategy="upsert",
            key_columns=["id"],
            partition_columns=["region"],
        )
        assert result.updated == 2
        final = _read_dataset(path).to_pylist()
        lookup = {r["id"]: r["v"] for r in final}
        assert lookup[None] == "N2"
        assert lookup[1] == "A2"
        assert lookup[2] == "b"
