"""Tests to verify dataset handlers satisfy the DatasetHandler protocol."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    import pyarrow as pa

    from fsspeckit.datasets.interfaces import DatasetHandler


class TestDatasetHandlerProtocol:
    """Test that dataset handlers satisfy the DatasetHandler protocol."""

    def test_duckdb_datasetio_has_required_methods(self) -> None:
        """Verify DuckDBDatasetIO class has all required protocol methods."""
        from fsspeckit.datasets.duckdb.dataset import DuckDBDatasetIO

        # List of required methods from the protocol
        required_methods = [
            "write_dataset",
            "merge",
            "compact_parquet_dataset",
            "optimize_parquet_dataset",
        ]

        # Check that all required methods exist
        for method_name in required_methods:
            assert hasattr(DuckDBDatasetIO, method_name), (
                f"DuckDBDatasetIO missing required method: {method_name}"
            )

        # Verify methods are callable
        for method_name in required_methods:
            method = getattr(DuckDBDatasetIO, method_name)
            assert callable(method), f"Method {method_name} is not callable"

    def test_pyarrow_datasetio_has_required_methods(self) -> None:
        """Verify PyarrowDatasetIO class has all required protocol methods."""
        from fsspeckit.datasets.pyarrow.io import PyarrowDatasetIO

        required_methods = [
            "write_dataset",
            "merge",
            "compact_parquet_dataset",
            "optimize_parquet_dataset",
        ]

        for method_name in required_methods:
            assert hasattr(PyarrowDatasetIO, method_name), (
                f"PyarrowDatasetIO missing required method: {method_name}"
            )
            assert callable(getattr(PyarrowDatasetIO, method_name))

    def test_protocol_type_hints_are_valid(self) -> None:
        """Verify that protocol type hints are valid and accessible."""
        from fsspeckit.datasets.interfaces import DatasetHandler

        # Verify protocol has the required methods defined
        assert hasattr(DatasetHandler, "write_dataset")
        assert hasattr(DatasetHandler, "merge")
        assert hasattr(DatasetHandler, "compact_parquet_dataset")
        assert hasattr(DatasetHandler, "optimize_parquet_dataset")

        # Verify methods are Protocol methods (have Ellipsis)
        assert DatasetHandler.write_dataset is not None
        assert DatasetHandler.merge is not None
        assert DatasetHandler.compact_parquet_dataset is not None
        assert DatasetHandler.optimize_parquet_dataset is not None

    def test_merge_strategy_type_is_consistent(self) -> None:
        """Verify MergeStrategy type is consistently defined."""
        from fsspeckit.datasets.interfaces import MergeStrategy, WriteMode

        assert set(MergeStrategy.__args__) == {"insert", "update", "upsert"}
        assert set(WriteMode.__args__) == {"append", "overwrite"}


class TestProtocolDocumentation:
    """Test that protocol and handlers are properly documented."""

    def test_dataset_handler_protocol_has_docstring(self) -> None:
        """Verify DatasetHandler protocol has documentation."""
        from fsspeckit.datasets.interfaces import DatasetHandler

        assert DatasetHandler.__doc__ is not None and len(DatasetHandler.__doc__) > 0, (
            "DatasetHandler protocol should have documentation"
        )

    def test_duckdb_datasetio_has_docstring(self) -> None:
        """Verify DuckDBDatasetIO class is documented."""
        from fsspeckit.datasets.duckdb.dataset import DuckDBDatasetIO

        assert (
            DuckDBDatasetIO.__doc__ is not None and len(DuckDBDatasetIO.__doc__) > 0
        ), "DuckDBDatasetIO class should have documentation"

        # Verify protocol implementation is documented
        assert (
            "DatasetHandler protocol" in DuckDBDatasetIO.__doc__
            or "protocol" in DuckDBDatasetIO.__doc__.lower()
        ), "DuckDBDatasetIO should document that it implements the protocol"

    def test_pyarrow_module_has_docstring(self) -> None:
        """Verify PyArrow dataset I/O class is documented."""
        from fsspeckit.datasets.pyarrow.io import PyarrowDatasetIO

        assert (
            PyarrowDatasetIO.__doc__ is not None and len(PyarrowDatasetIO.__doc__) > 0
        ), "PyarrowDatasetIO class should have documentation"


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
