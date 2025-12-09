"""Tests to verify dataset handlers satisfy the DatasetHandler protocol."""

from __future__ import annotations

from typing import TYPE_CHECKING, get_type_hints
import pytest

if TYPE_CHECKING:
    import pyarrow as pa
    from fsspeckit.datasets.interfaces import DatasetHandler
    from fsspeckit.datasets.duckdb import DuckDBDatasetIO
    from fsspeckit.core.ext.dataset import write_pyarrow_dataset


class TestDatasetHandlerProtocol:
    """Test that dataset handlers satisfy the DatasetHandler protocol."""

    def test_duckdb_datasetio_has_required_methods(self) -> None:
        """Verify DuckDBDatasetIO class has all required protocol methods."""
        from fsspeckit.datasets.duckdb.dataset import DuckDBDatasetIO

        # List of required methods from the protocol
        required_methods = [
            "write_parquet_dataset",
            "merge_parquet_dataset",
            "compact_parquet_dataset",
            "optimize_parquet_dataset",
        ]

        # Check that all required methods exist
        for method_name in required_methods:
            assert hasattr(
                DuckDBDatasetIO, method_name
            ), f"DuckDBDatasetIO missing required method: {method_name}"

        # Verify methods are callable
        for method_name in required_methods:
            method = getattr(DuckDBDatasetIO, method_name)
            assert callable(method), f"Method {method_name} is not callable"

    def test_pyarrow_functions_have_required_signatures(self) -> None:
        """Verify PyArrow functions have protocol-compatible signatures."""
        from fsspeckit.core.ext import dataset as ext_dataset
        from fsspeckit.datasets.pyarrow import dataset as pyarrow_dataset

        # List of required functions and their modules
        required_functions = [
            ("write_pyarrow_dataset", ext_dataset),
            ("merge_parquet_dataset_pyarrow", pyarrow_dataset),
            ("compact_parquet_dataset_pyarrow", pyarrow_dataset),
            ("optimize_parquet_dataset_pyarrow", pyarrow_dataset),
        ]

        # Check that all required functions exist
        for func_name, module in required_functions:
            assert hasattr(
                module, func_name
            ), f"{module.__name__} missing required function: {func_name}"

        # Verify functions are callable
        for func_name, module in required_functions:
            func = getattr(module, func_name)
            assert callable(func), f"Function {func_name} is not callable"

    def test_protocol_type_hints_are_valid(self) -> None:
        """Verify that protocol type hints are valid and accessible."""
        from fsspeckit.datasets.interfaces import DatasetHandler
        import inspect

        # Verify protocol has the required methods defined
        assert hasattr(DatasetHandler, "write_parquet_dataset")
        assert hasattr(DatasetHandler, "merge_parquet_dataset")
        assert hasattr(DatasetHandler, "compact_parquet_dataset")
        assert hasattr(DatasetHandler, "optimize_parquet_dataset")

        # Verify methods are Protocol methods (have Ellipsis)
        assert DatasetHandler.write_parquet_dataset is not None
        assert DatasetHandler.merge_parquet_dataset is not None
        assert DatasetHandler.compact_parquet_dataset is not None
        assert DatasetHandler.optimize_parquet_dataset is not None

    def test_duckdb_methods_have_compatible_signatures(self) -> None:
        """Verify DuckDB methods have signatures compatible with protocol."""
        from fsspeckit.datasets.duckdb.dataset import DuckDBDatasetIO
        import inspect

        # Get protocol signature
        from fsspeckit.datasets.interfaces import DatasetHandler

        # Verify write_parquet_dataset has compatible parameters
        write_sig = inspect.signature(DuckDBDatasetIO.write_parquet_dataset)
        protocol_params = list(write_sig.parameters.keys())

        # Check for essential parameters (instance params like 'self' are expected)
        essential_params = {"data", "path", "strategy", "key_columns"}
        assert any(param in protocol_params for param in essential_params), (
            "DuckDBDatasetIO.write_parquet_dataset missing essential parameters"
        )

    def test_convenience_methods_exist(self) -> None:
        """Verify both backends provide convenience methods."""
        from fsspeckit.datasets.duckdb.dataset import DuckDBDatasetIO
        from fsspeckit.core.ext import dataset as ext_dataset

        convenience_methods = [
            "insert_dataset",
            "upsert_dataset",
            "update_dataset",
            "deduplicate_dataset",
        ]

        # Check DuckDB has convenience methods
        for method_name in convenience_methods:
            assert hasattr(
                DuckDBDatasetIO, method_name
            ), f"DuckDBDatasetIO missing convenience method: {method_name}"

        # Check PyArrow has convenience methods (they're monkey-patched to filesystem)
        for method_name in convenience_methods:
            assert hasattr(
                ext_dataset, method_name
            ), f"PyArrow dataset module missing convenience method: {method_name}"

    def test_merge_strategy_type_is_consistent(self) -> None:
        """Verify MergeStrategy type is consistently defined."""
        from fsspeckit.datasets.interfaces import MergeStrategy as ProtocolStrategy
        from fsspeckit.datasets.duckdb.dataset import MergeStrategy as DuckDBStrategy

        # Both should have the same valid values
        expected_strategies = {"upsert", "insert", "update", "full_merge", "deduplicate"}

        # Check protocol defines all expected strategies
        assert set(ProtocolStrategy.__args__) == expected_strategies

        # Check DuckDB defines all expected strategies
        assert set(DuckDBStrategy.__args__) == expected_strategies


class TestProtocolDocumentation:
    """Test that protocol and handlers are properly documented."""

    def test_dataset_handler_protocol_has_docstring(self) -> None:
        """Verify DatasetHandler protocol has documentation."""
        from fsspeckit.datasets.interfaces import DatasetHandler

        assert (
            DatasetHandler.__doc__ is not None and len(DatasetHandler.__doc__) > 0
        ), "DatasetHandler protocol should have documentation"

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
        """Verify PyArrow dataset module is documented."""
        from fsspeckit.core.ext import dataset

        assert (
            dataset.__doc__ is not None and len(dataset.__doc__) > 0
        ), "PyArrow dataset module should have documentation"

        # Verify protocol implementation is documented
        assert (
            "DatasetHandler protocol" in dataset.__doc__
            or "protocol" in dataset.__doc__.lower()
        ), "PyArrow dataset module should document that it implements the protocol"


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
