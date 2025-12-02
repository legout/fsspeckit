"""Tests for type conversion utilities."""

import pytest
import polars as pl
import pyarrow as pa
import pandas as pd
from datetime import datetime

from fsspeckit.common.types import dict_to_dataframe, to_pyarrow_table


class TestDictToDataFrame:
    """Test dict_to_dataframe function."""

    def test_single_dict_list_values(self):
        """Test conversion of single dict with list values."""
        data = {"a": [1, 2, 3], "b": [4, 5, 6]}
        result = dict_to_dataframe(data)

        assert isinstance(result, pl.DataFrame)
        assert result.shape == (3, 2)
        assert result["a"].to_list() == [1, 2, 3]
        assert result["b"].to_list() == [4, 5, 6]

    def test_single_dict_scalar_values(self):
        """Test conversion of single dict with scalar values."""
        data = {"a": 1, "b": 2, "c": "x"}
        result = dict_to_dataframe(data)

        assert isinstance(result, pl.DataFrame)
        assert result.shape == (1, 3)
        assert result["a"][0] == 1
        assert result["b"][0] == 2
        assert result["c"][0] == "x"

    def test_list_of_dicts(self):
        """Test conversion of list of dictionaries."""
        data = [
            {"a": 1, "b": "x"},
            {"a": 2, "b": "y"},
            {"a": 3, "b": "z"},
        ]
        result = dict_to_dataframe(data)

        assert isinstance(result, pl.DataFrame)
        assert result.shape == (3, 2)
        assert result["a"].to_list() == [1, 2, 3]
        assert result["b"].to_list() == ["x", "y", "z"]

    def test_list_of_dicts_with_list_values(self):
        """Test conversion of list of dicts with list values."""
        data = [
            {"a": [1, 2], "b": ["x", "y"]},
            {"a": [3, 4], "b": ["z", "w"]},
        ]
        result = dict_to_dataframe(data)

        assert isinstance(result, pl.DataFrame)
        assert result.shape == (2, 2)
        assert result["a"].dtype == pl.List(pl.Int64)
        assert result["b"].dtype == pl.List(pl.Utf8)

    def test_unique_parameter_true(self):
        """Test unique parameter set to True."""
        data = [
            {"a": 1, "b": "x"},
            {"a": 1, "b": "x"},  # Duplicate
            {"a": 2, "b": "y"},
        ]
        result = dict_to_dataframe(data, unique=True)

        assert result.shape == (2, 2)  # One row removed
        assert set(result["a"].to_list()) == {1, 2}

    def test_unique_parameter_list(self):
        """Test unique parameter with list of columns."""
        data = [
            {"a": 1, "b": "x", "c": "p"},
            {"a": 1, "b": "x", "c": "q"},  # Duplicate in a,b
            {"a": 2, "b": "y", "c": "r"},
        ]
        result = dict_to_dataframe(data, unique=["a", "b"])

        assert result.shape == (2, 2)  # One row removed
        assert set(result["a"].to_list()) == {1, 2}

    def test_empty_dict(self):
        """Test empty dictionary."""
        data = {}
        result = dict_to_dataframe(data)

        assert isinstance(result, pl.DataFrame)
        assert result.shape == (0, 0)

    def test_empty_list_of_dicts(self):
        """Test empty list of dictionaries."""
        data = []
        result = dict_to_dataframe(data)

        assert isinstance(result, pl.DataFrame)
        assert result.shape == (0, 0)

    def test_mixed_data_types(self):
        """Test handling of mixed data types."""
        data = {
            "int": [1, 2, 3],
            "float": [1.5, 2.5, 3.5],
            "str": ["a", "b", "c"],
            "bool": [True, False, True],
            "null": [None, 1, None],
        }
        result = dict_to_dataframe(data)

        assert result.shape == (3, 5)
        assert result["int"].dtype == pl.Int64
        assert result["float"].dtype == pl.Float64
        assert result["str"].dtype == pl.Utf8
        assert result["bool"].dtype == pl.Boolean
        assert result["null"].dtype == pl.Int64  # Polars inference

    def test_nested_structures(self):
        """Test handling of nested structures."""
        data = {
            "nested": [{"a": 1}, {"a": 2}],
            "simple": [1, 2],
        }
        result = dict_to_dataframe(data)

        assert result.shape == (2, 2)
        assert result["nested"].dtype == pl.Struct({"a": pl.Int64})

    def test_datetime_values(self):
        """Test handling of datetime values."""
        dt1 = datetime(2023, 1, 1)
        dt2 = datetime(2023, 1, 2)
        data = [
            {"date": dt1, "value": 1},
            {"date": dt2, "value": 2},
        ]
        result = dict_to_dataframe(data)

        assert result["date"].dtype == pl.Datetime
        assert result["value"].dtype == pl.Int64

    def test_irregular_lists(self):
        """Test handling of irregular list lengths."""
        data = {
            "a": [[1, 2], [3], [4, 5, 6]],
            "b": ["x", "y", "z"],
        }
        result = dict_to_dataframe(data)

        assert result.shape == (3, 2)
        assert result["a"].dtype == pl.List(pl.Int64)
        assert result["b"].dtype == pl.Utf8


class TestToPyarrowTable:
    """Test to_pyarrow_table function."""

    def test_from_polars_dataframe(self):
        """Test conversion from Polars DataFrame."""
        df = pl.DataFrame(
            {
                "a": [1, 2, 3],
                "b": ["x", "y", "z"],
            }
        )

        result = to_pyarrow_table(df)

        assert isinstance(result, pa.Table)
        assert result.num_rows == 3
        assert result.num_columns == 2
        assert result.schema.field("a").type == pa.int64()
        assert result.schema.field("b").type == pa.string()

    def test_from_polars_lazyframe(self):
        """Test conversion from Polars LazyFrame."""
        df = pl.DataFrame(
            {
                "a": [1, 2, 3],
                "b": ["x", "y", "z"],
            }
        ).lazy()

        result = to_pyarrow_table(df)

        assert isinstance(result, pa.Table)
        assert result.num_rows == 3
        assert result.num_columns == 2

    def test_from_pandas_dataframe(self):
        """Test conversion from Pandas DataFrame."""
        df = pd.DataFrame(
            {
                "a": [1, 2, 3],
                "b": ["x", "y", "z"],
            }
        )

        result = to_pyarrow_table(df)

        assert isinstance(result, pa.Table)
        assert result.num_rows == 3
        assert result.num_columns == 2

    def test_from_pyarrow_table(self):
        """Test identity conversion from PyArrow Table."""
        table = pa.Table.from_pydict(
            {
                "a": [1, 2, 3],
                "b": ["x", "y", "z"],
            }
        )

        result = to_pyarrow_table(table)

        assert result is table  # Should return the same object

    def test_from_dict(self):
        """Test conversion from dictionary."""
        data = {
            "a": [1, 2, 3],
            "b": ["x", "y", "z"],
        }

        result = to_pyarrow_table(data)

        assert isinstance(result, pa.Table)
        assert result.num_rows == 3
        assert result.num_columns == 2
        assert result.schema.field("a").type == pa.int64()
        assert result.schema.field("b").type == pa.string()

    def test_from_list_of_dicts(self):
        """Test conversion from list of dictionaries."""
        data = [
            {"a": 1, "b": "x"},
            {"a": 2, "b": "y"},
            {"a": 3, "b": "z"},
        ]

        result = to_pyarrow_table(data)

        assert isinstance(result, pa.Table)
        assert result.num_rows == 3
        assert result.num_columns == 2

    def test_unsupported_type(self):
        """Test error handling for unsupported types."""
        with pytest.raises(ValueError, match="Unsupported data type"):
            to_pyarrow_table("unsupported")

    def test_preserve_schema(self):
        """Test schema preservation."""
        schema = pa.schema(
            [
                pa.field("a", pa.int32()),
                pa.field("b", pa.string()),
            ]
        )
        table = pa.Table.from_pydict(
            {"a": [1, 2, 3], "b": ["x", "y", "z"]}, schema=schema
        )

        result = to_pyarrow_table(table)
        assert result.schema.field("a").type == pa.int32()

    def test_null_handling(self):
        """Test null value handling."""
        data = {
            "a": [1, None, 3],
            "b": [None, "y", None],
        }

        result = to_pyarrow_table(data)

        assert result["a"].chunks[0].null_count == 1
        assert result["b"].chunks[0].null_count == 2


class TestTypeConversionIntegration:
    """Test integration between different type conversion functions."""

    def test_dict_to_dataframe_to_pyarrow(self):
        """Test round trip from dict to DataFrame to PyArrow."""
        data = {
            "int": [1, 2, 3],
            "float": [1.5, 2.5, 3.5],
            "str": ["a", "b", "c"],
        }

        # Convert to DataFrame
        df = dict_to_dataframe(data)

        # Convert to PyArrow
        table = to_pyarrow_table(df)

        # Verify data integrity
        assert table["int"].to_pylist() == [1, 2, 3]
        assert table["float"].to_pylist() == [1.5, 2.5, 3.5]
        assert table["str"].to_pylist() == ["a", "b", "c"]

    def test_complex_nested_data(self):
        """Test handling of complex nested data structures."""
        data = {
            "users": [
                {"name": "Alice", "age": 30, "scores": [85, 90, 95]},
                {"name": "Bob", "age": 25, "scores": [70, 80, 90]},
            ],
            "metadata": {"total": 2, "active": True},
        }

        # This should handle nested structures appropriately
        df = dict_to_dataframe(data["users"])
        assert df.shape == (2, 3)
        assert df["scores"].dtype == pl.List(pl.Int64)
