"""Pytest fixtures and test data for utils module tests."""

import pytest
import pyarrow as pa
import polars as pl
import pandas as pd
from datetime import datetime, timezone
from fsspec.implementations.local import LocalFileSystem


@pytest.fixture
def local_filesystem():
    """Create a local filesystem instance for testing."""
    return LocalFileSystem()


@pytest.fixture
def sample_polars_df():
    """Create a sample Polars DataFrame for testing."""
    return pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "age": [25, 30, 35, 40, 45],
            "score": [85.5, 90.2, 78.9, 92.1, 88.7],
            "active": [True, False, True, False, True],
            "join_date": [
                "2023-01-01",
                "2023-02-15",
                "2023-03-20",
                "2023-04-25",
                "2023-05-30",
            ],
            "category": ["A", "B", "A", "C", "B"],
            "int_string": ["1", "2", "3", "4", "5"],
            "float_string": ["85.5", "90.2", "78.9", "92.1", "88.7"],
            "bool_string": ["true", "false", "yes", "no", "1"],
            "mixed_string": ["1", "2.5", "true", "invalid", "null"],
            "nulls": [None, "value", None, "value", None],
            "empty_strings": ["", "non-empty", "", "non-empty", ""],
            "null_like": ["NULL", "value", "None", "value", "NaN"],
        }
    )


@pytest.fixture
def sample_pyarrow_table(sample_polars_df):
    """Create a sample PyArrow Table for testing."""
    # Convert Polars DataFrame to PyArrow Table
    return sample_polars_df.to_arrow()


@pytest.fixture
def sample_pandas_df(sample_polars_df):
    """Create a sample Pandas DataFrame for testing."""
    # Convert Polars DataFrame to Pandas DataFrame
    return sample_polars_df.to_pandas()


@pytest.fixture
def datetime_test_data():
    """Create test data with various datetime formats."""
    return {
        "iso": ["2023-12-31T23:59:59", "2024-01-01T00:00:00"],
        "us_date": ["12/31/2023", "01/01/2024"],
        "german_date": ["31.12.2023", "01.01.2024"],
        "compact": ["20231231", "20240101"],
        "with_tz": ["2023-12-31T23:59:59+01:00", "2024-01-01T00:00:00Z"],
        "time_only": ["23:59:59", "00:00:00"],
        "date_only": ["2023-12-31", "2024-01-01"],
        "invalid": ["invalid_date", "not_a_date"],
    }


@pytest.fixture
def schema_test_data():
    """Create test data for schema operations."""
    return [
        pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("value", pa.float64()),
            ]
        ),
        pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field("name", pa.large_string()),
                pa.field("extra", pa.bool_()),
            ]
        ),
        pa.schema(
            [
                pa.field("id", pa.uint64()),
                pa.field("name", pa.string()),
                pa.field("value", pa.float32()),
            ]
        ),
    ]


@pytest.fixture
def large_test_data():
    """Create larger test data for performance testing."""
    size = 1000
    return {
        "id": list(range(size)),
        "value": [i * 0.5 for i in range(size)],
        "category": ["A", "B", "C", "D"] * (size // 4),
        "timestamp": [datetime(2023, 1, 1) + pd.Timedelta(days=i) for i in range(size)],
        "active": [i % 2 == 0 for i in range(size)],
    }


@pytest.fixture
def edge_case_data():
    """Create edge case test data."""
    return {
        "all_nulls": [None, None, None, None, None],
        "mixed_types": [1, "string", 3.14, True, None],
        "special_strings": [
            "",
            " ",
            "NULL",
            "null",
            "None",
            "NaN",
            "N/A",
            "n/a",
            "-",
            "∅",
        ],
        "large_integers": [
            9223372036854775807,
            -9223372036854775808,
            0,
        ],  # int64 max/min
        "unicode_strings": ["café", "naïve", "résumé", "México", "Zürich"],
        "bool_variations": [
            "true",
            "false",
            "TRUE",
            "FALSE",
            "True",
            "False",
            "1",
            "0",
            "yes",
            "no",
            "YES",
            "NO",
            "y",
            "n",
            "Y",
            "N",
            "ok",
            "nok",
            "OK",
            "NOK",
        ],
    }


@pytest.fixture
def nested_test_data():
    """Create test data with nested structures."""
    return {
        "simple": [{"a": 1}, {"a": 2}, {"a": 3}],
        "nested": [
            {"outer": {"inner": 1}, "list": [1, 2]},
            {"outer": {"inner": 2}, "list": [3, 4]},
            {"outer": {"inner": 3}, "list": [5, 6]},
        ],
        "list_values": [[1, 2], [3, 4, 5], [6]],
        "mixed_list": [1, "a", [1, 2], {"b": 3}],
    }


@pytest.fixture
def timezone_test_data():
    """Create test data with timezone information."""
    return {
        "utc": ["2023-12-31T23:59:59Z"],
        "positive_offset": ["2023-12-31T23:59:59+01:00"],
        "negative_offset": ["2023-12-31T23:59:59-05:00"],
        "no_timezone": ["2023-12-31T23:59:59"],
    }


@pytest.fixture
def performance_data():
    """Create large dataset for performance testing."""
    size = 10000
    return pa.table(
        {
            "id": pa.array(range(size)),
            "value": pa.array([float(i) for i in range(size)]),
            "text": pa.array([f"text_{i}" for i in range(size)]),
            "date": pa.array(
                [datetime(2023, 1, 1) + pd.Timedelta(days=i % 365) for i in range(size)]
            ),
            "category": pa.array([f"cat_{i % 100}" for i in range(size)]),
        }
    )


# Custom markers for test categorization
def pytest_configure(config):
    """Configure custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line("markers", "integration: marks tests as integration tests")
    config.addinivalue_line("markers", "performance: marks tests as performance tests")
    config.addinivalue_line("markers", "edge_case: marks tests as testing edge cases")


# Parametrize fixtures for common test scenarios
@pytest.fixture(params=["polars", "pyarrow", "pandas"])
def df_type(request):
    """Fixture to test with different dataframe types."""
    return request.param


@pytest.fixture(params=["string", "int", "float", "bool", "datetime"])
def data_type(request):
    """Fixture to test with different data types."""
    return request.param


@pytest.fixture(params=[True, False])
def boolean_param(request):
    """Fixture to test with boolean parameters."""
    return request.param


# Helper functions for test data generation
def generate_test_data(rows=100, columns=5, include_nulls=True):
    """Generate test data with specified dimensions."""
    import random

    data = {}
    for i in range(columns):
        col_type = random.choice(["int", "float", "string", "bool"])
        if col_type == "int":
            values = [random.randint(0, 100) for _ in range(rows)]
        elif col_type == "float":
            values = [random.uniform(0, 100) for _ in range(rows)]
        elif col_type == "string":
            values = [f"value_{i}" for i in range(rows)]
        else:  # bool
            values = [random.choice([True, False]) for _ in range(rows)]

        if include_nulls and random.random() < 0.1:
            # Add some nulls
            null_indices = random.sample(range(rows), min(5, rows // 10))
            for idx in null_indices:
                values[idx] = None

        data[f"col_{i}"] = values

    return data


def create_temp_file(content="test content", suffix=".txt"):
    """Create a temporary file with given content."""
    import tempfile
    import os

    fd, path = tempfile.mkstemp(suffix=suffix)
    try:
        with os.fdopen(fd, "w") as f:
            f.write(content)
        return path
    except:
        os.unlink(path)
        raise


def create_temp_directory():
    """Create a temporary directory."""
    import tempfile

    return tempfile.mkdtemp()
