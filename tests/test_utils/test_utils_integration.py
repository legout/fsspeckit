"""Integration tests for utils module."""

import pytest
import polars as pl
import pyarrow as pa
import pandas as pd
from datetime import datetime

from fsspeckit.common.types import dict_to_dataframe, to_pyarrow_table
from fsspeckit.common.polars import opt_dtype as opt_dtype_pl
from fsspeckit.datasets.pyarrow import opt_dtype as opt_dtype_pa
from fsspeckit.sql.filters import sql2pyarrow_filter
from fsspeckit.common.misc import run_parallel
from fsspeckit.common.datetime import get_timestamp_column


class TestCrossModuleIntegration:
    """Test integration between different utility modules."""

    def test_dict_to_dataframe_to_pyarrow_pipeline(self):
        """Test pipeline from dict to DataFrame to PyArrow."""
        data = {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "values": [[1, 2], [3, 4], [5, 6]],
        }

        # Convert to DataFrame
        df = dict_to_dataframe(data)
        assert isinstance(df, pl.DataFrame)
        assert df.shape == (3, 3)

        # Convert to PyArrow
        table = to_pyarrow_table(df)
        assert isinstance(table, pa.Table)
        assert table.num_rows == 3
        assert table.num_columns == 3

    def test_opt_dtype_cross_framework(self, sample_polars_df):
        """Test opt_dtype works consistently across frameworks."""
        # Test with Polars
        result_pl = opt_dtype_pl(sample_polars_df)

        # Convert to PyArrow and test
        pa_table = sample_polars_df.to_arrow()
        result_pa = opt_dtype_pa(pa_table)

        # Results should be equivalent (though types might differ slightly)
        assert result_pl.shape == result_pa.shape
        assert list(result_pl.columns) == list(result_pa.schema.names)

    def test_sql_filter_with_opt_dtype(self, sample_polars_df):
        """Test SQL filtering with optimized data types."""
        # First optimize the DataFrame
        optimized_df = opt_dtype_pl(sample_polars_df)

        # Convert to PyArrow for SQL filtering
        table = to_pyarrow_table(optimized_df)

        # Apply SQL filter
        filter_expr = sql2pyarrow_filter("age > 30 AND active = true", table.schema)
        filtered_table = table.filter(filter_expr)

        # Verify results
        assert filtered_table.num_rows > 0
        assert filtered_table.num_rows < table.num_rows

    def test_parallel_processing_with_opt_dtype(self, large_test_data):
        """Test parallel processing of opt_dtype on multiple datasets."""
        # Create multiple datasets
        datasets = [
            pl.DataFrame(large_test_data).sample(fraction=0.1) for _ in range(5)
        ]

        # Process in parallel
        results = run_parallel(opt_dtype_pl, datasets, n_jobs=2, verbose=False)

        # Verify all were processed
        assert len(results) == 5
        for result in results:
            assert isinstance(result, pl.DataFrame)

    def test_timestamp_column_detection(self, sample_polars_df):
        """Test timestamp column detection across frameworks."""
        # Add timestamp column
        df_with_ts = sample_polars_df.with_columns(
            pl.col("join_date").str.to_datetime()
        )

        # Test detection in Polars
        ts_cols_pl = get_timestamp_column(df_with_ts)
        assert len(ts_cols_pl) > 0

        # Test detection in PyArrow
        pa_table = df_with_ts.to_arrow()
        ts_cols_pa = get_timestamp_column(pa_table)
        assert len(ts_cols_pa) > 0

    def test_complex_pipeline(self):
        """Test a complex pipeline using multiple utilities."""
        # Start with raw data
        raw_data = [
            {
                "id": i,
                "value": i * 1.5,
                "category": f"cat_{i % 3}",
                "date": f"2023-01-{i + 1:02d}",
            }
            for i in range(100)
        ]

        # Convert to DataFrame and optimize types
        df = dict_to_dataframe(raw_data)
        optimized_df = opt_dtype_pl(df)

        # Add derived columns
        result_df = optimized_df.with_columns(
            [
                (pl.col("value") * 2).alias("doubled_value"),
                pl.col("category").str.to_uppercase().alias("category_upper"),
            ]
        )

        # Convert to PyArrow for filtering
        table = to_pyarrow_table(result_df)

        # Apply complex SQL filter
        filter_expr = sql2pyarrow_filter(
            "value > 10 AND category IN ('cat_1', 'cat_2')", table.schema
        )
        filtered_table = table.filter(filter_expr)

        # Verify pipeline worked
        assert filtered_table.num_rows > 0
        assert filtered_table.num_rows < table.num_rows
        assert "doubled_value" in filtered_table.schema.names


class TestRealWorldScenarios:
    """Test real-world usage scenarios."""

    def test_log_processing_pipeline(self):
        """Test processing log files with type optimization."""
        # Simulate log data
        log_data = []
        for i in range(1000):
            log_data.append(
                {
                    "timestamp": f"2023-12-{i % 28 + 1:02d}T{i % 24:02d}:{i % 60:02d}:{i % 60:02d}",
                    "level": ["INFO", "WARNING", "ERROR"][i % 3],
                    "message": f"Log message {i}",
                    "user_id": str(i % 100),
                    "duration_ms": str(i * 10),
                    "success": str(i % 2 == 0),
                }
            )

        # Convert and optimize
        df = dict_to_dataframe(log_data)
        optimized_df = opt_dtype_pl(df)

        # Verify optimizations
        assert optimized_df.schema["timestamp"] == pl.Datetime
        assert optimized_df.schema["user_id"] == pl.Int64
        assert optimized_df.schema["duration_ms"] == pl.Int64
        assert optimized_df.schema["success"] == pl.Boolean

        # Analyze error rates
        error_df = optimized_df.filter(pl.col("level") == "ERROR")
        assert error_df.height > 0

    def test_sensor_data_processing(self):
        """Test processing sensor IoT data."""
        # Generate sensor data
        import random

        sensor_data = []
        base_time = datetime(2023, 1, 1)

        for i in range(500):
            sensor_data.append(
                {
                    "sensor_id": f"sensor_{i % 10}",
                    "timestamp": (base_time + pd.Timedelta(minutes=i)).isoformat(),
                    "temperature": f"{20 + random.uniform(-5, 5):.2f}",
                    "humidity": f"{50 + random.uniform(-10, 10):.2f}",
                    "pressure": f"{1013 + random.uniform(-5, 5):.2f}",
                    "status": random.choice(["OK", "WARNING", "ERROR"]),
                    "battery": f"{random.uniform(3.0, 4.2):.2f}",
                }
            )

        # Process with Polars
        df = dict_to_dataframe(sensor_data)
        optimized_df = opt_dtype_pl(df)

        # Add time-based aggregations
        result_df = optimized_df.with_columns(
            [
                pl.col("timestamp").str.to_datetime().alias("datetime"),
                (pl.col("temperature").cast(pl.Float32) * 9 / 5 + 32).alias(
                    "temperature_f"
                ),
            ]
        ).with_columns(
            [
                pl.col("datetime").dt.hour().alias("hour"),
                pl.col("datetime").dt.date().alias("date"),
            ]
        )

        # Group by sensor and calculate statistics
        stats = result_df.groupby("sensor_id").agg(
            [
                pl.col("temperature").mean().alias("avg_temp"),
                pl.col("humidity").mean().alias("avg_humidity"),
                pl.col("pressure").std().alias("pressure_std"),
            ]
        )

        # Verify results
        assert stats.height == 10  # 10 unique sensors
        assert "avg_temp" in stats.columns

    def test_e_commerce_data_processing(self):
        """Test processing e-commerce data."""
        # Generate e-commerce data
        ecommerce_data = []
        customers = [f"cust_{i:05d}" for i in range(100)]
        products = [f"prod_{i:03d}" for i in range(50)]

        for i in range(1000):
            ecommerce_data.append(
                {
                    "order_id": f"ORD-{2023}-{i:06d}",
                    "customer_id": random.choice(customers),
                    "product_id": random.choice(products),
                    "quantity": random.randint(1, 10),
                    "unit_price": f"{random.uniform(10, 100):.2f}",
                    "order_date": f"2023-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
                    "status": random.choice(
                        ["pending", "shipped", "delivered", "cancelled"]
                    ),
                    "priority": random.choice(["low", "medium", "high"]),
                }
            )

        # Process data
        df = dict_to_dataframe(ecommerce_data)
        optimized_df = opt_dtype_pl(df)

        # Calculate total amount
        result_df = optimized_df.with_columns(
            [
                (pl.col("quantity") * pl.col("unit_price")).alias("total_amount"),
                pl.col("order_date").str.to_datetime().alias("order_datetime"),
            ]
        )

        # Analyze by status
        status_analysis = result_df.groupby("status").agg(
            [
                pl.count().alias("order_count"),
                pl.col("total_amount").sum().alias("total_revenue"),
                pl.col("total_amount").mean().alias("avg_order_value"),
            ]
        )

        # Verify results
        assert status_analysis.height == 4  # 4 statuses
        assert all(status_analysis["order_count"] > 0)

    def test_financial_data_processing(self):
        """Test processing financial data."""
        # Generate financial data
        financial_data = []
        symbols = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]

        base_date = datetime(2023, 1, 1)
        for i in range(252):  # Trading days in a year
            for symbol in symbols:
                price = 100 + i * 0.1 + random.uniform(-5, 5)
                financial_data.append(
                    {
                        "symbol": symbol,
                        "date": (base_date + pd.Timedelta(days=i)).strftime("%Y-%m-%d"),
                        "open": f"{price:.2f}",
                        "high": f"{price * 1.02:.2f}",
                        "low": f"{price * 0.98:.2f}",
                        "close": f"{price * 1.01:.2f}",
                        "volume": f"{random.randint(1000000, 10000000)}",
                    }
                )

        # Process with PyArrow for better performance with large data
        df = dict_to_dataframe(financial_data)
        table = to_pyarrow_table(df)
        optimized_table = opt_dtype_pa(table)

        # Calculate daily returns
        # Note: This would require window functions, simplified for example
        returns_table = optimized_table.filter(pc.field("close") > 0)

        # Verify data integrity
        assert returns_table.num_rows > 0
        assert returns_table.num_columns == 7
        assert "symbol" in returns_table.schema.names

    def test_mixed_data_source_integration(self):
        """Test integrating data from multiple sources."""
        # Create different data formats
        dict_data = {"id": [1, 2, 3], "value": [10, 20, 30]}
        list_data = [{"id": 4, "value": 40}, {"id": 5, "value": 50}]

        # Convert to DataFrame
        df1 = dict_to_dataframe(dict_data)
        df2 = dict_to_dataframe(list_data)

        # Concatenate and optimize
        combined_df = pl.concat([df1, df2])
        optimized_df = opt_dtype_pl(combined_df)

        # Convert to PyArrow for SQL operations
        table = to_pyarrow_table(optimized_df)

        # Apply SQL query
        filter_expr = sql2pyarrow_filter("value > 25", table.schema)
        result_table = table.filter(filter_expr)

        # Verify results
        assert result_table.num_rows == 3  # Values 30, 40, 50
        assert list(result_table["id"].to_pylist()) == [3, 4, 5]


class TestErrorHandlingIntegration:
    """Test error handling in integration scenarios."""

    def test_malformed_data_handling(self):
        """Test handling malformed data in pipeline."""
        data = {
            "id": [1, 2, 3, 4, 5],
            "value": ["10", "20", "invalid", "40", "50"],
            "date": [
                "2023-01-01",
                "invalid_date",
                "2023-01-03",
                "2023-01-04",
                "2023-01-05",
            ],
        }

        # Should handle gracefully with strict=False
        df = dict_to_dataframe(data)
        result_df = opt_dtype_pl(df, strict=False)

        # Verify fallback to string for invalid values
        assert result_df.schema["value"] == pl.Utf8
        assert result_df.schema["date"] == pl.Utf8

    def test_memory_efficient_processing(self):
        """Test memory-efficient processing of large data."""
        # Generate large dataset
        large_data = {
            "id": list(range(10000)),
            "value": [i * 0.1 for i in range(10000)],
            "category": [f"cat_{i % 100}" for i in range(10000)],
        }

        # Process in chunks
        chunk_size = 1000
        results = []

        for i in range(0, 10000, chunk_size):
            chunk_data = {
                "id": large_data["id"][i : i + chunk_size],
                "value": large_data["value"][i : i + chunk_size],
                "category": large_data["category"][i : i + chunk_size],
            }

            df = dict_to_dataframe(chunk_data)
            optimized_df = opt_dtype_pl(df)
            results.append(optimized_df)

        # Combine results
        final_df = pl.concat(results)
        assert final_df.height == 10000

    def test_type_conversion_edge_cases(self):
        """Test edge cases in type conversion."""
        edge_data = {
            "very_large_int": ["999999999999999999999", "1000000000000000000000"],
            "very_small_float": ["0.0000000001", "1e-10"],
            "unicode_with_special": ["café👍", "测试\n\t"],
            "mixed_bool": ["TRUE", "false", "1", "0", "yes", "no"],
        }

        df = dict_to_dataframe(edge_data)
        result_df = opt_dtype_pl(df, strict=False)

        # Should handle without crashing
        assert result_df.height == 2
        assert result_df.width == 4
