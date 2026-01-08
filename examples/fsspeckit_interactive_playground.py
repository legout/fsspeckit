"""fsspeckit Interactive Playground - Marimo Notebook

This interactive notebook demonstrates and tests various functions and features of fsspeckit.
Run with: marimo edit fsspeckit_interactive_playground.py

Learn more about Marimo: https://marimo.io
"""

import marimo

__generated_with = "0.18.2"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 🐥 fsspeckit Interactive Playground

    Welcome to the interactive fsspeckit testing notebook! This notebook demonstrates
    the key features and capabilities of the fsspeckit library for efficient
    parquet dataset operations.

    ## Features Demonstrated

    1. **Dataset Operations** - Read/write with PyArrow, DuckDB, Pandas, and Polars
    2. **Merge Operations** - Upsert, insert, update, deduplicate strategies
    3. **Compaction & Optimization** - Reduce file count and improve performance
    4. **Dataset Statistics** - Collect and analyze dataset metadata
    5. **Filtering & SQL** - Query data with SQL filters
    6. **Error Handling** - Common errors and how to handle them

    ## How to Use

    - Each cell can be run independently
    - Use the interactive widgets to adjust parameters
    - Results are displayed inline for quick feedback

    ---
    """)
    return


@app.cell
def _(mo):
    """Import required libraries"""
    import tempfile
    from pathlib import Path

    import pandas as pd
    import polars as pl
    import pyarrow as pa
    import pyarrow.parquet as pq

    from fsspeckit.datasets.pyarrow import (
        # PyArrow backend
        PyarrowDatasetIO,
        PyarrowDatasetHandler,
        compact_parquet_dataset_pyarrow,
        optimize_parquet_dataset_pyarrow,


    )
    from fsspeckit.datasets.duckdb import (
        # DuckDB backend
        DuckDBDatasetIO,
        DuckDBParquetHandler,
        compact_parquet_dataset_duckdb,
        collect_dataset_stats_duckdb,
        create_duckdb_connection,
    )

    temp_dir_pa = Path(tempfile.mkdtemp())
    temp_dir_ddb = Path(tempfile.mkdtemp())

    mo.vstack(
        [
            mo.md("✅ Libraries imported successfully"),
            mo.md(f"📁 Working directory Pyarrow: {temp_dir_pa}"),
            mo.md(f"📁 Working directory DuckDB: {temp_dir_ddb}"),
        ]
    )
    return (
        DuckDBDatasetIO,
        DuckDBParquetHandler,
        PyarrowDatasetIO,
        compact_parquet_dataset_pyarrow,
        create_duckdb_connection,
        pa,
        pq,
        temp_dir_ddb,
        temp_dir_pa,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 📊 1. Dataset Operations

    Test reading and writing parquet files with different backends and formats.
    """)
    return


@app.cell
def _(mo):
    """Create sample data widget"""

    data_size = mo.ui.slider(
        start=100,
        stop=10_000_000,
        step=100_000,
        value=1_000_000,
        label="Number of records to generate",
    )

    backend_choice = mo.ui.dropdown(
        options=["PyArrow", "DuckDB", "Pandas", "Polars"],
        value="PyArrow",
        label="Choose backend",
    )

    mo.vstack([mo.md(f"**Configuration:**"), data_size, backend_choice])
    return backend_choice, data_size


@app.cell
def _(data_size, mo, pa):
    """Generate sample data"""

    import random
    from datetime import datetime, timedelta


    def create_sample_data(n_records):
        """Create realistic sample sales data"""
        products = [
            "Laptop",
            "Mouse",
            "Keyboard",
            "Monitor",
            "Headphones",
            "Webcam",
            "USB Hub",
            "External SSD",
            "Docking Station",
            "Cable Kit",
        ]
        categories = ["Electronics", "Accessories", "Peripherals", "Networking"]
        customers = [
            "Alice",
            "Bob",
            "Charlie",
            "Diana",
            "Eve",
            "Frank",
            "Grace",
            "Henry",
        ]

        records = []
        base_date = datetime(2024, 1, 1)

        for i in range(n_records):
            record = {
                "order_id": 1000 + i,
                "customer": random.choice(customers),
                "product": random.choice(products),
                "category": random.choice(categories),
                "quantity": random.randint(1, 10),
                "price": round(random.uniform(10.0, 500.0), 2),
                "date": (
                    base_date + timedelta(days=random.randint(0, 180))
                ).strftime("%Y-%m-%d"),
            }
            record["total"] = record["quantity"] * record["price"]
            records.append(record)

        return pa.Table.from_pylist(records)


    sample_data = create_sample_data(data_size.value)
    mo.vstack(
        [
            mo.md(f"✅ Generated {len(sample_data)} records."),
            mo.md(f"📋 Columns: {', '.join(sample_data.schema.names)}"),
        ]
    )
    return (sample_data,)


@app.cell
def _(mo, sample_data):
    """Display sample data preview"""

    mo.vstack([mo.md("### Sample Data Preview"), sample_data])
    return


@app.cell
def _(
    DuckDBDatasetIO,
    PyarrowDatasetIO,
    backend_choice,
    create_duckdb_connection,
    mo,
    sample_data,
    temp_dir_ddb,
    temp_dir_pa,
):
    """Write dataset with selected backend"""

    output_path_pa = temp_dir_pa / f"dataset_{backend_choice.value.lower()}"
    output_path_ddb = temp_dir_ddb / f"dataset_{backend_choice.value.lower()}"

    output_path_pa.mkdir(exist_ok=True)
    output_path_ddb.mkdir(exist_ok=True)

    output_file_pa = output_path_pa / "sales.parquet"
    output_file_ddb = output_path_ddb / "sales.parquet"


    io_pa = PyarrowDatasetIO()
    result_pa1 = io_pa.write_parquet(sample_data, str(output_file_pa))


    conn = create_duckdb_connection()

    io_ddb = DuckDBDatasetIO(conn)
    result_ddb1 = io_ddb.write_parquet(sample_data, str(output_file_ddb))

    mo.vstack(
        [
            mo.md(
                f"✅ **Written with DuckDB** to `{output_file_pa.name}`\n\n{result_pa1}"
            ),
            mo.md(
                f"✅ **Written with DuckDB** to `{output_file_ddb.name}`\n\n{result_ddb1}"
            ),
        ]
    )
    return io_ddb, io_pa


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 🔀 2. Merge Operations

    Test different merge strategies: upsert, insert, update, deduplicate.
    """)
    return


@app.cell
def _(mo):
    """Merge strategy configuration"""

    merge_strategy = mo.ui.dropdown(
        options=["upsert", "insert", "update"],
        value="upsert",
        label="Merge strategy",
    )

    key_columns = mo.ui.text(
        value="order_id", label="Key columns (comma-separated)"
    )

    mo.vstack([mo.md(f"**Merge Configuration:**"), merge_strategy, key_columns])
    return key_columns, merge_strategy


@app.cell
def _(sample_data, temp_dir_pa):
    """Demonstrate merge operation"""
    # Create source and target datasets
    target_path_pa = temp_dir_pa / "merge_target"
    source_path_pa = temp_dir_pa / "merge_source"
    target_path_ddb = temp_dir_pa / "merge_target"
    source_path_ddb = temp_dir_pa / "merge_source"

    target_path_pa.mkdir(exist_ok=True)
    source_path_pa.mkdir(exist_ok=True)
    target_path_ddb.mkdir(exist_ok=True)
    source_path_ddb.mkdir(exist_ok=True)

    # Split sample data
    split_point = len(sample_data) // 2

    target_data = sample_data.slice(0, split_point)
    source_data = sample_data.slice(split_point // 2, split_point)

    return (
        source_data,
        source_path_ddb,
        source_path_pa,
        target_data,
        target_path_ddb,
        target_path_pa,
    )


@app.cell
def _(
    pq,
    source_data,
    source_path_ddb,
    source_path_pa,
    target_data,
    target_path_ddb,
    target_path_pa,
):
    # Write initial data
    pq.write_table(target_data, target_path_pa / "data.parquet")
    pq.write_table(source_data, source_path_pa / "data.parquet")
    pq.write_table(target_data, target_path_ddb / "data.parquet")
    pq.write_table(source_data, source_path_ddb / "data.parquet")
    return


@app.cell
def _(
    io_pa,
    key_columns,
    merge_strategy,
    source_data,
    target_path_ddb,
    target_path_pa,
):
    result_merge_pa = io_pa.merge(
        data=source_data,
        path=str(target_path_pa),
        strategy=merge_strategy.value,
        key_columns=[k.strip() for k in key_columns.value.split(",")],
    )
    result_merge_ddb = io_pa.merge(
        data=source_data,
        path=str(target_path_ddb),
        strategy=merge_strategy.value,
        key_columns=[k.strip() for k in key_columns.value.split(",")],
    )
    return result_merge_ddb, result_merge_pa


@app.cell
def _(result_merge_ddb):
    result_merge_ddb
    return


@app.cell
def _(result_merge_pa):
    result_merge_pa
    return


@app.cell
def _(io_pa, target_path_pa):
    t_pa = io_pa.read_parquet(target_path_pa.as_posix())
    return


@app.cell
def _(io_ddb, target_path_ddb):
    t_ddb = io_ddb.read_parquet(target_path_ddb.as_posix())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 🗜️ 3. Compaction & Optimization

    Reduce file count and improve query performance with compaction.
    """)
    return


@app.cell
def _(mo):
    """Compaction configuration"""
    target_mb = mo.ui.slider(
        start=1,
        stop=100,
        step=1,
        value=10,
        label="Target file size (MB)",
    )

    target_rows = mo.ui.slider(
        start=100,
        stop=10000,
        step=100,
        value=1000,
        label="Target rows per file",
    )

    dry_run = mo.ui.checkbox(label="Dry run (plan only)", value=False)

    mo.md(f"**Compaction Configuration:**\n\n{target_mb}\n\n{target_rows}\n\n{dry_run}")
    return dry_run, target_mb, target_rows


@app.cell
def _(io_pa, target_path_pa):
    io_pa.compact_parquet_dataset(target_path_pa, dry_run=True, compression="zstd", target_mb_per_file=100, target_rows_per_file=1_000_000)
    return


@app.cell
def _(mo, pq, sample_data, temp_dir):
    """Create fragmented dataset for compaction demo"""
    # Create many small files
    frag_path = temp_dir / "fragmented"
    frag_path.mkdir(exist_ok=True)

    chunk_size = 10
    for i in range(0, len(sample_data), chunk_size):
        chunk = sample_data.slice(i, min(chunk_size, len(sample_data) - i))
        chunk_file = frag_path / f"chunk_{i // chunk_size:03d}.parquet"
        pq.write_table(chunk, chunk_file)

    file_count = len(list(frag_path.glob("*.parquet")))
    total_size = sum(f.stat().st_size for f in frag_path.glob("*.parquet"))

    mo.md(
        f"""
        ### 📁 Fragmented Dataset Created

        - **Files:** {file_count}
        - **Total size:** {total_size / 1024:.1f} KB
        - **Records:** {len(sample_data)}
        """
    )
    return (frag_path,)


@app.cell
def _(
    compact_parquet_dataset_pyarrow,
    dry_run,
    frag_path,
    mo,
    target_mb,
    target_rows,
):
    """Run compaction"""
    result4 = compact_parquet_dataset_pyarrow(
        str(frag_path),
        target_mb_per_file=target_mb.value,
        target_rows_per_file=target_rows.value if target_rows.value > 0 else None,
        dry_run=dry_run.value,
    )

    files_after = len(list(frag_path.glob("*.parquet")))

    mo.md(
        f"""
        ### ✅ Compaction Result

        **Before:** {result4['before_file_count']} files
        **After:** {files_after} files
        **Compacted:** {result4.get('compacted_file_count', 0)} files
        **Dry run:** {result4.get('dry_run', False)}

        **Details:**
        ```json
        {result4}
        ```
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 📈 4. Dataset Statistics

    Collect and analyze dataset metadata.
    """)
    return


@app.cell
def _(frag_path, mo):
    """Collect dataset statistics"""
    from fsspeckit.datasets.pyarrow import collect_dataset_stats_pyarrow

    stats = collect_dataset_stats_pyarrow(str(frag_path))

    mo.md(
        f"""
        ### 📊 Dataset Statistics

        **File Information:**
        - Total files: {len(stats['files'])}
        - Total bytes: {stats['total_bytes']:,}
        - Total rows: {stats['total_rows']:,}

        **File Details:**
        ```json
        {[{'path': f['path'], 'size_bytes': f['size_bytes'], 'num_rows': f['num_rows']}
          for f in stats['files'][:5]]}
        ...
        ```
        """
    )
    return


@app.cell
def _(mo):
    mo.md("""
    ## 🔍 5. Filtering & SQL

    Query data with SQL filters using DuckDB.
    """)
    return


@app.cell
def _(mo):
    """SQL query configuration"""
    sql_filter = mo.ui.text(
        value="total > 100",
        label="SQL WHERE clause",
        placeholder="e.g., total > 100 AND category = 'Electronics'",
    )

    mo.md(f"**SQL Filter:**\n\n{sql_filter}")
    return (sql_filter,)


@app.cell
def _(DuckDBParquetHandler, frag_path, mo, result, sql_filter):
    """Execute SQL query"""

    handler = DuckDBParquetHandler()
    result6 = None

    try:
        query = f"""
        SELECT *
        FROM read_parquet('{frag_path}/*.parquet')
        WHERE {sql_filter.value}
        ORDER BY total DESC
        LIMIT 10
        """

        result6 = handler.execute_sql(query)
        df6 = result.fetchdf()

        mo.md(
            f"""
            ### ✅ SQL Query Results

            **Query:**
            ```sql
            {query.strip()}
            ```

            **Results:** {len(df6)} rows
            """
        )

        mo.ui.table(df6)

    except Exception as e:
        mo.md(f"⚠️ **SQL Error:** `{str(e)}`")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## ⚠️ 6. Error Handling

    Demonstrate common errors and proper handling.
    """)
    return


@app.cell
def _(mo):
    """Error scenarios"""
    error_scenario = mo.ui.dropdown(
        options=[
            "Missing file",
            "Invalid key column",
            "Empty dataset",
            "Invalid merge strategy",
        ],
        value="Missing file",
        label="Error scenario to test",
    )

    test_button = mo.ui.button(label="Test error handling", value=False)

    mo.md(f"**Error Scenario:**\n\n{error_scenario}\n\n{test_button}")
    return error_scenario, test_button


@app.cell
def _(PyarrowDatasetIO, error_scenario, mo, pa, pq, temp_dir, test_button):
    """Test error handling"""
    from fsspeckit.datasets.pyarrow import collect_dataset_stats_pyarrow
    from fsspeckit.core.merge import MergeStrategy

    if not test_button.value:
        mo.md("👆 Click the button above to test error handling")
        #return

    scenario = error_scenario.value
    error_msg = None
    handled = False

    try:
        if scenario == "Missing file":
            try:
                io = PyarrowDatasetIO()
                io.read_parquet(str(temp_dir / "nonexistent.parquet"))
            except Exception as e:
                error_msg = f"FileNotFoundError: {str(e)}"
                handled = True

        elif scenario == "Invalid key column":
            try:
                io = PyarrowDatasetIO()
                # Create test data first
                test_path = temp_dir / "error_test"
                test_path.mkdir(exist_ok=True)
                pq.write_table(
                    pa.table({"id": [1, 2, 3], "value": [10, 20, 30]}),
                    test_path / "data.parquet",
                )
                # Try to merge with invalid key
                io.merge(
                    data=pa.table({"id": [4], "value": [40]}),
                    path=str(test_path),
                    key_columns=["nonexistent_column"],
                    strategy="upsert",
                )
            except Exception as e:
                error_msg = f"KeyError: {str(e)}"
                handled = True

        elif scenario == "Empty dataset":
            try:
                empty_path = temp_dir / "empty_test"
                empty_path.mkdir(exist_ok=True)
                collect_dataset_stats_pyarrow(str(empty_path))
            except Exception as e:
                error_msg = f"FileNotFoundError: {str(e)}"
                handled = True

        elif scenario == "Invalid merge strategy":
            try:
                MergeStrategy("invalid_strategy")
            except Exception as e:
                error_msg = f"ValueError: {str(e)}"
                handled = True

    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"

    if handled and error_msg:
        mo.md(
            f"""
            ### ✅ Error Handled Correctly

            **Scenario:** {scenario}

            **Error caught:**
            ```
            {error_msg}
            ```

            This error was properly caught and can be handled gracefully in your code.
            """
        )
    elif error_msg:
        mo.md(f"⚠️ **Unexpected error:** {error_msg}")
    else:
        mo.md(
            "❓ No error was raised - this might indicate an issue with the error handling test"
        )
    return


@app.cell
def _(mo, temp_dir):
    """Cleanup"""
    import shutil

    cleanup_button = mo.ui.button(label="Clean up temporary directory", value=False)

    mo.md(f"\n---\n\n{cleanup_button}")

    if cleanup_button.value:
        try:
            shutil.rmtree(temp_dir)
            mo.md(f"\n🧹 **Cleaned up** temporary directory: `{temp_dir}`")
        except Exception as e:
            mo.md(f"⚠️ **Cleanup error:** `{e}`")
    return


@app.cell
def _(mo):
    """Footer"""
    mo.md(
        """
        ---

        ## 🎯 Next Steps

        - Explore the [fsspeckit documentation](https://fsspeckit.readthedocs.io/)
        - Check out more examples in the `examples/` directory
        - Read the API reference for detailed function documentation

        **Resources:**
        - [GitHub Repository](https://github.com/yourusername/fsspeckit)
        - [PyPI Package](https://pypi.org/project/fsspeckit/)
        - [Issue Tracker](https://github.com/yourusername/fsspeckit/issues)

        ---
        *Created with ❤️ using [marimo](https://marimo.io)*
        """
    )
    return


if __name__ == "__main__":
    app.run()
