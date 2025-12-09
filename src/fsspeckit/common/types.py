"""Type conversion and data transformation utilities."""

from typing import TYPE_CHECKING, Any, Generator

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    import pyarrow as pa


def dict_to_dataframe(
    data: dict | list[dict], unique: bool | list[str] | str = False
) -> Any:
    """Convert a dictionary or list of dictionaries to a Polars DataFrame.

    Handles various input formats:
    - Single dict with list values → DataFrame rows
    - Single dict with scalar values → Single row DataFrame
    - List of dicts with scalar values → Multi-row DataFrame
    - List of dicts with list values → DataFrame with list columns

    Args:
        data: Dictionary or list of dictionaries to convert.
        unique: If True, remove duplicate rows. Can also specify columns.

    Returns:
        Polars DataFrame containing the converted data.

    Examples:
        >>> # Single dict with list values
        >>> data = {'a': [1, 2, 3], 'b': [4, 5, 6]}
        >>> dict_to_dataframe(data)
        shape: (3, 2)
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ i64 ┆ i64 │
        ╞═════╪═════╡
        │ 1   ┆ 4   │
        │ 2   ┆ 5   │
        │ 3   ┆ 6   │
        └─────┴─────┘

        >>> # Single dict with scalar values
        >>> data = {'a': 1, 'b': 2}
        >>> dict_to_dataframe(data)
        shape: (1, 2)
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ i64 ┆ i64 │
        ╞═════╪═════╡
        │ 1   ┆ 2   │
        └─────┴─────┘

        >>> # List of dicts with scalar values
        >>> data = [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}]
        >>> dict_to_dataframe(data)
        shape: (2, 2)
        ┌─────┬─────┐
        │ a   ┆ b   │
        │ --- ┆ --- │
        │ i64 ┆ i64 │
        ╞═════╪═════╡
        │ 1   ┆ 2   │
        │ 3   ┆ 4   │
        └─────┴─────┘
    """
    from fsspeckit.common.optional import _import_polars

    pl = _import_polars()

    if isinstance(data, list):
        # If it's a single-element list, just use the first element
        if len(data) == 1:
            data = data[0]
        # If it's a list of dicts
        else:
            first_item = data[0]
            # Check if the dict values are lists/tuples
            if any(isinstance(v, (list, tuple)) for v in first_item.values()):
                # Each dict becomes a row with list/tuple values
                data = pl.DataFrame(data)
            else:
                # If values are scalars, convert list of dicts to DataFrame
                data = pl.DataFrame(data)

            if unique:
                data = data.unique(
                    subset=None if not isinstance(unique, (str, list)) else unique,
                    maintain_order=True,
                )
            return data

    # If it's a single dict
    if isinstance(data, dict):
        # Check if values are lists/tuples
        if any(isinstance(v, (list, tuple)) for v in data.values()):
            # Get the length of any list value (assuming all lists have same length)
            length = len(next(v for v in data.values() if isinstance(v, (list, tuple))))
            # Convert to DataFrame where each list element becomes a row
            data = pl.DataFrame(
                {
                    k: v if isinstance(v, (list, tuple)) else [v] * length
                    for k, v in data.items()
                }
            )
        else:
            # If values are scalars, wrap them in a list to create a single row
            data = pl.DataFrame({k: [v] for k, v in data.items()})

        if unique:
            data = data.unique(
                subset=None if not isinstance(unique, (str, list)) else unique,
                maintain_order=True,
            )
        return data

    raise ValueError("Input must be a dictionary or list of dictionaries")


# Combined utility that works with multiple data types


def to_pyarrow_table(
    data: (
        Any  # pl.DataFrame, pl.LazyFrame, pd.DataFrame
        | dict
        | list[Any]  # list of DataFrames or dicts
    ),
    concat: bool = False,
    unique: bool | list[str] | str = False,
) -> Any:
    """Convert various data formats to PyArrow Table.

    Handles conversion from Polars DataFrames, Pandas DataFrames,
    dictionaries, and lists of these types to PyArrow Tables.

    Args:
        data: Input data to convert.
        concat: Whether to concatenate multiple inputs into single table.
        unique: Whether to remove duplicates. Can specify columns.

    Returns:
        PyArrow Table containing the converted data.

    Example:
        ```python
        df = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
        table = to_pyarrow_table(df)
        print(table.schema)
        # a: int64
        # b: int64
        ```
    """
    from fsspeckit.common.optional import (
        _import_pandas,
        _import_polars,
        _import_pyarrow,
    )
    from fsspeckit.datasets.pyarrow import convert_large_types_to_normal

    pl = _import_polars()
    pd = _import_pandas()
    pa = _import_pyarrow()

    # Convert dict to DataFrame first
    if isinstance(data, dict):
        data = dict_to_dataframe(data)
    if isinstance(data, list):
        if isinstance(data[0], dict):
            data = dict_to_dataframe(data, unique=unique)

    # Ensure data is a list for uniform processing
    if not isinstance(data, list):
        data = [data]

    # Collect lazy frames
    if isinstance(data[0], pl.LazyFrame):
        data = [dd.collect() for dd in data]

    # Convert based on the first item's type
    if isinstance(data[0], pl.DataFrame):
        if concat:
            data = pl.concat(data, how="diagonal_relaxed")
            if unique:
                data = data.unique(
                    subset=None if not isinstance(unique, (str, list)) else unique,
                    maintain_order=True,
                )
            data = data.to_arrow()
            data = data.cast(convert_large_types_to_normal(data.schema))
        else:
            data = [dd.to_arrow() for dd in data]
            data = [dd.cast(convert_large_types_to_normal(dd.schema)) for dd in data]

    elif isinstance(data[0], pd.DataFrame):
        data = [pa.Table.from_pandas(dd, preserve_index=False) for dd in data]
        if concat:
            data = pa.concat_tables(data, promote_options="permissive")
            if unique:
                data = (
                    pl.from_arrow(data)
                    .unique(
                        subset=None if not isinstance(unique, (str, list)) else unique,
                        maintain_order=True,
                    )
                    .to_arrow()
                )
                data = data.cast(convert_large_types_to_normal(data.schema))

    elif isinstance(data[0], (pa.RecordBatch, Generator)):
        if concat:
            data = pa.Table.from_batches(data)
            if unique:
                data = (
                    pl.from_arrow(data)
                    .unique(
                        subset=None if not isinstance(unique, (str, list)) else unique,
                        maintain_order=True,
                    )
                    .to_arrow()
                )
                data = data.cast(convert_large_types_to_normal(data.schema))
        else:
            data = [pa.Table.from_batches([dd]) for dd in data]

    return data
