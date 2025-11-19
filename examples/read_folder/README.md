# Reading Folder of Files into PyArrow Table

This example demonstrates how to read a folder of parquet, csv, or json files into a PyArrow table using fsspeckit.

## Overview

The example shows:
1. Creating sample data files in different formats (Parquet, CSV, JSON)
2. Reading all files of a specific format from a directory into a PyArrow table
3. Demonstrating the differences between reading different file formats
4. Showing how to handle various data types and structures

## Prerequisites

- Python 3.8+
- fsspeckit installed
- pyarrow installed
- polars installed (for CSV and JSON handling)

## Running the Example

Run the example script:

```bash
python read_folder_example.py
```

Or run the Jupyter notebook:

```bash
jupyter notebook read_folder_example.ipynb
```

Or run the Marimo notebook:

```bash
marimo run read_folder_example_mamo.py
```

## Files in This Example

- `read_folder_example.py`: Python script demonstrating the functionality
- `read_folder_example.ipynb`: Jupyter notebook version of the example
- `read_folder_example_mamo.py`: Marimo notebook version of the example
- `README.md`: This file