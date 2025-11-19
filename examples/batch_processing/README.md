# Batch Processing with fsspeckit

This example demonstrates how to perform batch processing operations with different file formats using fsspeckit.

## Overview

The example shows:
1. Creating sample data files in different formats (Parquet, CSV, JSON)
2. Reading files in batches using the `batch_size` parameter
3. Processing batches of data efficiently
4. Demonstrating the differences between batch processing for different file formats

## Prerequisites

- Python 3.8+
- fsspeckit installed
- pyarrow installed
- polars installed (for CSV and JSON handling)

## Running the Example

Run the example script:

```bash
python batch_processing_example.py
```

Or run the Jupyter notebook:

```bash
jupyter notebook batch_processing_example.ipynb
```

Or run the Marimo notebook:

```bash
marimo run batch_processing_example_mamo.py
```

## Files in This Example

- `batch_processing_example.py`: Python script demonstrating the functionality
- `batch_processing_example.ipynb`: Jupyter notebook version of the example
- `batch_processing_example_mamo.py`: Marimo notebook version of the example
- `README.md`: This file