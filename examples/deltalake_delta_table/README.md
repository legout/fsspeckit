# Working with Delta Lake DeltaTable using fsspeckit

This example demonstrates how to work with Delta Lake DeltaTable using fsspeckit and StorageOptions.

## Overview

The example shows:
1. Using StorageOptions with DeltaTable from deltalake
2. Creating a DeltaTable instance from S3 with storage options

## Prerequisites

- Python 3.8+
- fsspeckit installed
- deltalake installed
- fsspec-s3 installed (for S3 support)

## Running the Example

Run the example script:

```bash
python deltalake_delta_table_example.py
```

Or run the Jupyter notebook:

```bash
jupyter notebook deltalake_delta_table_example.ipynb
```

Or run the Marimo notebook:

```bash
marimo run deltalake_delta_table_example_mamo.py
```

## fsspeckit Components Used

This example uses the following fsspeckit components:

- `AwsStorageOptions`: A StorageOptions subclass for configuring AWS S3 connections with support for profiles and certificate settings
- `to_object_store_kwargs()`: A method that converts StorageOptions to a dictionary compatible with object store libraries like deltalake

The `AwsStorageOptions.create()` method is used to create a storage options object that can be passed to the deltalake library's DeltaTable constructor via the `storage_options` parameter.

## Files in This Example

- `deltalake_delta_table_example.py`: Python script demonstrating the functionality
- `deltalake_delta_table_example.ipynb`: Jupyter notebook version of the example
- `deltalake_delta_table_example_mamo.py`: Marimo notebook version of the example
- `README.md`: This file