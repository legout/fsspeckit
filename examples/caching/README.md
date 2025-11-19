# Using Caching with fsspeckit

This example demonstrates how to use the caching functionality in fsspeckit to improve performance for repeated file operations.

## Overview

The example shows:
1. Creating a filesystem with caching enabled
2. Performing file operations that populate the cache
3. Demonstrating improved performance on subsequent reads
4. Showing how cached files can be accessed even when the original source is unavailable

## Prerequisites

- Python 3.8+
- fsspeckit installed

## Running the Example

Run the example script:

```bash
python caching_example.py
```

Or run the Jupyter notebook:

```bash
jupyter notebook caching_example.ipynb
```

Or run the Marimo notebook:

```bash
marimo run caching_example_mamo.py
```

## Files in This Example

- `caching_example.py`: Python script demonstrating the functionality
- `caching_example.ipynb`: Jupyter notebook version of the example
- `caching_example_mamo.py`: Marimo notebook version of the example
- `README.md`: This file