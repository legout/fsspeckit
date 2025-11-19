# Reading PyArrow Dataset from S3

This example demonstrates how to read PyArrow datasets from S3-compatible storage systems including AWS S3, Cloudflare R2, and self-hosted MinIO.

## Overview

The example shows:
1. Configuring storage options for different S3-compatible services
2. Creating PyArrow datasets from these storage systems
3. Reading data into PyArrow tables
4. Working with partitioned datasets
5. Using optimized dataset reading with metadata files

## Prerequisites

- Python 3.8+
- fsspeckit installed
- Access to one of the supported S3-compatible services:
  - AWS S3 account with credentials
  - Cloudflare R2 account with credentials
  - MinIO server (can be run locally)

## Running the Examples

### AWS S3

1. Configure your AWS credentials using one of these methods:
   - AWS credentials file (`~/.aws/credentials`)
   - Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
   - IAM roles (for EC2 instances)

2. Update the bucket name and region in the example code.

3. Run the example:
   ```bash
   python s3_pyarrow_dataset.py
   ```

### Cloudflare R2

1. Obtain your R2 access key ID and secret access key from the Cloudflare dashboard.

2. Update the endpoint URL and credentials in the example code.

3. Run the example:
   ```bash
   python s3_pyarrow_dataset.py
   ```

### MinIO (Self-hosted)

1. Set up a local MinIO server by following the instructions in the [MinIO Local Testing Guide](../../docs/minio-local-testing-guide.md).

2. Update the endpoint URL and credentials in the example code to match your local setup.

3. Run the example:
   ```bash
   python s3_pyarrow_dataset.py
   ```

## Files in This Example

- `s3_pyarrow_dataset.py`: Python script demonstrating the functionality
- `s3_pyarrow_dataset.ipynb`: Jupyter notebook version of the example
- `s3_pyarrow_dataset_mamo.py`: Marimo notebook version of the example
- `README.md`: This file