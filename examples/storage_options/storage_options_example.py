"""
Example: Using StorageOptions with fsspeckit

This example demonstrates how to use different StorageOptions classes with fsspeckit
to configure various storage systems.

The example shows:
1. Creating instances of different StorageOptions subclasses
2. Converting them to fsspec filesystems using to_filesystem()
3. Performing simple operations with each filesystem
4. Demonstrating configuration for different storage systems
"""

import os
import tempfile
from pathlib import Path

# Import the storage options classes
from fsspeckit.storage_options import (
    LocalStorageOptions,
    AwsStorageOptions,
    AzureStorageOptions,
    GcsStorageOptions,
    StorageOptions,
)


def main():
    """Demonstrate usage of different StorageOptions classes."""

    print("=== StorageOptions to fsspec Filesystem Example ===\n")

    # 1. LocalStorageOptions Example
    print("1. LocalStorageOptions Example:")
    print("-" * 40)

    # Create local storage options
    local_options = LocalStorageOptions(auto_mkdir=True)
    print(
        f"Created LocalStorageOptions: protocol='{local_options.protocol}', auto_mkdir={local_options.auto_mkdir}"
    )

    # Convert to fsspec filesystem
    local_fs = local_options.to_filesystem()
    print(f"Created fsspec filesystem: {type(local_fs).__name__}")

    # Create a temporary directory and file for demonstration
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_file = os.path.join(temp_dir, "test_file.txt")

        # Write a test file
        with local_fs.open(temp_file, "w") as f:
            f.write("Hello, LocalStorageOptions!")

        # List files in the directory
        files = local_fs.ls(temp_dir)
        print(f"Files in {temp_dir}: {[os.path.basename(f) for f in files]}")

        # Read the file content
        with local_fs.open(temp_file, "r") as f:
            content = f.read()
        print(f"File content: '{content}'")

    print("Local storage example completed.\n")

    # 2. AwsStorageOptions Example (using MinIO for demonstration)
    print("2. AwsStorageOptions Example:")
    print("-" * 40)

    # Create AWS storage options for MinIO (local S3-compatible service)
    aws_options = AwsStorageOptions(
        endpoint_url="http://localhost:9000",  # Default MinIO endpoint
        access_key_id="minioadmin",  # Default MinIO credentials
        secret_access_key="minioadmin",
        allow_http=True,  # Allow HTTP for local testing
        region="us-east-1",
    )
    print(
        f"Created AwsStorageOptions: protocol='{aws_options.protocol}', endpoint='{aws_options.endpoint_url}'"
    )

    # Convert to fsspec filesystem
    try:
        aws_fs = aws_options.to_filesystem()
        print(f"Created fsspec filesystem: {type(aws_fs).__name__}")

        # Try to list buckets (this will fail if MinIO is not running)
        try:
            buckets = aws_fs.ls("")
            print(f"Available buckets: {buckets}")
        except Exception as e:
            print(f"Could not connect to MinIO (expected if not running): {e}")
    except Exception as e:
        print(f"Could not create AWS filesystem: {e}")

    print("AWS storage example completed.\n")

    # 3. AzureStorageOptions Example
    print("3. AzureStorageOptions Example:")
    print("-" * 40)

    # Create Azure storage options (using connection string for simplicity)
    azure_options = AzureStorageOptions(
        protocol="az",
        account_name="demoaccount",
        connection_string="DefaultEndpointsProtocol=https;AccountName=demoaccount;AccountKey=demokey==;EndpointSuffix=core.windows.net",
    )
    print(
        f"Created AzureStorageOptions: protocol='{azure_options.protocol}', account='{azure_options.account_name}'"
    )

    # Convert to fsspec filesystem
    try:
        azure_fs = azure_options.to_filesystem()
        print(f"Created fsspec filesystem: {type(azure_fs).__name__}")

        # Try to list containers (this will fail without real credentials)
        try:
            containers = azure_fs.ls("")
            print(f"Available containers: {containers}")
        except Exception as e:
            print(
                f"Could not connect to Azure Storage (expected without real credentials): {e}"
            )
    except Exception as e:
        print(f"Could not create Azure filesystem: {e}")

    print("Azure storage example completed.\n")

    # 4. GcsStorageOptions Example
    print("4. GcsStorageOptions Example:")
    print("-" * 40)

    # Create GCS storage options
    gcs_options = GcsStorageOptions(
        protocol="gs",
        project="demo-project",
        token="path/to/service-account.json",  # This would be a real file path in practice
    )
    print(
        f"Created GcsStorageOptions: protocol='{gcs_options.protocol}', project='{gcs_options.project}'"
    )

    # Convert to fsspec filesystem
    try:
        gcs_fs = gcs_options.to_filesystem()
        print(f"Created fsspec filesystem: {type(gcs_fs).__name__}")

        # Try to list buckets (this will fail without real credentials)
        try:
            buckets = gcs_fs.ls("")
            print(f"Available buckets: {buckets}")
        except Exception as e:
            print(f"Could not connect to GCS (expected without real credentials): {e}")
    except Exception as e:
        print(f"Could not create GCS filesystem: {e}")

    print("GCS storage example completed.\n")

    # 5. StorageOptions Factory Example
    print("5. StorageOptions Factory Example:")
    print("-" * 40)

    # Create storage options using the factory class
    storage_options = StorageOptions.create(protocol="file", auto_mkdir=True)
    print(
        f"Created StorageOptions using factory: protocol='{storage_options.storage_options.protocol}'"
    )

    # Convert to fsspec filesystem
    factory_fs = storage_options.to_filesystem()
    print(f"Created fsspec filesystem: {type(factory_fs).__name__}")

    # Create a temporary file for demonstration
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
        temp_file.write("Hello from StorageOptions factory!")
        temp_file_path = temp_file.name

    try:
        # Read the file using the filesystem
        with factory_fs.open(temp_file_path, "r") as f:
            content = f.read()
        print(f"Read from temp file: '{content}'")
    finally:
        # Clean up
        os.unlink(temp_file_path)

    print("StorageOptions factory example completed.\n")

    print("=== All Examples Completed ===")


if __name__ == "__main__":
    main()
