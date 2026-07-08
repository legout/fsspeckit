# Configure Cloud Storage

This guide shows you how to configure cloud storage providers for use with fsspeckit. You'll learn how to use environment variables, structured configuration classes, and URI-based setup.

## Environment-Based Configuration

The recommended approach for production deployments is to load configuration from environment variables.

### AWS S3

```python
from fsspeckit import filesystem
from fsspeckit.storage_options import storage_options_from_env

# Set environment variables
import os
os.environ["AWS_ACCESS_KEY_ID"] = "your_access_key"
os.environ["AWS_SECRET_ACCESS_KEY"] = "your_secret_key"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

# Load AWS options from environment
aws_options = storage_options_from_env("s3")
fs = filesystem("s3", storage_options=aws_options.to_dict())

print(f"Created S3 filesystem in region: {aws_options.region}")
```

### Google Cloud Storage

```python
# Set environment variables
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/path/to/service-account.json"
os.environ["GOOGLE_CLOUD_PROJECT"] = "your-gcp-project"

# Load GCS options from environment
gcs_options = storage_options_from_env("gs")
fs = filesystem("gs", storage_options=gcs_options.to_dict())
```

### Azure Blob Storage

```python
# Set environment variables
os.environ["AZURE_STORAGE_ACCOUNT"] = "your_storage_account"
os.environ["AZURE_STORAGE_KEY"] = "your_storage_key"

# Load Azure options from environment
azure_options = storage_options_from_env("az")
fs = filesystem("az", storage_options=azure_options.to_dict())
```

## Manual Configuration with Storage Options Classes

For more control, use the structured storage options classes.

### AWS S3 Configuration

```python
from fsspeckit import AwsStorageOptions

# Configure AWS S3
aws_options = AwsStorageOptions(
    region="us-east-1",
    access_key_id="YOUR_ACCESS_KEY",
    secret_access_key="YOUR_SECRET_KEY",
    endpoint_url=None,  # Use default endpoint
    allow_http=False,  # Enforce HTTPS
    assume_role_arn=None  # Optional role assumption
)

# Create filesystem
fs = aws_options.to_filesystem()
```

### Google Cloud Storage Configuration

```python
from fsspeckit import GcsStorageOptions

gcs_options = GcsStorageOptions(
    project="your-gcp-project",
    token="path/to/service-account.json",  # or None for default credentials
    endpoint_override=None  # Use default endpoint
)

fs = gcs_options.to_filesystem()
```

### Azure Blob Storage Configuration

```python
from fsspeckit import AzureStorageOptions

azure_options = AzureStorageOptions(
    account_name="yourstorageaccount",
    account_key="YOUR_ACCOUNT_KEY",
    connection_string=None,  # Alternative to account_name/account_key
    sas_token=None  # Optional SAS token
)

fs = azure_options.to_filesystem()
```

### GitHub Repository Configuration

```python
from fsspeckit import GitHubStorageOptions

github_options = GitHubStorageOptions(
    token="github_pat_YOUR_TOKEN",
    default_branch="main"
)

fs = github_options.to_filesystem()
```

### GitLab Repository Configuration

```python
from fsspeckit import GitLabStorageOptions

gitlab_options = GitLabStorageOptions(
    project_id=12345,
    token="glpat_xxx",
    timeout=60.0  # Optional: customize request timeout (default: 30.0)
)

fs = gitlab_options.to_filesystem()
```

The GitLab filesystem now includes hardened features:
- **Automatic URL encoding** for project names and file paths with special characters
- **Automatic pagination** for large directory listings (no more truncated results)
- **Configurable timeouts** to prevent hanging requests
- **Enhanced error logging** for better debugging

## URI-Based Configuration

You can extract storage options directly from URIs, which is useful for configuration files or command-line arguments.

```python
from fsspeckit import filesystem
from fsspeckit.storage_options import storage_options_from_uri

# Extract storage options from URIs
uris = [
    "s3://bucket/path?region=us-east-1&endpoint_url=https://s3.amazonaws.com",
    "gs://bucket/path?project=my-gcp-project",
    "az://container/path?account_name=mystorageaccount"
]

for uri in uris:
    options = storage_options_from_uri(uri)
    fs = filesystem(options.protocol, storage_options=options.to_dict())
    print(f"URI: {uri}")
    print(f"Protocol: {options.protocol}")
    print(f"Filesystem: {type(fs).__name__}")
    print()
```

## Multi-Cloud Configuration

You can configure multiple cloud providers simultaneously:

```python
from fsspeckit import (
    AwsStorageOptions,
    GcsStorageOptions,
    AzureStorageOptions
)

# AWS configuration
aws_config = AwsStorageOptions(
    region="us-west-2",
    access_key_id="aws_key",
    secret_access_key="aws_secret"
)

# Google Cloud configuration
gcs_config = GcsStorageOptions(
    project="gcp-project",
    token="path/to/service-account.json"
)

# Azure configuration
azure_config = AzureStorageOptions(
    account_name="storageaccount",
    account_key="azure_key"
)

# Create filesystems for each provider
aws_fs = aws_config.to_filesystem()
gcs_fs = gcs_config.to_filesystem()
azure_fs = azure_config.to_filesystem()

# Use them interchangeably
for provider, fs in [("AWS", aws_fs), ("GCS", gcs_fs), ("Azure", azure_fs)]:
    print(f"{provider} filesystem: {type(fs).__name__}")
```

## Configuration Methods

All storage option classes provide useful conversion methods:

```python
opts = AwsStorageOptions(...)

# Convert to fsspec kwargs
kwargs = opts.to_fsspec_kwargs()

# Convert to filesystem
fs = opts.to_filesystem()

# Convert to object store kwargs (for deltalake, etc.)
obj_store_kwargs = opts.to_object_store_kwargs()

# Convert to YAML
yaml_str = opts.to_yaml()

# Load from YAML
opts = AwsStorageOptions.from_yaml(yaml_str)

# Convert to environment variables
env = opts.to_env()

# Load from environment
opts = AwsStorageOptions.from_env()
```

## Best Practices

### Production Deployments

1. **Use Environment Variables**: Store credentials in environment variables, not in code
2. **IAM Roles**: Use IAM roles instead of access keys when possible
3. **Endpoint Configuration**: Use appropriate endpoints for your region
4. **HTTPS Only**: Ensure `allow_http=False` for security

### Security

```python
# Good: Use environment variables
aws_options = storage_options_from_env("s3")

# Good: Use structured classes
aws_options = AwsStorageOptions(
    region="us-east-1",
    access_key_id=get_secret("aws_access_key"),
    secret_access_key=get_secret("aws_secret_key")
)

# Avoid: Hardcoded credentials
aws_options = AwsStorageOptions(
    region="us-east-1",
    access_key_id="AKIAIOSFODNN7EXAMPLE",  # Don't do this!
    secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"  # Don't do this!
)
```

### Testing

For testing, use local filesystem or mock services:

```python
from fsspeckit import LocalStorageOptions

# Use local filesystem for testing
local_options = LocalStorageOptions(auto_mkdir=True)
test_fs = local_options.to_filesystem()
```

## Troubleshooting

### Common Issues

1. **Authentication Errors**: Verify credentials and permissions
2. **Region Mismatch**: Ensure correct region configuration
3. **Network Issues**: Check connectivity to cloud endpoints
4. **IAM Permissions**: Verify IAM roles and policies

### Debug Configuration

```python
# Debug storage options
aws_options = AwsStorageOptions(...)
print("FSSpec kwargs:", aws_options.to_fsspec_kwargs())
print("Object store kwargs:", aws_options.to_object_store_kwargs())

# Test filesystem creation
try:
    fs = aws_options.to_filesystem()
    files = fs.ls("/")
    print(f"Successfully connected, found {len(files)} files")
except Exception as e:
    print(f"Connection failed: {e}")
```

For more information on filesystem operations, see [Work with Filesystems](work-with-filesystems.md).