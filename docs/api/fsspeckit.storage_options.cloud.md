# `fsspeckit.storage_options.cloud` API Documentation

This module defines storage option classes for various cloud providers, including Azure, Google Cloud Storage (GCS), and Amazon Web Services (AWS) S3. These classes provide structured ways to configure access to cloud storage, supporting different authentication methods and specific cloud service parameters.

---

## `AzureStorageOptions`

Azure Storage configuration options.

Provides configuration for Azure storage services:

 - Azure Blob Storage (`az://`)
 - Azure Data Lake Storage Gen2 (`abfs://`)
 - Azure Data Lake Storage Gen1 (`adl://`)

Supports multiple authentication methods:

- Connection string
- Account key
- Service principal
- Managed identity
- SAS token

**Attributes:**

*   `protocol` (`str`): Storage protocol ("az", "abfs", or "adl")
*   `account_name` (`str`): Storage account name
*   `account_key` (`str`): Storage account access key
*   `connection_string` (`str`): Full connection string
*   `tenant_id` (`str`): Azure AD tenant ID
*   `client_id` (`str`): Service principal client ID
*   `client_secret` (`str`): Service principal client secret
*   `sas_token` (`str`): SAS token for limited access

**Example:**

```python
from fsspeckit.storage_options.cloud import AzureStorageOptions

# Blob Storage with account key
options = AzureStorageOptions(
    protocol="az",
    account_name="mystorageacct",
    account_key="key123..."
)

# Data Lake with service principal
options = AzureStorageOptions(
    protocol="abfs",
    account_name="mydatalake",
    tenant_id="tenant123",
    client_id="client123",
    client_secret="secret123"
)

# Simple connection string auth
options = AzureStorageOptions(
    protocol="az",
    connection_string="DefaultEndpoints..."
)
```

### `from_env()`

Create storage options from environment variables.

Reads standard Azure environment variables:

- `AZURE_STORAGE_PROTOCOL`
- `AZURE_STORAGE_ACCOUNT_NAME`
- `AZURE_STORAGE_ACCOUNT_KEY`
- `AZURE_STORAGE_CONNECTION_STRING`
- `AZURE_TENANT_ID`
- `AZURE_CLIENT_ID`
- `AZURE_CLIENT_SECRET`
- `AZURE_STORAGE_SAS_TOKEN`

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `AzureStorageOptions` | `AzureStorageOptions` | Configured storage options |

**Example:**

```python
# With environment variables set:
from fsspeckit.storage_options.cloud import AzureStorageOptions
import os

# Set environment variables for testing (replace with actual values if needed)
os.environ["AZURE_STORAGE_ACCOUNT_NAME"] = "mystorageacct"
os.environ["AZURE_STORAGE_ACCOUNT_KEY"] = "dummy_key" # Dummy key for example

options = AzureStorageOptions.from_env()
print(options.account_name)  # From AZURE_STORAGE_ACCOUNT_NAME
# 'mystorageacct'

# Clean up environment variables
del os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
del os.environ["AZURE_STORAGE_ACCOUNT_KEY"]
```

### `to_env()`

Export options to environment variables.

Sets standard Azure environment variables.

**Example:**

```python
from fsspeckit.storage_options.cloud import AzureStorageOptions
import os

options = AzureStorageOptions(
    protocol="az",
    account_name="mystorageacct",
    account_key="key123"
)
options.to_env()
print(os.getenv("AZURE_STORAGE_ACCOUNT_NAME"))
# 'mystorageacct'

# Clean up environment variables
del os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
del os.environ["AZURE_STORAGE_ACCOUNT_KEY"]
```

---

## `GcsStorageOptions`

Google Cloud Storage configuration options.

Provides configuration for GCS access with support for:

- Service account authentication
- Default application credentials
- Token-based authentication
- Project configuration
- Custom endpoints

**Attributes:**

*   `protocol` (`str`): Storage protocol ("gs" or "gcs")
*   `token` (`str`): Path to service account JSON file
*   `project` (`str`): Google Cloud project ID
*   `access_token` (`str`): OAuth2 access token
*   `endpoint_url` (`str`): Custom storage endpoint
*   `timeout` (`int`): Request timeout in seconds

**Example:**

```python
from fsspeckit.storage_options.cloud import GcsStorageOptions

# Service account auth
options = GcsStorageOptions(
    protocol="gs",
    token="path/to/service-account.json",
    project="my-project-123"
)

# Application default credentials
options = GcsStorageOptions(
    protocol="gcs",
    project="my-project-123"
)

# Custom endpoint (e.g., test server)
options = GcsStorageOptions(
    protocol="gs",
    endpoint_url="http://localhost:4443",
    token="test-token.json"
)
```

### `from_env()`

Create storage options from environment variables.

Reads standard GCP environment variables:

- `GOOGLE_CLOUD_PROJECT`: Project
- `GOOGLE_APPLICATION_CREDENTIALS`: Service account file path
- `STORAGE_EMULATOR_HOST`: Custom endpoint (for testing)
- `GCS_OAUTH_TOKEN`: OAuth2 access token

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `GcsStorageOptions` | `GcsStorageOptions` | Configured storage options |

**Example:**

```python
# With environment variables set:
from fsspeckit.storage_options.cloud import GcsStorageOptions
import os

# Set environment variables for testing (replace with actual values if needed)
os.environ["GOOGLE_CLOUD_PROJECT"] = "my-project-123"

options = GcsStorageOptions.from_env()
print(options.project)  # From GOOGLE_CLOUD_PROJECT
# 'my-project-123'

# Clean up environment variables
del os.environ["GOOGLE_CLOUD_PROJECT"]
```

### `to_env()`

Export options to environment variables.

Sets standard GCP environment variables.

**Example:**

```python
from fsspeckit.storage_options.cloud import GcsStorageOptions
import os

options = GcsStorageOptions(
    protocol="gs",
    project="my-project",
    token="service-account.json"
)
options.to_env()
print(os.getenv("GOOGLE_CLOUD_PROJECT"))
# 'my-project'

# Clean up environment variables
del os.environ["GOOGLE_CLOUD_PROJECT"]
```

### `to_fsspec_kwargs()`

Convert options to fsspec filesystem arguments.

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `dict` | `dict` | Arguments suitable for GCSFileSystem |

**Example:**

```python
from fsspeckit.storage_options.cloud import GcsStorageOptions
from fsspeckit.core.base import filesystem

options = GcsStorageOptions(
    protocol="gs",
    token="service-account.json",
    project="my-project"
)
kwargs = options.to_fsspec_kwargs()
fs = filesystem("gcs", **kwargs)
```

---

## `AwsStorageOptions`

AWS S3 storage configuration options.

Provides comprehensive configuration for S3 access with support for:

- Multiple authentication methods (keys, profiles, environment)
- Custom endpoints for S3-compatible services
- Region configuration
- SSL/TLS settings
- Anonymous access for public buckets

**Attributes:**

*   `protocol` (`str`): Always "s3" for S3 storage
*   `access_key_id` (`str`): AWS access key ID
*   `secret_access_key` (`str`): AWS secret access key
*   `session_token` (`str`): AWS session token
*   `endpoint_url` (`str`): Custom S3 endpoint URL
*   `region` (`str`): AWS region name
*   `allow_invalid_certificates` (`bool`): Skip SSL certificate validation
*   `allow_http` (`bool`): Allow unencrypted HTTP connections
*   `anonymous` (`bool`): Use anonymous (unsigned) S3 access

**Example:**

```python
# Basic credentials
options = AwsStorageOptions(
    access_key_id="AKIAXXXXXXXX",
    secret_access_key="SECRETKEY",
    region="us-east-1"
)

# Profile-based auth
options = AwsStorageOptions.create(profile="dev")

# S3-compatible service (MinIO)
options = AwsStorageOptions(
    endpoint_url="http://localhost:9000",
    access_key_id="minioadmin",
    secret_access_key="minioadmin",
    allow_http=True
)

# Anonymous access for public buckets
options = AwsStorageOptions(anonymous=True)
```

### `create()`

Creates an `AwsStorageOptions` instance, handling aliases and profile loading.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `protocol` | `str` | Storage protocol, defaults to "s3". |
| `access_key_id` | `str | None` | AWS access key ID. |
| `secret_access_key` | `str | None` | AWS secret access key. |
| `session_token` | `str | None` | AWS session token. |
| `endpoint_url` | `str | None` | Custom S3 endpoint URL. |
| `region` | `str | None` | AWS region name. |
| `allow_invalid_certificates` | `bool | None` | Skip SSL certificate validation. |
| `allow_http` | `bool | None` | Allow unencrypted HTTP connections. |
| `anonymous` | `bool | None` | Use anonymous (unsigned) S3 access. |
| `key` | `str | None` | Alias for `access_key_id`. |
| `secret` | `str | None` | Alias for `secret_access_key`. |
| `token` | `str | None` | Alias for `session_token`. |
| `profile` | `str | None` | AWS credentials profile name to load credentials from. |


| Returns | Type | Description |
| :------ | :--- | :---------- |
| `AwsStorageOptions` | `AwsStorageOptions` | An initialized `AwsStorageOptions` instance. |

### `from_aws_credentials()`

Create storage options from AWS credentials file.

Loads credentials from `~/.aws/credentials` and `~/.aws/config` files.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `profile` | `str` | AWS credentials profile name |
| `allow_invalid_certificates` | `bool` | Skip SSL certificate validation |
| `allow_http` | `bool` | Allow unencrypted HTTP connections |
| `anonymous` | `bool` | Use anonymous (unsigned) S3 access |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `AwsStorageOptions` | `AwsStorageOptions` | Configured storage options |

| Raises | Type | Description |
| :----- | :--- | :---------- |
| `ValueError` | `ValueError` | If profile not found |
| `FileNotFoundError` | `FileNotFoundError` | If credentials files missing |

**Example:**

```python
# Load developer profile
options = AwsStorageOptions.from_aws_credentials(
    profile="dev",
    allow_http=True  # For local testing
)
```

### `from_env()`

Create storage options from environment variables.

Reads standard AWS environment variables:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_SESSION_TOKEN`
- `AWS_ENDPOINT_URL`
- `AWS_DEFAULT_REGION`
- `ALLOW_INVALID_CERTIFICATE`
- `AWS_ALLOW_HTTP`
- `AWS_S3_ANONYMOUS`

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `AwsStorageOptions` | `AwsStorageOptions` | Configured storage options |

**Example:**

```python
# Load from environment
from fsspeckit.storage_options.cloud import AwsStorageOptions
import os

# Set environment variables for testing (replace with actual values if needed)
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

options = AwsStorageOptions.from_env()
print(options.region)
# 'us-east-1'  # From AWS_DEFAULT_REGION

# Clean up environment variables
del os.environ["AWS_DEFAULT_REGION"]
```

### `to_fsspec_kwargs()`

Convert options to fsspec filesystem arguments.

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `dict` | `dict` | Arguments suitable for fsspec S3FileSystem |

**Example:**

```python
options = AwsStorageOptions(
    access_key_id="KEY",
    secret_access_key="SECRET",
    region="us-west-2"
)
kwargs = options.to_fsspec_kwargs()
fs = filesystem("s3", **kwargs)
```

### `to_object_store_kwargs()`

Convert options to object store arguments.

| Parameter | Type | Description |
| :-------- | :--- | :---------- |
| `with_conditional_put` | `bool` | Add etag-based conditional put support |

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `dict` | `dict` | Arguments suitable for object store clients |

**Example:**

```python
from fsspeckit.storage_options.cloud import AwsStorageOptions
# Assuming ObjectStore is a hypothetical client for demonstration
# from some_object_store_library import ObjectStore

options = AwsStorageOptions(
    access_key_id="KEY",
    secret_access_key="SECRET"
)
kwargs = options.to_object_store_kwargs()
# client = ObjectStore(**kwargs)
```

### `to_env()`

Export options to environment variables.

Sets standard AWS environment variables.

**Example:**

```python
from fsspeckit.storage_options.cloud import AwsStorageOptions
import os

options = AwsStorageOptions(
    access_key_id="KEY",
    secret_access_key="SECRET",
    region="us-east-1"
)
options.to_env()
print(os.getenv("AWS_ACCESS_KEY_ID"))
# 'KEY'

# Clean up environment variables
del os.environ["AWS_ACCESS_KEY_ID"]
del os.environ["AWS_SECRET_ACCESS_KEY"]
if "AWS_DEFAULT_REGION" in os.environ: # Only delete if it was set
    del os.environ["AWS_DEFAULT_REGION"]
```

### `to_filesystem()`

Create fsspec filesystem instance from options.

| Returns | Type | Description |
| :------ | :--- | :---------- |
| `AbstractFileSystem` | `AbstractFileSystem` | Configured filesystem instance |