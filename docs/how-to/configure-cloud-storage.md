# Configure Cloud Storage

This guide shows how to configure cloud storage providers for fsspeckit using
environment variables and structured options classes. For the provider
configuration reference, see
[Storage Options](../reference/storage-options.md).

Cloud providers require their respective extra. See the
[extras matrix](../installation.md#optional-extras): `aws` for S3, `gcp` for
Google Cloud Storage, and `azure` for Azure Blob/Data Lake. GitHub and GitLab
require no extra.

## Environment-based configuration

The recommended approach for production is to load configuration from standard
environment variables. The module-level `from_env(protocol)` factory reads the
provider's conventional variables:

```python
import os
from fsspeckit import filesystem, AwsStorageOptions
from fsspeckit.storage_options import from_env

os.environ["AWS_ACCESS_KEY_ID"] = "your_access_key"
os.environ["AWS_SECRET_ACCESS_KEY"] = "your_secret_key"
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

aws_options = from_env("s3")
fs = filesystem("s3://my-bucket/", storage_options=aws_options.to_dict())
```

Each options class also has a `from_env()` classmethod that reads the same
variables for its provider:

```python
from fsspeckit import AwsStorageOptions, GcsStorageOptions, AzureStorageOptions

aws_options = AwsStorageOptions.from_env()
gcs_options = GcsStorageOptions.from_env()
azure_options = AzureStorageOptions.from_env()
```

## Manual configuration

For development or when you need explicit control, construct the options class
directly and pass it to `filesystem()`:

```python
from fsspeckit import filesystem, AwsStorageOptions, GcsStorageOptions, AzureStorageOptions

# AWS S3 (requires the aws extra)
aws_options = AwsStorageOptions(
    region="us-east-1",
    access_key_id="YOUR_ACCESS_KEY",
    secret_access_key="YOUR_SECRET_KEY",
)
aws_fs = filesystem("s3://my-bucket/", storage_options=aws_options.to_dict())

# Google Cloud Storage (requires the gcp extra)
gcs_options = GcsStorageOptions(protocol="gs", project="your-gcp-project")
gcs_fs = filesystem("gs://my-bucket/", storage_options=gcs_options.to_dict())

# Azure Blob Storage (requires the azure extra)
azure_options = AzureStorageOptions(
    protocol="az",
    account_name="yourstorageaccount",
    account_key="YOUR_ACCOUNT_KEY",
)
azure_fs = filesystem("az://my-container/", storage_options=azure_options.to_dict())
```

You can also call `to_filesystem()` on an options object to build the
filesystem directly. Use `to_dict()` when you want a kwargs mapping to pass to
`filesystem()` yourself; use `to_filesystem()` to build the filesystem from
the options object in one step.

## Git providers

GitHub and GitLab filesystems need no extra. They use a token for
authentication:

```python
from fsspeckit import filesystem, GitHubStorageOptions, GitLabStorageOptions

github_options = GitHubStorageOptions(token="github_pat_YOUR_TOKEN")
github_fs = filesystem("github://owner/repo", storage_options=github_options.to_dict())

gitlab_options = GitLabStorageOptions(project_id=12345, token="glpat_xxx")
gitlab_fs = filesystem("gitlab", storage_options=gitlab_options.to_dict())
```

## Choosing a provider from a URI

fsspeckit does not derive cloud credentials from a URI. Configure each
provider explicitly with its options class or `from_env()` as shown above, and
pass the resulting options to `filesystem()`. To pick a provider
programmatically from a URI's scheme, `infer_protocol_from_uri()` returns just
the protocol string:

```python
from fsspeckit.storage_options import infer_protocol_from_uri

protocol = infer_protocol_from_uri("s3://bucket/path")  # "s3"
```

For the full list of option classes and factory helpers, see
[Storage Options](../reference/storage-options.md).

## Merging configurations

`merge_storage_options(*options, overwrite=True)` combines several sources into
one. Later sources win when `overwrite` is true (the default), which lets you
layer a base configuration with per-environment overrides:

```python
from fsspeckit.storage_options import merge_storage_options, from_dict

base = from_env("s3")
override = from_dict("s3", {"region": "us-west-2"})
merged = merge_storage_options(base, override)
```

## Security best practices

- Store credentials in environment variables or a secrets manager, never in
  source. Prefer IAM roles or workload identity over long-lived access keys.
- Use `from_env()` so secrets never appear in code.
- Redact secrets in logs with `scrub_credentials()` from `fsspeckit.common`.

```python
from fsspeckit.common.security import scrub_credentials

safe = scrub_credentials(f"Failed with key={aws_options.access_key_id}")
```

## Related documentation

- [Storage Options](../reference/storage-options.md) - provider classes and
  factory helpers.
- [Work with Filesystems](work-with-filesystems.md) - filesystem creation,
  caching, and extended I/O.
- [Installation](../installation.md#optional-extras) - the extras matrix.
