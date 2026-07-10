# Storage Options

Storage options are structured configuration objects for local, cloud, and Git
providers. This page explains how to choose and configure them. For exact
signatures, see [fsspeckit.storage_options](../api/fsspeckit.storage_options.md).

Cloud providers require their respective extra. See the
[extras matrix](../installation.md#optional-extras).

## Provider classes

Each provider has a dedicated options class. All inherit from
`BaseStorageOptions`.

| Provider | Class | Extra | Notes |
|----------|-------|-------|-------|
| Local | `LocalStorageOptions` | none | Default; no credentials |
| AWS S3 | `AwsStorageOptions` | `aws` | Region, profile, anonymous access |
| Google Cloud Storage | `GcsStorageOptions` | `gcp` | Project, token |
| Azure Blob / Data Lake | `AzureStorageOptions` | `azure` | Account, connection string |
| GitHub | `GitHubStorageOptions` | none | Org, repo, token |
| GitLab | `GitLabStorageOptions` | none | Project, token |

Import from the root package or from `fsspeckit.storage_options`:

```python
from fsspeckit import AwsStorageOptions, GcsStorageOptions, GitHubStorageOptions
```

## Configuring options

### Direct construction

Pass credentials and settings as keyword arguments:

```python
from fsspeckit import AwsStorageOptions

options = AwsStorageOptions(
    region="us-east-1",
    access_key_id="KEY",
    secret_access_key="SECRET",
)
```

### From environment variables

The module-level `from_env(protocol)` factory reads standard environment
variables for each provider:

```python
from fsspeckit.storage_options import from_env

options = from_env("s3")
```

Each provider class also has a `from_env()` classmethod (for example,
`AwsStorageOptions.from_env()`), which reads provider-specific environment
variables.

### From a URI

`storage_options_from_uri(uri)` infers the protocol from the scheme and extracts
available configuration:

```python
from fsspeckit.storage_options import storage_options_from_uri

options = storage_options_from_uri("s3://my-bucket/data")
```

### From a dictionary

`from_dict(protocol, mapping)` builds the right class for a protocol from a
plain dictionary. This is useful when options arrive as configuration data:

```python
from fsspeckit.storage_options import from_dict

options = from_dict("s3", {"region": "us-east-1"})
```

### Merging configurations

`merge_storage_options(*options, overwrite=True)` combines several sources into
one. Later sources win when `overwrite` is true (the default):

```python
from fsspeckit.storage_options import merge_storage_options, AwsStorageOptions

base = AwsStorageOptions(region="us-east-1", access_key_id="OLD")
override = {"access_key_id": "NEW"}
merged = merge_storage_options(base, override)
```

### Protocol inference

`infer_protocol_from_uri(uri)` returns the protocol string for a URI without
building an options object:

```python
from fsspeckit.storage_options import infer_protocol_from_uri

infer_protocol_from_uri("s3://bucket/data")   # "s3"
infer_protocol_from_uri("/local/path")         # "file"
```

## Using options with a filesystem

Pass an options object (or its dict form) to `filesystem()`:

```python
from fsspeckit import filesystem, AwsStorageOptions

options = AwsStorageOptions(region="us-east-1")
fs = filesystem("s3://my-bucket/", storage_options=options.to_dict())
```

For a walkthrough of cloud setup, see
[Configure Cloud Storage](../how-to/configure-cloud-storage.md).

## Related documentation

- [API Guide](api-guide.md) - import selection across all packages.
- [Installation](../installation.md) - extras matrix and provider dependencies.
- [Generated API: storage_options](../api/fsspeckit.storage_options.md) - signatures.
