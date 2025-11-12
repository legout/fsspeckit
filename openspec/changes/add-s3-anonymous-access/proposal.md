# Change Proposal: Allow Anonymous Access for S3 Filesystems

## Why

Many public S3 buckets expose open datasets that can be downloaded without AWS
credentials. Today `AwsStorageOptions` always builds a credentialed
`S3FileSystem` and never emits the `anon=True` flag, so even read-only public
access requires callers to supply fake keys or drop down to raw fsspec APIs.
This makes it impossible to use `filesystem("s3", ...)` for unsigned requests
through the convenience helpers, and it blocks scenarios like reading datasets
from `s3://commoncrawl/` or testing against public MinIO buckets in CI.

## What Changes

- Introduce an `anonymous` flag on `AwsStorageOptions` (constructor, `create`,
  `from_env`, and `from_aws_credentials`) to declare unsigned access.
- When `anonymous=True`, omit key/secret/token fields from generated kwargs and
  pass `anon=True` to fsspec builders plus any `FsspecStore` adapters.
- Allow environment configuration via a truthy `AWS_S3_ANONYMOUS` variable so
  automation can toggle the behavior without code edits.
- Update storage documentation and examples to show how to read public buckets
  using the new option.
- Add tests covering anonymous configurations, env parsing, and ensuring the
  flag does not regress the default authenticated behavior.

## Impact

- Specs: Add new `storage-aws` capability definitions for anonymous access.
- Code: Primarily `src/fsspeckit/storage_options/cloud.py` plus any helpers that
  forward S3 storage options.
- Tests: Enhance `tests/test_storage_options/test_cloud.py` (or similar) with
  anonymous scenarios.
- Docs: Update README / docs sections describing S3 configuration.
- No new dependencies or breaking changes; authenticated flows remain default.
