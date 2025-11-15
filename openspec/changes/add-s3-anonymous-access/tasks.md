# Implementation Tasks

## 1. Implementation

- [x] 1.1 Add an `anonymous: bool | None` field to `AwsStorageOptions` with
        parsing in `__post_init__` (using `_parse_bool`).
- [x] 1.2 Extend `AwsStorageOptions.create`, `.from_env`, and
        `.from_aws_credentials` to accept/produce the `anonymous` flag (env var
        name `AWS_S3_ANONYMOUS`).
- [x] 1.3 Update `to_fsspec_kwargs` / `to_fsspec_fs` to inject `anon=True` and
        suppress credential keys whenever `anonymous` is truthy.
- [x] 1.4 Ensure `to_obstore_kwargs` and `to_obstore_fsspec` propagate the
        anonymous flag so object-store adapters run unsigned.
- [x] 1.5 Add docstrings/comments explaining when to use anonymous mode and how
        it interacts with existing credential fields.

## 2. Testing

- [x] 2.1 Unit test that explicit `AwsStorageOptions(anonymous=True)` yields
        `anon=True` and no credentials in `to_fsspec_kwargs`.
- [x] 2.2 Unit test that providing credentials + `anonymous=True` still results
        in unsigned mode (keys removed, anon flag set).
- [x] 2.3 Unit test `AwsStorageOptions.from_env()` parses `AWS_S3_ANONYMOUS` and
        supports the standard truthy/falsey strings.
- [x] 2.4 Unit test object-store helpers (`to_obstore_kwargs` /
        `to_obstore_fsspec`) pass through anonymous mode without requiring
        credentials.
- [x] 2.5 Regression test ensuring default behavior (no anonymous flag)
        continues to include credentials.

## 3. Documentation

- [x] 3.1 Update README / docs storage section with a public bucket example
        showing `anonymous=True` usage.
- [x] 3.2 Document the new `anonymous` option and the `AWS_S3_ANONYMOUS`
        environment variable in API/docstrings.

## 4. Validation

- [x] 4.1 Run targeted storage option tests (e.g.
        `pytest tests/test_storage_options -k aws`).
- [x] 4.2 Run `ruff check src/fsspeckit/storage_options/cloud.py`.
- [x] 4.3 Run `mypy src/fsspeckit/storage_options/cloud.py`.
- [x] 4.4 Validate OpenSpec: `openspec validate add-s3-anonymous-access --strict`.
