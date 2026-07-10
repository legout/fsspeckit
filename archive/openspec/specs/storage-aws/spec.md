# storage-aws Specification

## Purpose
TBD - created by archiving change add-s3-anonymous-access. Update Purpose after archive.
## Requirements
### Requirement: Anonymous S3 Access Flag

`AwsStorageOptions` SHALL expose an `anonymous` flag that instructs every
fsspec-based filesystem or store it builds to use unsigned (`anon=True`) S3
requests.

#### Scenario: Public bucket without credentials

- **WHEN** a caller instantiates `AwsStorageOptions(anonymous=True)` without
  providing any credentials
- **AND** calls `options.to_fsspec_kwargs()` / `options.to_fsspec_fs()`
- **THEN** the generated kwargs include `anon=True`
- **AND** no `key`, `secret`, or `token` entries are present
- **AND** `filesystem("s3", **kwargs)` can read from a public bucket.

#### Scenario: Anonymous overrides provided credentials

- **WHEN** a caller passes valid credentials together with `anonymous=True`
- **THEN** the resulting kwargs still set `anon=True`
- **AND** credential fields are omitted so requests remain unsigned
- **AND** the helper never mixes signed and unsigned parameters.

#### Scenario: Default remains credentialed

- **WHEN** `anonymous` is not provided or is falsey
- **THEN** the helpers behave exactly as today (emit credentials, no `anon`
  flag)
- **AND** existing authenticated integrations see no behavioral difference.

### Requirement: Configuration Paths for Anonymous Access

The anonymous flag SHALL be available through every supported configuration
entry point so automation can enable it without custom code.

#### Scenario: Environment variable enables anonymous mode

- **WHEN** `AwsStorageOptions.from_env()` is used with `AWS_S3_ANONYMOUS` set to
  a truthy value ("1", "true", "yes", etc.)
- **THEN** the returned options have `anonymous=True`
- **AND** `to_fsspec_kwargs()` emits `anon=True` even without credentials.

#### Scenario: Factory helpers propagate anonymous flag

- **WHEN** `AwsStorageOptions.create(..., anonymous=True)` or
  `.from_aws_credentials(..., anonymous=True)` is called
- **THEN** the resulting instance stores the flag
- **AND** every downstream helper (`to_fsspec_kwargs`, `to_obstore_kwargs`,
  `to_obstore_fsspec`) propagates it.

#### Scenario: Object-store adapters honor anonymous mode

- **WHEN** `AwsStorageOptions.to_obstore_fsspec()` / `.to_obstore_kwargs()` is
  used while `anonymous=True`
- **THEN** the created store uses unsigned access (passes `anon=True`, skips
  credentials)
- **AND** consumers can interact with public buckets through the higher-level
  store abstractions.

