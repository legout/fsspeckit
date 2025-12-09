import configparser
import os
import warnings
from typing import Any

from fsspec import AbstractFileSystem
from fsspec import filesystem as fsspec_filesystem
from obstore.fsspec import FsspecStore
from obstore.store import S3Store

from .base import BaseStorageOptions


def _parse_bool(value: Any) -> bool | None:
    """Parse a boolean-like value from env/config entries.

    Accepts actual booleans, strings (case-insensitive), and returns ``None``
    when the value cannot be interpreted.
    """

    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    return None


class AzureStorageOptions(BaseStorageOptions, frozen=False):
    """Azure Storage configuration options.

    Provides configuration for Azure storage services:
    - Azure Blob Storage (az://)
    - Azure Data Lake Storage Gen2 (abfs://)
    - Azure Data Lake Storage Gen1 (adl://)

    Supports multiple authentication methods:
    - Connection string
    - Account key
    - Service principal
    - Managed identity
    - SAS token

    Attributes:
        protocol (str): Storage protocol ("az", "abfs", or "adl")
        account_name (str): Storage account name
        account_key (str): Storage account access key
        connection_string (str): Full connection string
        tenant_id (str): Azure AD tenant ID
        client_id (str): Service principal client ID
        client_secret (str): Service principal client secret
        sas_token (str): SAS token for limited access

    Example:
        >>> # Blob Storage with account key
        >>> options = AzureStorageOptions(
        ...     protocol="az",
        ...     account_name="mystorageacct",
        ...     account_key="key123..."
        ... )
        >>>
        >>> # Data Lake with service principal
        >>> options = AzureStorageOptions(
        ...     protocol="abfs",
        ...     account_name="mydatalake",
        ...     tenant_id="tenant123",
        ...     client_id="client123",
        ...     client_secret="secret123"
        ... )
        >>>
        >>> # Simple connection string auth
        >>> options = AzureStorageOptions(
        ...     protocol="az",
        ...     connection_string="DefaultEndpoints..."
        ... )
    """

    protocol: str
    account_name: str | None = None
    account_key: str | None = None
    connection_string: str | None = None
    tenant_id: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    sas_token: str | None = None

    @property
    def _normalized_protocol(self) -> str:
        """Get the normalized protocol (lowercase with validation)."""
        protocol = (self.protocol or "az").lower()
        if protocol not in {"az", "abfs", "adl"}:
            raise ValueError("Azure protocol must be one of 'az', 'abfs', or 'adl'")
        return protocol

    @classmethod
    def from_env(cls) -> "AzureStorageOptions":
        """Create storage options from environment variables.

        Reads standard Azure environment variables:
        - AZURE_STORAGE_ACCOUNT_NAME
        - AZURE_STORAGE_ACCOUNT_KEY
        - AZURE_STORAGE_CONNECTION_STRING
        - AZURE_TENANT_ID
        - AZURE_CLIENT_ID
        - AZURE_CLIENT_SECRET
        - AZURE_STORAGE_SAS_TOKEN

        Returns:
            AzureStorageOptions: Configured storage options

        Example:
            >>> # With environment variables set:
            >>> options = AzureStorageOptions.from_env()
            >>> print(options.account_name)  # From AZURE_STORAGE_ACCOUNT_NAME
            'mystorageacct'
        """
        return cls(
            protocol=os.getenv("AZURE_STORAGE_PROTOCOL", "az"),
            account_name=os.getenv("AZURE_STORAGE_ACCOUNT_NAME"),
            account_key=os.getenv("AZURE_STORAGE_ACCOUNT_KEY"),
            connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
            tenant_id=os.getenv("AZURE_TENANT_ID"),
            client_id=os.getenv("AZURE_CLIENT_ID"),
            client_secret=os.getenv("AZURE_CLIENT_SECRET"),
            sas_token=os.getenv("AZURE_STORAGE_SAS_TOKEN"),
        )

    def to_env(self) -> None:
        """Export options to environment variables.

        Sets standard Azure environment variables.

        Example:
            >>> options = AzureStorageOptions(
            ...     protocol="az",
            ...     account_name="mystorageacct",
            ...     account_key="key123"
            ... )
            >>> options.to_env()
            >>> print(os.getenv("AZURE_STORAGE_ACCOUNT_NAME"))
            'mystorageacct'
        """
        env = {
            "AZURE_STORAGE_PROTOCOL": self._normalized_protocol,
            "AZURE_STORAGE_ACCOUNT_NAME": self.account_name,
            "AZURE_STORAGE_ACCOUNT_KEY": self.account_key,
            "AZURE_STORAGE_CONNECTION_STRING": self.connection_string,
            "AZURE_TENANT_ID": self.tenant_id,
            "AZURE_CLIENT_ID": self.client_id,
            "AZURE_CLIENT_SECRET": self.client_secret,
            "AZURE_STORAGE_SAS_TOKEN": self.sas_token,
        }
        env = {k: str(v) for k, v in env.items() if v is not None}
        os.environ.update(env)  # type: ignore[arg-type]

    def to_fsspec_kwargs(self) -> dict:
        """Convert options to Azure-compatible fsspec kwargs."""

        kwargs = {
            "account_name": self.account_name,
            "account_key": self.account_key,
            "connection_string": self.connection_string,
            "tenant_id": self.tenant_id,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "sas_token": self.sas_token,
        }
        return {k: v for k, v in kwargs.items() if v is not None}


class GcsStorageOptions(BaseStorageOptions, frozen=False):
    """Google Cloud Storage configuration options.

    Provides configuration for GCS access with support for:
    - Service account authentication
    - Default application credentials
    - Token-based authentication
    - Project configuration
    - Custom endpoints

    Attributes:
        protocol (str): Storage protocol ("gs" or "gcs")
        token (str): Path to service account JSON file
        project (str): Google Cloud project ID
        access_token (str): OAuth2 access token
        endpoint_url (str): Custom storage endpoint
        timeout (int): Request timeout in seconds

    Example:
        >>> # Service account auth
        >>> options = GcsStorageOptions(
        ...     protocol="gs",
        ...     token="path/to/service-account.json",
        ...     project="my-project-123"
        ... )
        >>>
        >>> # Application default credentials
        >>> options = GcsStorageOptions(
        ...     protocol="gcs",
        ...     project="my-project-123"
        ... )
        >>>
        >>> # Custom endpoint (e.g., test server)
        >>> options = GcsStorageOptions(
        ...     protocol="gs",
        ...     endpoint_url="http://localhost:4443",
        ...     token="test-token.json"
        ... )
    """

    protocol: str
    token: str | None = None
    project: str | None = None
    access_token: str | None = None
    endpoint_url: str | None = None
    timeout: int | None = None

    @property
    def _normalized_protocol(self) -> str:
        """Get the normalized protocol (always 'gs' if valid)."""
        protocol = (self.protocol or "gs").lower()
        if protocol not in {"gs", "gcs"}:
            raise ValueError("GCS protocol must be 'gs' or 'gcs'")
        return "gs"

    @classmethod
    def from_env(cls) -> "GcsStorageOptions":
        """Create storage options from environment variables.

        Reads standard GCP environment variables:
        - GOOGLE_CLOUD_PROJECT: Project ID
        - GOOGLE_APPLICATION_CREDENTIALS: Service account file path
        - STORAGE_EMULATOR_HOST: Custom endpoint (for testing)
        - GCS_OAUTH_TOKEN: OAuth2 access token

        Returns:
            GcsStorageOptions: Configured storage options

        Example:
            >>> # With environment variables set:
            >>> options = GcsStorageOptions.from_env()
            >>> print(options.project)  # From GOOGLE_CLOUD_PROJECT
            'my-project-123'
        """
        return cls(
            protocol="gs",
            project=os.getenv("GOOGLE_CLOUD_PROJECT"),
            token=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
            endpoint_url=os.getenv("STORAGE_EMULATOR_HOST"),
            access_token=os.getenv("GCS_OAUTH_TOKEN"),
        )

    def to_env(self) -> None:
        """Export options to environment variables.

        Sets standard GCP environment variables.

        Example:
            >>> options = GcsStorageOptions(
            ...     protocol="gs",
            ...     project="my-project",
            ...     token="service-account.json"
            ... )
            >>> options.to_env()
            >>> print(os.getenv("GOOGLE_CLOUD_PROJECT"))
            'my-project'
        """
        env = {
            "GOOGLE_CLOUD_PROJECT": self.project,
            "GOOGLE_APPLICATION_CREDENTIALS": self.token,
            "STORAGE_EMULATOR_HOST": self.endpoint_url,
            "GCS_OAUTH_TOKEN": self.access_token,
        }
        env = {k: str(v) for k, v in env.items() if v is not None}
        os.environ.update(env)  # type: ignore[arg-type]

    def to_fsspec_kwargs(self) -> dict:
        """Convert options to fsspec filesystem arguments.

        Returns:
            dict: Arguments suitable for GCSFileSystem

        Example:
            >>> options = GcsStorageOptions(
            ...     protocol="gs",
            ...     token="service-account.json",
            ...     project="my-project"
            ... )
            >>> kwargs = options.to_fsspec_kwargs()
            >>> fs = filesystem("gcs", **kwargs)
        """
        kwargs = {
            "token": self.token,
            "project": self.project,
            "access_token": self.access_token,
            "endpoint_url": self.endpoint_url,
            "timeout": self.timeout,
        }
        return {k: v for k, v in kwargs.items() if v is not None}


class AwsStorageOptions(BaseStorageOptions, frozen=False):
    """AWS S3 storage configuration options.

    Provides comprehensive configuration for S3 access with support for:
    - Multiple authentication methods (keys, profiles, environment)
    - Custom endpoints for S3-compatible services
    - Region configuration
    - SSL/TLS settings
    - Anonymous access for public buckets

    Attributes:
        protocol (str): Always "s3" for S3 storage
        access_key_id (str): AWS access key ID
        secret_access_key (str): AWS secret access key
        session_token (str): AWS session token
        endpoint_url (str): Custom S3 endpoint URL
        region (str): AWS region name
        allow_invalid_certificates (bool): Skip SSL certificate validation
        allow_http (bool): Allow unencrypted HTTP connections
        anonymous (bool): Use anonymous (unsigned) S3 access

    Example:
        >>> # Basic credentials
        >>> options = AwsStorageOptions(
        ...     access_key_id="AKIAXXXXXXXX",
        ...     secret_access_key="SECRETKEY",
        ...     region="us-east-1"
        ... )
        >>>
        >>> # Profile-based auth
        >>> options = AwsStorageOptions.create(profile="dev")
        >>>
        >>> # S3-compatible service (MinIO)
        >>> options = AwsStorageOptions(
        ...     endpoint_url="http://localhost:9000",
        ...     access_key_id="minioadmin",
        ...     secret_access_key="minioadmin",
        ...     allow_http=True
        ... )
        >>>
        >>> # Anonymous access for public buckets
        >>> options = AwsStorageOptions(anonymous=True)
    """

    protocol: str = "s3"
    access_key_id: str | None = None
    secret_access_key: str | None = None
    session_token: str | None = None
    endpoint_url: str | None = None
    region: str | None = None
    allow_invalid_certificates: bool | None = None
    allow_invalid_certs: bool | None = None
    allow_http: bool | None = None
    anonymous: bool | None = None

    def __post_init__(self) -> None:
        # Handle deprecation warning for allow_invalid_certs
        if self.allow_invalid_certs is not None:
            warnings.warn(
                "allow_invalid_certs is deprecated; use allow_invalid_certificates",
                DeprecationWarning,
                stacklevel=2,
            )

    @property
    def _parsed_allow_invalid_certificates(self) -> bool | None:
        """Get the parsed allow_invalid_certificates value, handling the deprecated alias."""
        canonical_allow_invalid = _parse_bool(self.allow_invalid_certificates)
        alias_value = _parse_bool(self.allow_invalid_certs)

        if canonical_allow_invalid is None:
            canonical_allow_invalid = alias_value
        elif alias_value is not None and canonical_allow_invalid != alias_value:
            canonical_allow_invalid = alias_value

        return canonical_allow_invalid

    @property
    def _parsed_allow_http(self) -> bool | None:
        """Get the parsed allow_http value."""
        return _parse_bool(self.allow_http)

    @property
    def _parsed_anonymous(self) -> bool | None:
        """Get the parsed anonymous value."""
        return _parse_bool(self.anonymous)

    @classmethod
    def create(
        cls,
        protocol: str = "s3",
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        session_token: str | None = None,
        endpoint_url: str | None = None,
        region: str | None = None,
        allow_invalid_certificates: bool | str | None = None,
        allow_invalid_certs: bool | str | None = None,
        allow_http: bool | str | None = None,
        anonymous: bool | str | None = None,
        # Alias and loading params
        key: str | None = None,
        secret: str | None = None,
        token: str | None = None,  # maps to session_token
        profile: str | None = None,
    ) -> "AwsStorageOptions":
        """Creates an AwsStorageOptions instance, handling aliases and profile loading.

        Args:
            protocol: Storage protocol, defaults to "s3".
            access_key_id: AWS access key ID.
            secret_access_key: AWS secret access key.
            session_token: AWS session token.
            endpoint_url: Custom S3 endpoint URL.
            region: AWS region name.
            allow_invalid_certificates: Skip SSL certificate validation.
            allow_http: Allow unencrypted HTTP connections.
            anonymous: Use anonymous (unsigned) S3 access.
            key: Alias for access_key_id.
            secret: Alias for secret_access_key.
            token: Alias for session_token.
            profile: AWS credentials profile name to load credentials from.

        Returns:
            An initialized AwsStorageOptions instance.
        """
        protocol = protocol or "s3"
        canonical_allow_invalid = _parse_bool(allow_invalid_certificates)
        alias_allow_invalid = _parse_bool(allow_invalid_certs)
        if canonical_allow_invalid is None:
            canonical_allow_invalid = alias_allow_invalid

        allow_http_flag = _parse_bool(allow_http)
        anonymous_flag = _parse_bool(anonymous)

        # Initial values from explicit args or their aliases
        args = {
            "protocol": protocol,
            "access_key_id": access_key_id if access_key_id is not None else key,
            "secret_access_key": secret_access_key
            if secret_access_key is not None
            else secret,
            "session_token": session_token if session_token is not None else token,
            "endpoint_url": endpoint_url,
            "region": region,
            "allow_invalid_certificates": canonical_allow_invalid,
            "allow_invalid_certs": None,
            "allow_http": allow_http_flag,
            "anonymous": anonymous_flag,
        }

        if profile is not None:
            profile_instance = cls.from_aws_credentials(
                profile=profile,
                allow_invalid_certificates=args["allow_invalid_certificates"],
                allow_http=args["allow_http"],
                anonymous=args["anonymous"],
            )
            # Fill in missing values from profile if not already set by direct/aliased args
            if args["access_key_id"] is None:
                args["access_key_id"] = profile_instance.access_key_id
            if args["secret_access_key"] is None:
                args["secret_access_key"] = profile_instance.secret_access_key
            if args["session_token"] is None:
                args["session_token"] = profile_instance.session_token
            if args["endpoint_url"] is None:
                args["endpoint_url"] = profile_instance.endpoint_url
            if args["region"] is None:
                args["region"] = profile_instance.region
            if (
                args["allow_invalid_certificates"] is None
                and profile_instance.allow_invalid_certificates is not None
            ):
                args["allow_invalid_certificates"] = (
                    profile_instance.allow_invalid_certificates
                )
            if args["allow_http"] is None and profile_instance.allow_http is not None:
                args["allow_http"] = profile_instance.allow_http

        # Ensure protocol is 's3' if it somehow became None
        if args["protocol"] is None:
            args["protocol"] = "s3"

        return cls(**args)  # type: ignore[arg-type]

    @classmethod
    def from_aws_credentials(
        cls,
        profile: str,
        allow_invalid_certificates: bool | str | None = None,
        allow_invalid_certs: bool | str | None = None,
        allow_http: bool | str | None = None,
        anonymous: bool | str | None = None,
    ) -> "AwsStorageOptions":
        """Create storage options from AWS credentials file.

        Loads credentials from ~/.aws/credentials and ~/.aws/config files.

        Args:
            profile: AWS credentials profile name
            allow_invalid_certificates: Skip SSL certificate validation
            allow_invalid_certs: Skip SSL certificate validation (deprecated, use allow_invalid_certificates)
            allow_http: Allow unencrypted HTTP connections
            anonymous: Use anonymous (unsigned) S3 access

        Returns:
            AwsStorageOptions: Configured storage options

        Raises:
            ValueError: If profile not found
            FileNotFoundError: If credentials files missing

        Example:
            >>> # Load developer profile
            >>> options = AwsStorageOptions.from_aws_credentials(
            ...     profile="dev",
            ...     allow_http=True  # For local testing
            ... )
        """
        cp = configparser.ConfigParser()
        cp.read(os.path.expanduser("~/.aws/credentials"))
        cp.read(os.path.expanduser("~/.aws/config"))
        if profile not in cp:
            raise ValueError(f"Profile '{profile}' not found in AWS credentials file")

        canonical_allow_invalid = _parse_bool(allow_invalid_certificates)
        alias_allow_invalid = _parse_bool(allow_invalid_certs)
        if canonical_allow_invalid is None:
            canonical_allow_invalid = alias_allow_invalid

        allow_http_flag = _parse_bool(allow_http)
        anonymous_flag = _parse_bool(anonymous)

        return cls(
            protocol="s3",
            access_key_id=cp[profile].get("aws_access_key_id", None),
            secret_access_key=cp[profile].get("aws_secret_access_key", None),
            session_token=cp[profile].get("aws_session_token", None),
            endpoint_url=cp[profile].get("aws_endpoint_url", None)
            or cp[profile].get("endpoint_url", None)
            or cp[profile].get("aws_endpoint", None)
            or cp[profile].get("endpoint", None),
            region=(
                cp[profile].get("region", None)
                or cp[f"profile {profile}"].get("region", None)
                if f"profile {profile}" in cp
                else None
            ),
            allow_invalid_certificates=canonical_allow_invalid,
            allow_invalid_certs=None,
            allow_http=allow_http_flag,
            anonymous=anonymous_flag,
        )

    @classmethod
    def from_env(cls) -> "AwsStorageOptions":
        """Create storage options from environment variables.

        Reads standard AWS environment variables:
        - AWS_ACCESS_KEY_ID
        - AWS_SECRET_ACCESS_KEY
        - AWS_SESSION_TOKEN
        - AWS_ENDPOINT_URL
        - AWS_DEFAULT_REGION
        - ALLOW_INVALID_CERTIFICATES
        - AWS_ALLOW_HTTP
        - AWS_S3_ANONYMOUS

    Returns:
            AwsStorageOptions: Configured storage options

        Example:
            ```python
            # Load from environment
            options = AwsStorageOptions.from_env()
            print(options.region)
            # 'us-east-1'  # From AWS_DEFAULT_REGION
            ```
        """
        return cls(
            access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            session_token=os.getenv("AWS_SESSION_TOKEN"),
            endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
            region=os.getenv("AWS_DEFAULT_REGION"),
            allow_invalid_certificates=_parse_bool(
                os.getenv("ALLOW_INVALID_CERTIFICATES")
            ),
            allow_invalid_certs=None,
            allow_http=_parse_bool(os.getenv("AWS_ALLOW_HTTP")),
            anonymous=_parse_bool(os.getenv("AWS_S3_ANONYMOUS")),
        )

    def to_fsspec_kwargs(
        self,
        use_listings_cache: bool = True,
        skip_instance_cache: bool = False,
    ) -> dict:
        """Convert options to fsspec filesystem arguments.
        Args:
            conditional_put: Enable etag-based conditional puts
            use_listings_cache: Enable fsspec listings cache
            skip_instance_cache: Skip fsspec instance cache

        Returns:
            dict: Arguments suitable for fsspec S3FileSystem

        Example:
            ```python
            options = AwsStorageOptions(
                access_key_id="KEY",
                secret_access_key="SECRET",
                region="us-west-2",
            )
            kwargs = options.to_fsspec_kwargs()
            fs = filesystem("s3", **kwargs)
            ```
        """
        fsspec_kwargs: dict[str, Any] = {
            "endpoint_url": self.endpoint_url,
            "use_listings_cache": use_listings_cache,
            "skip_instance_cache": skip_instance_cache,
        }

        # Handle anonymous access
        if self._parsed_anonymous:
            fsspec_kwargs["anon"] = True
        else:
            # Include credentials only if not anonymous
            fsspec_kwargs.update(
                {
                    "key": self.access_key_id,
                    "secret": self.secret_access_key,
                    "token": self.session_token,
                }
            )

        verify_value = (
            None
            if self._parsed_allow_invalid_certificates is None
            else not self._parsed_allow_invalid_certificates
        )
        use_ssl_value = (
            None if self._parsed_allow_http is None else not self._parsed_allow_http
        )

        client_kwargs = {
            "region_name": self.region,
            "verify": verify_value,
            "use_ssl": use_ssl_value,
        }
        client_kwargs = {k: v for k, v in client_kwargs.items() if v is not None}
        if client_kwargs:
            fsspec_kwargs["client_kwargs"] = client_kwargs

        return {k: v for k, v in fsspec_kwargs.items() if v is not None}

    def to_object_store_kwargs(
        self,
        *,
        with_conditional_put: bool | str = False,
        conditional_put: str | None = None,
    ) -> dict:
        """Convert options to object store arguments."""

        storage_options = {
            k: str(v)
            for k, v in self.to_dict().items()
            if v is not None and k != "protocol"
        }

        cond_value: str | None = None
        if conditional_put is not None:
            cond_value = conditional_put
        elif isinstance(with_conditional_put, str):
            cond_value = with_conditional_put
        elif with_conditional_put:
            cond_value = "etag"

        if cond_value:
            storage_options["conditional_put"] = cond_value

        return storage_options

    def to_storage_options_dict(self, conditional_put: str = "etag") -> dict:
        warnings.warn(
            "to_storage_options_dict is deprecated; use to_object_store_kwargs",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.to_object_store_kwargs(conditional_put=conditional_put)

    def to_obstore_kwargs(
        self,
        bucket: str | None = None,
        prefix: str | None = None,
        conditional_put: str = "etag",
        client_options: dict | None = None,
        **kwargs,
    ) -> dict:
        obstore_kwargs: dict[str, Any] = {
            "bucket": bucket,
            "prefix": prefix,
            "endpoint": self.endpoint_url,
            "conditional_put": conditional_put,
            "region": self.region,
        }

        # Handle anonymous access
        if not self._parsed_anonymous:
            obstore_kwargs.update(
                {
                    "access_key_id": self.access_key_id,
                    "secret_access_key": self.secret_access_key,
                    "session_token": self.session_token,
                }
            )

        merged_client_options = {
            "allow_invalid_certificates": self._parsed_allow_invalid_certificates,
            "allow_http": self._parsed_allow_http,
            **(client_options or {}),
        }
        merged_client_options = {
            k: v for k, v in merged_client_options.items() if v is not None
        }
        if merged_client_options:
            obstore_kwargs["client_options"] = merged_client_options

        obstore_kwargs.update(kwargs)  # type: ignore[arg-type]
        return {k: v for k, v in obstore_kwargs.items() if v is not None}

    def to_env(self) -> None:
        """Export options to environment variables.

        Sets standard AWS environment variables.

        Example:
            ```python
            options = AwsStorageOptions(
                access_key_id="KEY",
                secret_access_key="SECRET",
                region="us-east-1",
            )
            options.to_env()
            print(os.getenv("AWS_ACCESS_KEY_ID"))
            # 'KEY'
            ```
        """
        env = {
            "AWS_ACCESS_KEY_ID": self.access_key_id,
            "AWS_SECRET_ACCESS_KEY": self.secret_access_key,
            "AWS_SESSION_TOKEN": self.session_token,
            "AWS_ENDPOINT_URL": self.endpoint_url,
            "AWS_DEFAULT_REGION": self.region,
            "ALLOW_INVALID_CERTIFICATES": self._parsed_allow_invalid_certificates,
            "AWS_ALLOW_HTTP": self._parsed_allow_http,
            "AWS_S3_ANONYMOUS": self._parsed_anonymous,
        }
        env = {k: str(v) for k, v in env.items() if v is not None}
        os.environ.update(env)  # type: ignore[arg-type]

    def to_fsspec_fs(
        self, use_listings_cache: bool = True, skip_instance_cache: bool = False
    ) -> AbstractFileSystem:
        return fsspec_filesystem(
            self.protocol,
            **self.to_fsspec_kwargs(
                use_listings_cache=use_listings_cache,
                skip_instance_cache=skip_instance_cache,
            ),
        )

    def to_obstore(
        self, bucket: str | None = None, prefix: str | None = None
    ) -> S3Store:
        return S3Store(**self.to_obstore_kwargs(bucket=bucket, prefix=prefix))

    def to_obstore_fsspec(self) -> FsspecStore:
        kwargs = self.to_obstore_kwargs()
        kwargs.pop("bucket", None)
        kwargs.pop("prefix", None)
        return FsspecStore("s3", **kwargs)

    @property
    def fsspec_fs(self) -> AbstractFileSystem:
        return self.to_fsspec_fs()

    @property
    def storage_options(self) -> dict:
        return self.to_object_store_kwargs()
