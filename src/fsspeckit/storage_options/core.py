"""Storage configuration options for cloud providers and services.

This module provides comprehensive configuration classes for various storage
backends, including AWS S3, Google Cloud Storage, Azure Storage, GitHub,
GitLab, and local filesystems.
"""

from typing import Any

import msgspec
import yaml
from fsspec import AbstractFileSystem
from fsspec import filesystem as fsspec_filesystem
from fsspec.utils import infer_storage_options

from .base import BaseStorageOptions
from .cloud import AwsStorageOptions, AzureStorageOptions, GcsStorageOptions

# Import Git storage options
from .git import GitHubStorageOptions, GitLabStorageOptions


class LocalStorageOptions(BaseStorageOptions):
    """Local filesystem configuration options.

    Provides basic configuration for local file access. While this class
    is simple, it maintains consistency with other storage options and
    enables transparent switching between local and remote storage.

    Attributes:
        protocol (str): Always "file" for local filesystem
        auto_mkdir (bool): Create directories automatically
        mode (int): Default file creation mode (unix-style)

    Example:
        ```python
        # Basic local access
        options = LocalStorageOptions()
        fs = options.to_filesystem()
        files = fs.ls("/path/to/data")

        # With auto directory creation
        options = LocalStorageOptions(auto_mkdir=True)
        fs = options.to_filesystem()
        with fs.open("/new/path/file.txt", "w") as f:
            f.write("test")  # Creates /new/path/ automatically
        ```
    """

    protocol: str = "file"
    auto_mkdir: bool = False
    mode: int | None = None

    def to_fsspec_kwargs(self) -> dict:
        """Convert options to fsspec filesystem arguments.

        Returns:
            dict: Arguments suitable for LocalFileSystem

        Example:
            ```python
            options = LocalStorageOptions(auto_mkdir=True)
            kwargs = options.to_fsspec_kwargs()
            fs = filesystem("file", **kwargs)
            ```
        """
        kwargs = {
            "auto_mkdir": self.auto_mkdir,
            "mode": self.mode,
        }
        return {k: v for k, v in kwargs.items() if v is not None}


# Factory functions
def from_dict(protocol: str, storage_options: dict) -> BaseStorageOptions:
    """Create appropriate storage options instance from dictionary.

    Factory function that creates the correct storage options class based on protocol.

    Args:
        protocol: Storage protocol identifier (e.g., "s3", "gs", "file")
        storage_options: Dictionary of configuration options

    Returns:
        BaseStorageOptions: Appropriate storage options instance

    Raises:
        ValueError: If protocol is not supported

    Example:
        ```python
        # Create S3 options
        options = from_dict(
            "s3",
            {
                "access_key_id": "KEY",
                "secret_access_key": "SECRET",
            },
        )
        print(type(options).__name__)
        # 'AwsStorageOptions'
        ```
    """
    if protocol == "s3":
        # Handle anon -> anonymous parameter translation
        if "anon" in storage_options:
            storage_options = storage_options.copy()
            storage_options["anonymous"] = storage_options.pop("anon")

        if (
            "profile" in storage_options
            or "key" in storage_options
            or "secret" in storage_options
        ):
            return AwsStorageOptions.create(**storage_options)
        return AwsStorageOptions(**storage_options)
    elif protocol in ["az", "abfs", "adl"]:
        return AzureStorageOptions(**storage_options)
    elif protocol in ["gs", "gcs"]:
        return GcsStorageOptions(**storage_options)
    elif protocol == "github":
        return GitHubStorageOptions(**storage_options)
    elif protocol == "gitlab":
        return GitLabStorageOptions(**storage_options)
    elif protocol == "file":
        return LocalStorageOptions(**storage_options)
    else:
        raise ValueError(f"Unsupported protocol: {protocol}")


def from_env(protocol: str) -> BaseStorageOptions:
    """Create storage options from environment variables.

    Factory function that creates and configures storage options from
    protocol-specific environment variables.

    Args:
        protocol: Storage protocol identifier (e.g., "s3", "github")

    Returns:
        BaseStorageOptions: Configured storage options instance

    Raises:
        ValueError: If protocol is not supported

    Example:
        ```python
        # With AWS credentials in environment
        options = from_env("s3")
        print(options.access_key_id)  # From AWS_ACCESS_KEY_ID
        # 'AKIAXXXXXX'
        ```
    """
    if protocol == "s3":
        return AwsStorageOptions.from_env()
    elif protocol in ["az", "abfs", "adl"]:
        return AzureStorageOptions.from_env()
    elif protocol in ["gs", "gcs"]:
        return GcsStorageOptions.from_env()
    elif protocol == "github":
        return GitHubStorageOptions.from_env()
    elif protocol == "gitlab":
        return GitLabStorageOptions.from_env()
    elif protocol == "file":
        return LocalStorageOptions()
    else:
        raise ValueError(f"Unsupported protocol: {protocol}")


def infer_protocol_from_uri(uri: str) -> str:
    """Infer the storage protocol from a URI string.

    Analyzes the URI to determine the appropriate storage protocol based on
    the scheme or path format.

    Args:
        uri: URI or path string to analyze. Examples:
            - "s3://bucket/path"
            - "gs://bucket/path"
            - "github://org/repo"
            - "/local/path"

    Returns:
        str: Inferred protocol identifier

    Example:
        ```python
        # S3 protocol
        infer_protocol_from_uri("s3://my-bucket/data")
        # 's3'

        # Local file
        infer_protocol_from_uri("/home/user/data")
        # 'file'

        # GitHub repository
        infer_protocol_from_uri("github://microsoft/vscode")
        # 'github'
        ```
    """
    if uri.startswith("s3://"):
        return "s3"
    elif uri.startswith("gs://") or uri.startswith("gcs://"):
        return "gs"
    elif uri.startswith("github://"):
        return "github"
    elif uri.startswith("gitlab://"):
        return "gitlab"
    elif uri.startswith(("az://", "abfs://", "adl://")):
        return uri.split("://")[0]
    else:
        return "file"


def storage_options_from_uri(uri: str) -> BaseStorageOptions:
    """Create storage options instance from a URI string.

    Infers the protocol and extracts relevant configuration from the URI
    to create appropriate storage options.

    Args:
        uri: URI string containing protocol and optional configuration.
            Examples:
            - "s3://bucket/path"
            - "gs://project/bucket/path"
            - "github://org/repo"

    Returns:
        BaseStorageOptions: Configured storage options instance

    Example:
        ```python
        # S3 options
        opts = storage_options_from_uri("s3://my-bucket/data")
        print(opts.protocol)
        # 's3'

        # GitHub options
        opts = storage_options_from_uri("github://microsoft/vscode")
        print(opts.org)
        # 'microsoft'
        print(opts.repo)
        # 'vscode'
        ```
    """
    protocol = infer_protocol_from_uri(uri)
    options = infer_storage_options(uri)

    if protocol == "s3":
        return AwsStorageOptions(protocol=protocol, **options)
    elif protocol in ["gs", "gcs"]:
        return GcsStorageOptions(protocol=protocol, **options)
    elif protocol == "github":
        parts = uri.replace("github://", "").split("/")
        return GitHubStorageOptions(
            protocol=protocol, org=parts[0], repo=parts[1] if len(parts) > 1 else None
        )
    elif protocol == "gitlab":
        parts = uri.replace("gitlab://", "").split("/")
        return GitLabStorageOptions(
            protocol=protocol, project_name=parts[-1] if parts else None
        )
    elif protocol in ["az", "abfs", "adl"]:
        return AzureStorageOptions(protocol=protocol, **options)
    else:
        return LocalStorageOptions()


def merge_storage_options(
    *options: BaseStorageOptions | dict | None, overwrite: bool = True
) -> BaseStorageOptions:
    """Merge multiple storage options into a single configuration.

    Combines options from multiple sources with control over precedence.

    Args:
        *options: Storage options to merge. Can be:
            - BaseStorageOptions instances
            - Dictionaries of options
            - None values (ignored)
        overwrite: Whether later options override earlier ones

    Returns:
        BaseStorageOptions: Combined storage options

    Example:
        ```python
        # Merge with overwrite
        base = AwsStorageOptions(
            region="us-east-1",
            access_key_id="OLD_KEY",
        )
        override = {"access_key_id": "NEW_KEY"}
        merged = merge_storage_options(base, override)
        print(merged.access_key_id)
        # 'NEW_KEY'

        # Preserve existing values
        merged = merge_storage_options(
            base,
            override,
            overwrite=False,
        )
        print(merged.access_key_id)
        # 'OLD_KEY'
        ```
    """
    result = {}
    protocol = None

    for opts in options:
        if opts is None:
            continue
        if isinstance(opts, BaseStorageOptions):
            opts = opts.to_dict(with_protocol=True)
        if not protocol and "protocol" in opts:
            protocol = opts["protocol"]
        for k, v in opts.items():
            if overwrite or k not in result:
                result[k] = v

    if not protocol:
        protocol = "file"
    return from_dict(protocol, result)


class StorageOptions(msgspec.Struct):
    """High-level storage options container and factory.

    Provides a unified interface for creating and managing storage options
    for different protocols.

    Attributes:
        storage_options (BaseStorageOptions): Underlying storage options instance

    Example:
        ```python
        # Create from protocol
        options = StorageOptions.create(
            protocol="s3",
            access_key_id="KEY",
            secret_access_key="SECRET",
        )

        # Create from existing options
        s3_opts = AwsStorageOptions(access_key_id="KEY")
        options = StorageOptions(storage_options=s3_opts)
        ```
    """

    storage_options: BaseStorageOptions

    @classmethod
    def create(cls, **data: Any) -> "StorageOptions":
        """Create storage options from arguments.

        Args:
            **data: Either:
                - protocol and configuration options
                - storage_options=pre-configured instance

        Returns:
            StorageOptions: Configured storage options instance

        Raises:
            ValueError: If protocol missing or invalid

        Example:
            >>> # Direct protocol config
            >>> options = StorageOptions.create(
            ...     protocol="s3",
            ...     region="us-east-1"
            ... )
        """
        protocol = data.get("protocol")
        if protocol is None and "storage_options" not in data:
            raise ValueError("protocol must be specified")

        if "storage_options" not in data:
            if protocol == "s3":
                if "profile" in data or "key" in data or "secret" in data:
                    storage_options = AwsStorageOptions.create(**data)
                else:
                    storage_options = AwsStorageOptions(**data)
            elif protocol == "github":
                storage_options = GitHubStorageOptions(**data)
            elif protocol == "gitlab":
                storage_options = GitLabStorageOptions(**data)
            elif protocol in ["az", "abfs", "adl"]:
                storage_options = AzureStorageOptions(**data)
            elif protocol in ["gs", "gcs"]:
                storage_options = GcsStorageOptions(**data)
            elif protocol == "file":
                storage_options = LocalStorageOptions(**data)
            else:
                raise ValueError(f"Unsupported protocol: {protocol}")

            return cls(storage_options=storage_options)
        else:
            return cls(**data)

    @classmethod
    def from_yaml(cls, path: str, fs: AbstractFileSystem = None) -> "StorageOptions":
        """Create storage options from YAML configuration.

        Args:
            path: Path to YAML configuration file
            fs: Filesystem for reading configuration

        Returns:
            StorageOptions: Configured storage options

        Example:
            >>> # Load from config file
            >>> options = StorageOptions.from_yaml("storage.yml")
            >>> print(options.storage_options.protocol)
            's3'
        """
        if fs is None:
            fs = fsspec_filesystem("file")
        with fs.open(path, "r") as f:
            data = yaml.safe_load(f)
        return cls.create(**data)

    @classmethod
    def from_env(cls, protocol: str) -> "StorageOptions":
        """Create storage options from environment variables.

        Args:
            protocol: Storage protocol to configure

        Returns:
            StorageOptions: Environment-configured options

        Example:
            >>> # Load AWS config from environment
            >>> options = StorageOptions.from_env("s3")
        """
        storage_options = from_env(protocol)
        return cls(storage_options=storage_options)

    def to_filesystem(
        self,
        use_listings_cache: bool = True,
        skip_instance_cache: bool = False,
        **kwargs: Any,
    ) -> AbstractFileSystem:
        """Create fsspec filesystem instance.

        Returns:
            AbstractFileSystem: Configured filesystem instance

        Example:
            >>> options = StorageOptions.create(protocol="file")
            >>> fs = options.to_filesystem()
            >>> files = fs.ls("/data")
        """
        return self.storage_options.to_filesystem(
            use_listings_cache=use_listings_cache,
            skip_instance_cache=skip_instance_cache,
            **kwargs,
        )

    def to_dict(self, with_protocol: bool = False) -> dict:
        """Convert storage options to dictionary.

        Args:
            with_protocol: Whether to include protocol in output

        Returns:
            dict: Storage options as dictionary

        Example:
            >>> options = StorageOptions.create(
            ...     protocol="s3",
            ...     region="us-east-1"
            ... )
            >>> print(options.to_dict())
            {'region': 'us-east-1'}
        """
        return self.storage_options.to_dict(with_protocol=with_protocol)

    def to_object_store_kwargs(self, with_conditional_put: bool = False) -> dict:
        """Get options formatted for object store clients.

        Args:
            with_conditional_put: Add etag-based conditional put support

        Returns:
            dict: Object store configuration dictionary

        Example:
            >>> options = StorageOptions.create(protocol="s3")
            >>> kwargs = options.to_object_store_kwargs()
            >>> # store = ObjectStore(**kwargs)
        """
        if hasattr(self.storage_options, "to_object_store_kwargs"):
            return self.storage_options.to_object_store_kwargs(
                with_conditional_put=with_conditional_put
            )
        else:
            return self.storage_options.to_dict(with_protocol=True)
