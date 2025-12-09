from typing import Any

import msgspec
import yaml
from fsspec import AbstractFileSystem
from fsspec import filesystem as fsspec_filesystem


class BaseStorageOptions(msgspec.Struct, frozen=False):
    """Base class for filesystem storage configuration options.

    Provides common functionality for all storage option classes including:
    - YAML serialization/deserialization
    - Dictionary conversion
    - Filesystem instance creation
    - Configuration updates

    Attributes:
        protocol (str): Storage protocol identifier (e.g., "s3", "gs", "file")

    Example:
        ```python
        # Create and save options
        options = BaseStorageOptions(protocol="s3")
        options.to_yaml("config.yml")

        # Load from YAML
        loaded = BaseStorageOptions.from_yaml("config.yml")
        print(loaded.protocol)
        # 's3'
        ```
    """

    protocol: str

    def to_dict(self, with_protocol: bool = False) -> dict:
        """Convert storage options to dictionary.

        Args:
            with_protocol: Whether to include protocol in output dictionary

        Returns:
            dict: Dictionary of storage options with non-None values

        Example:
            ```python
            options = BaseStorageOptions(protocol="s3")
            print(options.to_dict())
            # {}
            print(options.to_dict(with_protocol=True))
            # {'protocol': 's3'}
            ```
        """
        data = msgspec.structs.asdict(self)
        result = {}
        for key, value in data.items():
            if value is None:
                continue

            if key == "protocol":
                if with_protocol:
                    result[key] = value
            else:
                result[key] = value
        return result

    @classmethod
    def from_yaml(
        cls, path: str, fs: AbstractFileSystem = None
    ) -> "BaseStorageOptions":
        """Load storage options from YAML file.

        Args:
            path: Path to YAML configuration file
            fs: Filesystem to use for reading file

        Returns:
            BaseStorageOptions: Loaded storage options instance

        Example:
            ```python
            # Load from local file
            options = BaseStorageOptions.from_yaml("config.yml")
            print(options.protocol)
            # 's3'
            ```
        """
        if fs is None:
            fs = fsspec_filesystem("file")
        with fs.open(path) as f:
            data = yaml.safe_load(f)
        return cls(**data)

    def to_yaml(self, path: str, fs: AbstractFileSystem = None) -> None:
        """Save storage options to YAML file.

        Args:
            path: Path where to save configuration
            fs: Filesystem to use for writing

        Example:
            ```python
            options = BaseStorageOptions(protocol="s3")
            options.to_yaml("config.yml")
            ```
        """
        if fs is None:
            fs = fsspec_filesystem("file")
        data = self.to_dict()
        with fs.open(path, "w") as f:
            yaml.safe_dump(data, f)

    def to_filesystem(
        self,
        use_listings_cache: bool = True,
        skip_instance_cache: bool = False,
        **kwargs: Any,
    ) -> AbstractFileSystem:
        """Create fsspec filesystem instance from options.

        Args:
            use_listings_cache: Whether to enable the fsspec listings cache.
            skip_instance_cache: Whether to skip fsspec's instance cache.
            **kwargs: Additional arguments forwarded to ``fsspec.filesystem``.

        Returns:
            AbstractFileSystem: Configured filesystem instance

        Example:
            ```python
            options = BaseStorageOptions(protocol="file")
            fs = options.to_filesystem()
            files = fs.ls("/path/to/data")
            ```
        """

        fsspec_kwargs: dict[str, Any]
        if hasattr(self, "to_fsspec_kwargs"):
            to_kwargs = getattr(self, "to_fsspec_kwargs")
            try:
                fsspec_kwargs = to_kwargs(
                    use_listings_cache=use_listings_cache,
                    skip_instance_cache=skip_instance_cache,
                )
            except TypeError:
                fsspec_kwargs = to_kwargs()
        else:
            fsspec_kwargs = self.to_dict(with_protocol=False)

        merged_kwargs: dict[str, Any] = {}
        if fsspec_kwargs:
            merged_kwargs.update(fsspec_kwargs)

        merged_kwargs.setdefault("use_listings_cache", use_listings_cache)
        merged_kwargs.setdefault("skip_instance_cache", skip_instance_cache)
        merged_kwargs.update({k: v for k, v in kwargs.items() if v is not None})

        filtered_kwargs = {k: v for k, v in merged_kwargs.items() if v is not None}

        return fsspec_filesystem(self.protocol, **filtered_kwargs)

    def update(self, **kwargs: Any) -> "BaseStorageOptions":
        """Update storage options with new values.

        Args:
            **kwargs: New option values to set

        Returns:
            BaseStorageOptions: Updated instance

        Example:
            ```python
            options = BaseStorageOptions(protocol="s3")
            options = options.update(region="us-east-1")
            print(options.region)
            # 'us-east-1'
            ```
        """
        return msgspec.structs.replace(self, **kwargs)
