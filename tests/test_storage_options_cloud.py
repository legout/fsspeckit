import os

import pytest

from fsspeckit.storage_options.cloud import (
    AwsStorageOptions,
    AzureStorageOptions,
    GcsStorageOptions,
)


def test_aws_object_store_conditional_put_variants():
    options = AwsStorageOptions(access_key_id="key", secret_access_key="secret")

    base_kwargs = options.to_object_store_kwargs()
    assert "conditional_put" not in base_kwargs

    bool_kwargs = options.to_object_store_kwargs(with_conditional_put=True)
    assert bool_kwargs["conditional_put"] == "etag"

    explicit_kwargs = options.to_object_store_kwargs(conditional_put="md5")
    assert explicit_kwargs["conditional_put"] == "md5"


def test_aws_to_storage_options_dict_deprecated_warning():
    options = AwsStorageOptions(access_key_id="key", secret_access_key="secret")
    with pytest.warns(DeprecationWarning):
        deprecated = options.to_storage_options_dict()

    assert deprecated["conditional_put"] == "etag"
    assert deprecated["access_key_id"] == "key"


def test_aws_allow_invalid_certs_alias_warns():
    with pytest.warns(DeprecationWarning):
        options = AwsStorageOptions(allow_invalid_certs=True)

    assert options._parsed_allow_invalid_certificates is True
    # allow_invalid_certs field is no longer modified, we check the original input value
    assert options.allow_invalid_certs is True


def test_aws_to_fsspec_kwargs_filters_none_values():
    options = AwsStorageOptions(
        access_key_id="key",
        secret_access_key="secret",
        region="us-east-1",
    )

    kwargs = options.to_fsspec_kwargs()
    assert kwargs["client_kwargs"]["region_name"] == "us-east-1"
    assert "verify" not in kwargs["client_kwargs"]
    assert "use_ssl" not in kwargs["client_kwargs"]

    options = AwsStorageOptions(
        access_key_id="key",
        secret_access_key="secret",
        allow_invalid_certificates=True,
        allow_http=True,
    )
    kwargs = options.to_fsspec_kwargs()
    client_kwargs = kwargs["client_kwargs"]
    assert client_kwargs["verify"] is False
    assert client_kwargs["use_ssl"] is False


def test_aws_to_obstore_kwargs_sanitises_client_options():
    base = AwsStorageOptions()
    base_kwargs = base.to_obstore_kwargs()
    assert "client_options" not in base_kwargs
    assert "default_region" not in base_kwargs

    enriched = AwsStorageOptions(
        allow_invalid_certificates=True,
        allow_http=False,
    )
    merged = enriched.to_obstore_kwargs(client_options={"extra": "value"})
    client_opts = merged["client_options"]
    assert client_opts["allow_invalid_certificates"] is True
    assert client_opts["allow_http"] is False
    assert client_opts["extra"] == "value"


def test_aws_env_roundtrip(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "env-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "env-secret")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "env-token")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:9000")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-2")
    monkeypatch.setenv("ALLOW_INVALID_CERTIFICATES", "yes")
    monkeypatch.setenv("AWS_ALLOW_HTTP", "1")

    options = AwsStorageOptions.from_env()
    assert options.access_key_id == "env-key"
    assert options.allow_invalid_certificates is True
    assert options.allow_http is True

    for key in [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_ENDPOINT_URL",
        "AWS_DEFAULT_REGION",
        "ALLOW_INVALID_CERTIFICATES",
        "AWS_ALLOW_HTTP",
    ]:
        monkeypatch.delenv(key, raising=False)

    options.to_env()
    assert os.getenv("AWS_ACCESS_KEY_ID") == "env-key"
    assert os.getenv("ALLOW_INVALID_CERTIFICATES") == "True"
    assert os.getenv("AWS_ALLOW_HTTP") == "True"


def test_gcs_protocol_normalisation():
    options = GcsStorageOptions(protocol="gcs")
    assert options._normalized_protocol == "gs"


def test_azure_protocol_validation_and_kwargs():
    options = AzureStorageOptions(protocol="az", account_name="acct", account_key="key")
    kwargs = options.to_fsspec_kwargs()
    assert kwargs["account_name"] == "acct"
    assert kwargs["account_key"] == "key"

    with pytest.raises(ValueError):
        # Access the property to trigger validation
        invalid_options = AzureStorageOptions(protocol="blob")
        _ = invalid_options._normalized_protocol


def test_aws_anonymous_explicit_flag():
    """Test that explicit anonymous=True yields anon=True and no credentials."""
    options = AwsStorageOptions(anonymous=True)
    kwargs = options.to_fsspec_kwargs()

    assert kwargs["anon"] is True
    assert "key" not in kwargs
    assert "secret" not in kwargs
    assert "token" not in kwargs


def test_aws_anonymous_overrides_credentials():
    """Test that providing credentials with anonymous=True still results in unsigned mode."""
    options = AwsStorageOptions(
        access_key_id="AKIATEST",
        secret_access_key="secret123",
        session_token="token456",
        anonymous=True,
    )
    kwargs = options.to_fsspec_kwargs()

    assert kwargs["anon"] is True
    assert "key" not in kwargs
    assert "secret" not in kwargs
    assert "token" not in kwargs


def test_aws_anonymous_from_env_parsing(monkeypatch: pytest.MonkeyPatch):
    """Test that from_env() parses AWS_S3_ANONYMOUS correctly."""
    # Test truthy values
    for truthy_value in ["1", "true", "yes", "y", "on"]:
        monkeypatch.setenv("AWS_S3_ANONYMOUS", truthy_value)
        options = AwsStorageOptions.from_env()
        assert options.anonymous is True
        kwargs = options.to_fsspec_kwargs()
        assert kwargs["anon"] is True

    # Test falsy values
    for falsy_value in ["0", "false", "no", "n", "off"]:
        monkeypatch.setenv("AWS_S3_ANONYMOUS", falsy_value)
        options = AwsStorageOptions.from_env()
        assert options.anonymous is False

    # Test unset
    monkeypatch.delenv("AWS_S3_ANONYMOUS", raising=False)
    options = AwsStorageOptions.from_env()
    assert options.anonymous is None


def test_aws_anonymous_object_store_helpers():
    """Test that object-store helpers pass through anonymous mode."""
    options = AwsStorageOptions(anonymous=True, region="us-east-1")

    # Test to_obstore_kwargs
    obstore_kwargs = options.to_obstore_kwargs()
    assert "access_key_id" not in obstore_kwargs
    assert "secret_access_key" not in obstore_kwargs
    assert "session_token" not in obstore_kwargs
    assert obstore_kwargs["region"] == "us-east-1"

    # Test to_obstore_fsspec
    fsspec_store = options.to_obstore_fsspec()
    # Should not raise an exception and should create a valid store
    assert fsspec_store is not None


def test_aws_anonymous_regression_default_behavior():
    """Regression test ensuring default behavior continues to include credentials."""
    options = AwsStorageOptions(
        access_key_id="AKIATEST", secret_access_key="secret123", region="us-west-2"
    )
    kwargs = options.to_fsspec_kwargs()

    assert "anon" not in kwargs
    assert kwargs["key"] == "AKIATEST"
    assert kwargs["secret"] == "secret123"
    assert kwargs["client_kwargs"]["region_name"] == "us-west-2"


def test_aws_anonymous_create_method():
    """Test that create method accepts and propagates anonymous flag."""
    options = AwsStorageOptions.create(anonymous=True)
    assert options.anonymous is True
    kwargs = options.to_fsspec_kwargs()
    assert kwargs["anon"] is True


def test_aws_anonymous_from_aws_credentials():
    """Test that from_aws_credentials accepts anonymous flag."""
    # This test would require a mock AWS credentials file
    # For now, just test the parameter acceptance
    try:
        options = AwsStorageOptions.from_aws_credentials(
            profile="nonexistent", anonymous=True
        )
    except ValueError:
        # Expected since profile doesn't exist
        pass
    else:
        # If somehow created, should have anonymous flag
        assert options.anonymous is True


def test_aws_anonymous_env_roundtrip(monkeypatch: pytest.MonkeyPatch):
    """Test environment roundtrip includes anonymous flag."""
    monkeypatch.setenv("AWS_S3_ANONYMOUS", "yes")

    options = AwsStorageOptions.from_env()
    assert options.anonymous is True

    # Clear env and test to_env
    monkeypatch.delenv("AWS_S3_ANONYMOUS", raising=False)
    options.to_env()
    assert os.getenv("AWS_S3_ANONYMOUS") == "True"


def test_aws_anonymous_parameter_translation():
    """Test that anon parameter is translated to anonymous in from_dict."""
    from fsspeckit.storage_options.core import from_dict

    # Test anon -> anonymous translation
    storage_opts = from_dict("s3", {"anon": True})
    assert isinstance(storage_opts, AwsStorageOptions)
    assert storage_opts.anonymous is True

    # Test that both anon and anonymous work
    storage_opts = from_dict("s3", {"anonymous": True})
    assert isinstance(storage_opts, AwsStorageOptions)
    assert storage_opts.anonymous is True

    # Test that anon takes precedence if both provided
    storage_opts = from_dict("s3", {"anon": True, "anonymous": False})
    assert isinstance(storage_opts, AwsStorageOptions)
    assert storage_opts.anonymous is True
