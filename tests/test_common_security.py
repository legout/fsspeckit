"""Tests for security validation helpers in fsspeckit.common.security."""

import pytest

from fsspeckit.common.security import (
    validate_path,
    validate_compression_codec,
    scrub_credentials,
    scrub_exception,
    safe_format_error,
    validate_columns,
    VALID_COMPRESSION_CODECS,
)


class TestValidatePath:
    """Tests for validate_path function."""

    def test_valid_local_path(self):
        """Valid local paths should pass validation."""
        assert validate_path("/data/file.parquet") == "/data/file.parquet"
        assert validate_path("data/file.parquet") == "data/file.parquet"
        assert validate_path("./data/file.parquet") == "./data/file.parquet"

    def test_valid_s3_path(self):
        """Valid S3 paths should pass validation."""
        assert validate_path("s3://bucket/key/file.parquet") == "s3://bucket/key/file.parquet"

    def test_empty_path_rejected(self):
        """Empty paths should raise ValueError."""
        with pytest.raises(ValueError, match="cannot be empty"):
            validate_path("")
        with pytest.raises(ValueError, match="cannot be empty"):
            validate_path("   ")

    def test_null_byte_rejected(self):
        """Paths with null bytes should raise ValueError."""
        with pytest.raises(ValueError, match="forbidden control character"):
            validate_path("/data/file\x00.parquet")

    def test_control_characters_rejected(self):
        """Paths with control characters should raise ValueError."""
        with pytest.raises(ValueError, match="forbidden control character"):
            validate_path("/data/file\x01.parquet")
        with pytest.raises(ValueError, match="forbidden control character"):
            validate_path("/data/file\x1f.parquet")

    def test_path_traversal_with_base_dir(self, tmp_path):
        """Path traversal attempts should be blocked when base_dir is specified."""
        base = str(tmp_path / "data")

        # Valid path within base
        assert validate_path("subdir/file.parquet", base_dir=base) == "subdir/file.parquet"

        # Path traversal attempt
        with pytest.raises(ValueError, match="escapes base directory"):
            validate_path("../../../etc/passwd", base_dir=base)

    def test_path_traversal_absolute_escape(self, tmp_path):
        """Absolute paths that escape base_dir should be rejected."""
        base = str(tmp_path / "data")

        with pytest.raises(ValueError, match="escapes base directory"):
            validate_path("/etc/passwd", base_dir=base)


class TestValidateCompressionCodec:
    """Tests for validate_compression_codec function."""

    def test_valid_codecs(self):
        """All valid codecs should be accepted."""
        for codec in ["snappy", "gzip", "lz4", "zstd", "brotli", "uncompressed"]:
            assert validate_compression_codec(codec) == codec

    def test_case_insensitive(self):
        """Codec validation should be case-insensitive."""
        assert validate_compression_codec("SNAPPY") == "snappy"
        assert validate_compression_codec("Gzip") == "gzip"
        assert validate_compression_codec("ZSTD") == "zstd"

    def test_whitespace_stripped(self):
        """Whitespace should be stripped from codec names."""
        assert validate_compression_codec("  snappy  ") == "snappy"

    def test_invalid_codec_rejected(self):
        """Invalid codecs should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid compression codec"):
            validate_compression_codec("invalid")

        with pytest.raises(ValueError, match="Invalid compression codec"):
            validate_compression_codec("malicious; DROP TABLE")

    def test_empty_codec_rejected(self):
        """Empty codec should raise ValueError."""
        with pytest.raises(ValueError, match="must be a non-empty string"):
            validate_compression_codec("")

    def test_none_codec_rejected(self):
        """None codec should raise ValueError."""
        with pytest.raises(ValueError, match="must be a non-empty string"):
            validate_compression_codec(None)

    def test_valid_compression_codecs_constant(self):
        """VALID_COMPRESSION_CODECS should contain expected codecs."""
        assert "snappy" in VALID_COMPRESSION_CODECS
        assert "gzip" in VALID_COMPRESSION_CODECS
        assert "zstd" in VALID_COMPRESSION_CODECS


class TestScrubCredentials:
    """Tests for scrub_credentials function."""

    def test_aws_access_key_redacted(self):
        """AWS access key IDs should be redacted."""
        msg = "Error: access_key_id=AKIAIOSFODNN7EXAMPLE"
        result = scrub_credentials(msg)
        assert "AKIAIOSFODNN7EXAMPLE" not in result
        assert "[REDACTED]" in result

    def test_secret_key_redacted(self):
        """Secret keys in key=value format should be redacted."""
        msg = "Config: secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        result = scrub_credentials(msg)
        assert "wJalrXUtnFEMI" not in result
        assert "[REDACTED]" in result

    def test_bearer_token_redacted(self):
        """Bearer tokens should be redacted."""
        msg = "Auth header: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ"
        result = scrub_credentials(msg)
        assert "eyJhbGciOiJIUzI1NiIs" not in result
        assert "[REDACTED]" in result

    def test_azure_connection_string_redacted(self):
        """Azure connection string credentials should be redacted."""
        msg = "Connection: AccountKey=somebase64encodedkey12345678901234567890=="
        result = scrub_credentials(msg)
        assert "somebase64encoded" not in result
        assert "[REDACTED]" in result

    def test_password_redacted(self):
        """Passwords in key=value format should be redacted."""
        msg = "Database: password=mysupersecretpassword123"
        result = scrub_credentials(msg)
        assert "mysupersecretpassword" not in result
        assert "[REDACTED]" in result

    def test_non_credential_text_preserved(self):
        """Non-credential text should be preserved."""
        msg = "Error reading file at /data/customers/2024/01/data.parquet"
        result = scrub_credentials(msg)
        assert result == msg

    def test_empty_string(self):
        """Empty string should return empty string."""
        assert scrub_credentials("") == ""

    def test_none_returns_none(self):
        """None input should return None."""
        assert scrub_credentials(None) is None


class TestScrubException:
    """Tests for scrub_exception function."""

    def test_exception_with_credentials_scrubbed(self):
        """Exception messages with credentials should be scrubbed."""
        exc = ValueError("Failed to connect: access_key_id=AKIAIOSFODNN7EXAMPLE")
        result = scrub_exception(exc)
        assert "AKIAIOSFODNN7EXAMPLE" not in result
        assert "[REDACTED]" in result

    def test_exception_without_credentials(self):
        """Exception messages without credentials should be unchanged."""
        exc = FileNotFoundError("File not found: /data/file.parquet")
        result = scrub_exception(exc)
        assert result == "File not found: /data/file.parquet"


class TestSafeFormatError:
    """Tests for safe_format_error function."""

    def test_basic_error_message(self):
        """Basic error message formatting."""
        result = safe_format_error("read parquet")
        assert "Failed to read parquet" in result

    def test_error_with_path(self):
        """Error message with path."""
        result = safe_format_error("read parquet", path="/data/file.parquet")
        assert "Failed to read parquet" in result
        assert "/data/file.parquet" in result

    def test_error_with_exception(self):
        """Error message with exception."""
        exc = FileNotFoundError("File not found")
        result = safe_format_error("read parquet", error=exc)
        assert "Failed to read parquet" in result
        assert "File not found" in result

    def test_error_with_credentials_scrubbed(self):
        """Error message with credentials in exception should be scrubbed."""
        exc = ConnectionError("Auth failed: access_key_id=AKIAIOSFODNN7EXAMPLE")
        result = safe_format_error("connect to S3", error=exc)
        assert "AKIAIOSFODNN7EXAMPLE" not in result
        assert "[REDACTED]" in result

    def test_error_with_context(self):
        """Error message with additional context."""
        result = safe_format_error(
            "read parquet",
            path="/data/file.parquet",
            bucket="my-bucket",
            region="us-east-1"
        )
        assert "my-bucket" in result
        assert "us-east-1" in result


class TestValidateColumns:
    """Tests for validate_columns function."""

    def test_valid_columns(self):
        """Valid columns should pass validation."""
        result = validate_columns(["col1", "col2"], ["col1", "col2", "col3"])
        assert result == ["col1", "col2"]

    def test_none_columns(self):
        """None columns should return None."""
        result = validate_columns(None, ["col1", "col2"])
        assert result is None

    def test_invalid_column_rejected(self):
        """Invalid columns should raise ValueError."""
        with pytest.raises(ValueError, match="Invalid column"):
            validate_columns(["col1", "nonexistent"], ["col1", "col2"])

    def test_multiple_invalid_columns(self):
        """Multiple invalid columns should all be listed."""
        with pytest.raises(ValueError, match="col_a"):
            validate_columns(["col_a", "col_b"], ["col1", "col2"])
