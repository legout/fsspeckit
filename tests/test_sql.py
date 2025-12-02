"""Tests for SQL filter functions."""

import pytest
import pyarrow as pa
import pyarrow.compute as pc
from fsspeckit.sql.filters import sql2pyarrow_filter, sql2polars_filter


@pytest.fixture
def sample_schema():
    """Create a sample PyArrow schema for testing."""
    return pa.schema(
        [
            ("afko_aufnr", pa.string()),
            ("vbap_yybcvfgrp", pa.string()),
            ("afko_getri_timestamp", pa.timestamp("ns", "UTC")),
            ("resb_matnr", pa.string()),
            ("afko_system", pa.string()),
            ("afpo_dwerk", pa.string()),
            ("makt_maktx", pa.string()),
            ("resb_posnr", pa.string()),
            ("afko_fevor", pa.string()),
            ("afko_gltrs", pa.string()),
            ("afko_zzfabnr", pa.string()),
            ("afko_gltri", pa.string()),
            ("afko_getri", pa.string()),
            ("afko_gamng", pa.float64()),
            ("afpo_kdpos", pa.string()),
            ("resb_potx1", pa.string()),
            ("resb_bdmng", pa.float64()),
            ("vbap_yybcezndr", pa.string()),
            ("vbap_yykurzanga", pa.string()),
            ("afpo_kdauf", pa.string()),
            ("resb_meins", pa.string()),
            ("afko_lead_aufnr", pa.string()),
            ("afko_plnbez", pa.string()),
            ("mara_zzkenn", pa.string()),
            ("age", pa.int32()),
            ("score", pa.int32()),
            ("category", pa.string()),
            ("timestamp_field", pa.timestamp("ns", "UTC")),
        ]
    )


def test_sql2pyarrow_filter_simple_conditions(sample_schema):
    """Test simple conditions without parentheses."""
    # Test single condition
    expr = sql2pyarrow_filter("age > 30", sample_schema)
    assert isinstance(expr, pc.Expression)

    # Test AND condition
    expr = sql2pyarrow_filter("age > 30 AND score > 85", sample_schema)
    assert isinstance(expr, pc.Expression)

    # Test OR condition
    expr = sql2pyarrow_filter("age > 30 OR category = 'A'", sample_schema)
    assert isinstance(expr, pc.Expression)


def test_sql2pyarrow_filter_with_parentheses(sample_schema):
    """Test conditions with parentheses - this was the original bug."""
    # Test single level parentheses
    expr = sql2pyarrow_filter("(age > 30 AND score > 85)", sample_schema)
    assert isinstance(expr, pc.Expression)

    # Test nested parentheses - the original failing case
    sql_expr = (
        "((afko_aufnr<='53702645' AND afko_aufnr>='7200629') OR afko_aufnr IS NULL)"
    )
    expr = sql2pyarrow_filter(sql_expr, sample_schema)
    assert isinstance(expr, pc.Expression)

    # Test complex nested expression from the original bug report
    complex_expr = (
        "((afko_aufnr<='53702645' AND afko_aufnr>='7200629') OR afko_aufnr IS NULL) AND "
        "((vbap_yybcvfgrp<='X0TZ' AND vbap_yybcvfgrp>='4R33') OR vbap_yybcvfgrp IS NULL) AND "
        "((afko_getri_timestamp<='2025-09-24 00:00:00+00:00' AND "
        "afko_getri_timestamp>='2025-09-22 00:00:00+00:00') OR afko_getri_timestamp IS NULL)"
    )
    expr = sql2pyarrow_filter(complex_expr, sample_schema)
    assert isinstance(expr, pc.Expression)


def test_sql2polars_filter_simple_conditions(sample_schema):
    """Test simple conditions without parentheses for Polars."""
    import polars as pl

    # Create proper Polars schema
    polars_schema = pl.Schema(
        {
            "afko_aufnr": pl.String,
            "vbap_yybcvfgrp": pl.String,
            "afko_getri_timestamp": pl.Datetime(time_zone="UTC"),
            "resb_matnr": pl.String,
            "afko_system": pl.String,
            "afpo_dwerk": pl.String,
            "makt_maktx": pl.String,
            "resb_posnr": pl.String,
            "afko_fevor": pl.String,
            "afko_gltrs": pl.String,
            "afko_zzfabnr": pl.String,
            "afko_gltri": pl.String,
            "afko_getri": pl.String,
            "afko_gamng": pl.Float64,
            "afpo_kdpos": pl.String,
            "resb_potx1": pl.String,
            "resb_bdmng": pl.Float64,
            "vbap_yybcezndr": pl.String,
            "vbap_yykurzanga": pl.String,
            "afpo_kdauf": pl.String,
            "resb_meins": pl.String,
            "afko_lead_aufnr": pl.String,
            "afko_plnbez": pl.String,
            "mara_zzkenn": pl.String,
            "age": pl.Int32,
            "score": pl.Int32,
            "category": pl.String,
            "timestamp_field": pl.Datetime(time_zone="UTC"),
        }
    )

    # Test single condition
    expr = sql2polars_filter("age > 30", polars_schema)
    assert isinstance(expr, pl.Expr)

    # Test AND condition
    expr = sql2polars_filter("age > 30 AND score > 85", polars_schema)
    assert isinstance(expr, pl.Expr)

    # Test OR condition
    expr = sql2polars_filter("age > 30 OR category = 'A'", polars_schema)
    assert isinstance(expr, pl.Expr)


def test_sql2polars_filter_with_parentheses(sample_schema):
    """Test conditions with parentheses for Polars."""
    import polars as pl

    # Create proper Polars schema
    polars_schema = pl.Schema(
        {
            "afko_aufnr": pl.String,
            "vbap_yybcvfgrp": pl.String,
            "afko_getri_timestamp": pl.Datetime(time_zone="UTC"),
            "age": pl.Int32,
            "score": pl.Int32,
            "category": pl.String,
        }
    )

    # Test single level parentheses
    expr = sql2polars_filter("(age > 30 AND score > 85)", polars_schema)
    assert isinstance(expr, pl.Expr)

    # Test nested parentheses - the original failing case
    sql_expr = (
        "((afko_aufnr<='53702645' AND afko_aufnr>='7200629') OR afko_aufnr IS NULL)"
    )
    expr = sql2polars_filter(sql_expr, polars_schema)
    assert isinstance(expr, pl.Expr)

    # Test complex nested expression from the original bug report
    complex_expr = (
        "((afko_aufnr<='53702645' AND afko_aufnr>='7200629') OR afko_aufnr IS NULL) AND "
        "((vbap_yybcvfgrp<='X0TZ' AND vbap_yybcvfgrp>='4R33') OR vbap_yybcvfgrp IS NULL) AND "
        "((afko_getri_timestamp<='2025-09-24 00:00:00+00:00' AND "
        "afko_getri_timestamp>='2025-09-22 00:00:00+00:00') OR afko_getri_timestamp IS NULL)"
    )
    expr = sql2polars_filter(complex_expr, polars_schema)
    assert isinstance(expr, pl.Expr)


def test_parentheses_edge_cases():
    """Test SQL expressions with various parentheses combinations."""
    schema = pa.schema([("test_field", pa.int32())])

    # Test various parentheses combinations
    test_cases = [
        "((test_field > 10))",  # Multiple nested parentheses
        "(test_field > 10)",  # Single parentheses
        "test_field > 10",  # No parentheses
        "  (  test_field  >  10  )  ",  # With whitespace
    ]

    for sql_expr in test_cases:
        expr = sql2pyarrow_filter(sql_expr, schema)
        assert isinstance(expr, pc.Expression)


def test_error_cases():
    """Test error handling."""
    schema = pa.schema([("age", pa.int32())])

    # Test invalid field name
    with pytest.raises(ValueError, match="Unknown field"):
        sql2pyarrow_filter("invalid_field > 10", schema)

    # Test invalid SQL syntax
    with pytest.raises(ValueError, match="Failed to parse SQL expression"):
        sql2pyarrow_filter("age > ", schema)
