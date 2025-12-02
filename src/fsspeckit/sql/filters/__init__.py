from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.compute as pc
    from sqlglot import exp, parse_one
    from fsspeckit.common.polars import pl

from fsspeckit.common.datetime import timestamp_from_string


def sql2pyarrow_filter(string: str, schema: pa.Schema) -> pc.Expression:
    """
    Generates a filter expression for PyArrow based on a given string and schema.

    Parameters:
        string (str): The string containing the filter expression.
        schema (pa.Schema): The PyArrow schema used to validate the filter expression.

    Returns:
        pc.Expression: The generated filter expression.

    Raises:
        ValueError: If the input string is invalid or contains unsupported operations.
    """
    from fsspeckit.common.optional import _import_pyarrow, _import_sqlglot

    pa = _import_pyarrow()
    import pyarrow.compute as pc

    sqlglot = _import_sqlglot()
    from sqlglot import exp, parse_one

    def parse_value(val: str, type_: pa.DataType) -> Any:
        """Parse and convert value based on the field type."""
        if isinstance(val, (tuple, list)):
            return type(val)(parse_value(v, type_) for v in val)

        # Remove quotes from the value if present
        val = val.strip().strip("'\"")

        if pa.types.is_timestamp(type_):
            return timestamp_from_string(val, tz=type_.tz)
        elif pa.types.is_date(type_):
            parsed = timestamp_from_string(val)
            return parsed.date() if hasattr(parsed, "date") else parsed
        elif pa.types.is_time(type_):
            parsed = timestamp_from_string(val)
            return parsed.time() if hasattr(parsed, "time") else parsed

        elif pa.types.is_integer(type_):
            return int(float(val.replace(",", ".")))
        elif pa.types.is_floating(type_):
            return float(val.replace(",", "."))
        elif pa.types.is_boolean(type_):
            return val.lower() in ("true", "1", "yes")
        else:
            return val

    def _get_field_type_from_context(expr):
        """Try to determine the field type from a comparison expression."""
        if isinstance(expr.this, exp.Column):
            field_name = expr.this.name
            if field_name in schema.names:
                return schema.field(field_name).type
        elif isinstance(expr.expression, exp.Column):
            field_name = expr.expression.name
            if field_name in schema.names:
                return schema.field(field_name).type
        return None

    def _convert_expression(expr, field_type=None) -> pc.Expression:
        """Convert a sqlglot expression to a PyArrow compute expression."""
        if isinstance(expr, exp.Column):
            field_name = expr.name
            if field_name not in schema.names:
                raise ValueError(f"Unknown field: {field_name}")
            return pc.field(field_name)

        elif isinstance(expr, exp.Literal):
            # Convert literal value based on field type if available
            if field_type:
                val = str(expr.this)
                # Remove quotes if present
                val = val.strip().strip("'\"")
                return parse_value(val, field_type)
            return expr.this

        elif isinstance(expr, exp.Boolean):
            return bool(expr.this)

        elif isinstance(expr, exp.Null):
            return None

        elif isinstance(expr, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
            # Binary comparison operations
            # Try to determine field type from context
            context_type = _get_field_type_from_context(expr)

            left = _convert_expression(expr.this, context_type)
            right = _convert_expression(expr.expression, context_type)

            if isinstance(expr, exp.EQ):
                return left == right
            elif isinstance(expr, exp.NEQ):
                return left != right
            elif isinstance(expr, exp.GT):
                return left > right
            elif isinstance(expr, exp.GTE):
                return left >= right
            elif isinstance(expr, exp.LT):
                return left < right
            elif isinstance(expr, exp.LTE):
                return left <= right

        elif isinstance(expr, exp.In):
            # IN operation
            context_type = _get_field_type_from_context(expr)
            left = _convert_expression(expr.this, context_type)
            expressions = expr.args.get("expressions")
            if expressions is None and getattr(expr, "expression", None) is not None:
                expressions = getattr(expr.expression, "expressions", None)

            if expressions is None:
                right = _convert_expression(expr.expression, context_type)
            else:
                right = [
                    _convert_expression(e, context_type)
                    for e in expressions  # type: ignore[arg-type]
                ]
            return left.isin(right)

        elif isinstance(expr, exp.Not):
            # NOT operation - check if it's NOT IN or IS NOT NULL
            inner = expr.this
            if isinstance(inner, exp.In):
                # NOT IN case
                context_type = _get_field_type_from_context(inner)
                left = _convert_expression(inner.this, context_type)
                expressions = inner.args.get("expressions")
                if (
                    expressions is None
                    and getattr(inner, "expression", None) is not None
                ):
                    expressions = getattr(inner.expression, "expressions", None)

                if expressions is None:
                    right = _convert_expression(inner.expression, context_type)
                else:
                    right = [
                        _convert_expression(e, context_type)
                        for e in expressions  # type: ignore[arg-type]
                    ]
                return ~left.isin(right)
            elif isinstance(inner, exp.Is):
                # IS NOT NULL case
                left = _convert_expression(inner.this)
                return ~left.is_null(nan_is_null=True)
            else:
                # Generic NOT
                return ~_convert_expression(inner)

        elif isinstance(expr, exp.Is):
            left = _convert_expression(expr.this)
            return left.is_null(nan_is_null=True)

        elif isinstance(expr, exp.And):
            return _convert_expression(expr.this) & _convert_expression(expr.expression)

        elif isinstance(expr, exp.Or):
            return _convert_expression(expr.this) | _convert_expression(expr.expression)

        elif isinstance(expr, exp.Not):
            return ~_convert_expression(expr.this)

        elif isinstance(expr, exp.Paren):
            return _convert_expression(expr.this)

        else:
            raise ValueError(f"Unsupported expression type: {type(expr)}")

    try:
        # Parse the SQL expression using sqlglot
        parsed = parse_one(string)

        # Convert to PyArrow expression
        return _convert_expression(parsed)

    except Exception as e:
        raise ValueError(f"Failed to parse SQL expression: {e}")


def sql2polars_filter(string: str, schema: pl.Schema) -> pl.Expr:
    """
    Generates a filter expression for Polars based on a given string and schema.

    Parameters:
        string (str): The string containing the filter expression.
        schema (pl.Schema): The Polars schema used to validate the filter expression.

    Returns:
        pl.Expr: The generated filter expression.

    Raises:
        ValueError: If the input string is invalid or contains unsupported operations.
    """
    from fsspeckit.common.optional import _import_polars, _import_sqlglot

    pl = _import_polars()
    sqlglot = _import_sqlglot()
    from sqlglot import exp, parse_one

    def parse_value(val: str, dtype: pl.DataType) -> Any:
        """Parse and convert value based on the field type."""
        if isinstance(val, (tuple, list)):
            return type(val)(parse_value(v, dtype) for v in val)

        # Remove quotes from the value if present
        val = val.strip().strip("'\"")

        if dtype == pl.Datetime:
            return timestamp_from_string(val, tz=dtype.time_zone)
        elif dtype == pl.Date:
            return timestamp_from_string(val).date()
        elif dtype == pl.Time:
            return timestamp_from_string(val).time()
        elif dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64):
            return int(float(val.replace(",", ".")))
        elif dtype in (pl.Float32, pl.Float64):
            return float(val.replace(",", "."))
        elif dtype == pl.Boolean:
            return val.lower() in ("true", "1", "yes")
        else:
            return val

    def _get_field_type_from_context(expr):
        """Try to determine the field type from a comparison expression."""
        if isinstance(expr.this, exp.Column):
            field_name = expr.this.name
            if field_name in schema.names():
                return schema[field_name]
        elif isinstance(expr.expression, exp.Column):
            field_name = expr.expression.name
            if field_name in schema.names():
                return schema[field_name]
        return None

    def _convert_expression(expr, field_type=None) -> pl.Expr:
        """Convert a sqlglot expression to a Polars expression."""
        if isinstance(expr, exp.Column):
            field_name = expr.name
            if field_name not in schema.names():
                raise ValueError(f"Unknown field: {field_name}")
            return pl.col(field_name)

        elif isinstance(expr, exp.Literal):
            # Convert literal value based on field type if available
            if field_type:
                val = str(expr.this)
                # Remove quotes if present
                val = val.strip().strip("'\"")
                return parse_value(val, field_type)
            return expr.this

        elif isinstance(expr, exp.Null):
            return None

        elif isinstance(expr, (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)):
            # Binary comparison operations
            # Try to determine field type from context
            context_type = _get_field_type_from_context(expr)

            left = _convert_expression(expr.this, context_type)
            right = _convert_expression(expr.expression, context_type)

            if isinstance(expr, exp.EQ):
                return left == right
            elif isinstance(expr, exp.NEQ):
                return left != right
            elif isinstance(expr, exp.GT):
                return left > right
            elif isinstance(expr, exp.GTE):
                return left >= right
            elif isinstance(expr, exp.LT):
                return left < right
            elif isinstance(expr, exp.LTE):
                return left <= right

        elif isinstance(expr, exp.In):
            # IN operation
            context_type = _get_field_type_from_context(expr)
            left = _convert_expression(expr.this, context_type)
            # Convert the IN list
            if hasattr(expr, "expression") and hasattr(expr.expression, "expressions"):
                right = [
                    _convert_expression(e, context_type)
                    for e in expr.expression.expressions
                ]
            else:
                right = _convert_expression(expr.expression, context_type)
            return left.is_in(right)

        elif isinstance(expr, exp.Not):
            # NOT operation - check if it's NOT IN or IS NOT NULL
            inner = expr.this
            if isinstance(inner, exp.In):
                # NOT IN case
                context_type = _get_field_type_from_context(inner)
                left = _convert_expression(inner.this, context_type)
                if hasattr(inner, "expression") and hasattr(
                    inner.expression, "expressions"
                ):
                    right = [
                        _convert_expression(e, context_type)
                        for e in inner.expression.expressions
                    ]
                else:
                    right = _convert_expression(inner.expression, context_type)
                return ~left.is_in(right)
            elif isinstance(inner, exp.Is):
                # IS NOT NULL case
                left = _convert_expression(inner.this)
                return left.is_not_null()
            else:
                # Generic NOT
                return ~_convert_expression(inner)

        elif isinstance(expr, exp.Is):
            left = _convert_expression(expr.this)
            return left.is_null()

        elif isinstance(expr, exp.And):
            return _convert_expression(expr.this) & _convert_expression(expr.expression)

        elif isinstance(expr, exp.Or):
            return _convert_expression(expr.this) | _convert_expression(expr.expression)

        elif isinstance(expr, exp.Not):
            return ~_convert_expression(expr.this)

        elif isinstance(expr, exp.Paren):
            return _convert_expression(expr.this)

        else:
            raise ValueError(f"Unsupported expression type: {type(expr)}")

    try:
        # Parse the SQL expression using sqlglot
        parsed = parse_one(string)

        # Convert to Polars expression
        return _convert_expression(parsed)

    except Exception as e:
        raise ValueError(f"Failed to parse SQL expression: {e}")


def get_table_names(sql_query):
    from fsspeckit.common.optional import _import_sqlglot

    sqlglot = _import_sqlglot()
    from sqlglot import exp, parse_one

    return [table.name for table in parse_one(sql_query).find_all(exp.Table)]
