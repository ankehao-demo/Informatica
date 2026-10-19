"""PySpark emulation of the Informatica PowerCenter expression functions used by
m_EHRP2BIIS_UPDATE.

Every helper documents the Informatica semantic it reproduces; see
docs/migration/ASSUMPTIONS.md for the cases where PowerCenter behaviour is ambiguous.
"""
from __future__ import annotations

from functools import reduce

from pyspark.sql import Column
from pyspark.sql import functions as F

# Informatica IS_SPACES treats blank, tab, newline, carriage return, form feed and
# vertical tab as "spaces".
_SPACES_ONLY = r"^[ \t\n\r\f\x0B]*$"


def lit_null() -> Column:
    return F.lit(None)


def iif(cond: Column, when_true, when_false=None) -> Column:
    """IIF(cond, a, b). A NULL condition is FALSE in PowerCenter -> returns b."""
    t = when_true if isinstance(when_true, Column) else F.lit(when_true)
    f = when_false if isinstance(when_false, Column) else F.lit(when_false)
    return F.when(cond, t).otherwise(f)


def decode_true(*pairs, default=None) -> Column:
    """DECODE(TRUE, c1, v1, c2, v2, ..., default).

    The first condition that evaluates to TRUE wins. A condition that evaluates to
    NULL (e.g. ``NOT IN(ERNCD, ...)`` with ERNCD NULL) does *not* match, exactly like
    Spark's CASE WHEN.
    """
    if len(pairs) % 2:
        raise ValueError("decode_true expects condition/value pairs")
    expr = None
    for cond, value in zip(pairs[0::2], pairs[1::2]):
        v = value if isinstance(value, Column) else F.lit(value)
        expr = F.when(cond, v) if expr is None else expr.when(cond, v)
    d = default if isinstance(default, Column) else F.lit(default)
    return expr.otherwise(d)


def decode_value(value: Column, *pairs, default=None) -> Column:
    """DECODE(value, s1, r1, s2, r2, ..., default) -- equality search; NULL never matches."""
    conds = []
    for search, result in zip(pairs[0::2], pairs[1::2]):
        conds += [value == F.lit(search), result]
    return decode_true(*conds, default=default)


def in_(value: Column, *items) -> Column:
    """IN(value, i1, i2, ...). Returns NULL when value is NULL (three-valued logic)."""
    return value.isin(*items)


def is_spaces(value: Column) -> Column:
    """IS_SPACES(value): TRUE if the string consists only of space characters.

    NULL input returns NULL. An empty string is treated as spaces (Oracle stores ''
    as NULL, so it never reaches PowerCenter as a non-NULL value anyway).
    """
    return F.when(value.isNull(), F.lit(None).cast("boolean")).otherwise(value.rlike(_SPACES_ONLY))


def isnull(value: Column) -> Column:
    return value.isNull()


def concat(*parts) -> Column:
    """The ``||`` operator. PowerCenter treats a NULL operand as an empty string and
    returns NULL only when *every* operand is NULL (unlike Spark ``concat``)."""
    cols = [p if isinstance(p, Column) else F.lit(p) for p in parts]
    as_str = [c.cast("string") for c in cols]
    all_null = reduce(lambda a, b: a & b, [c.isNull() for c in as_str])
    return F.when(all_null, F.lit(None).cast("string")).otherwise(
        F.concat(*[F.coalesce(c, F.lit("")) for c in as_str])
    )


def rtrim(value: Column) -> Column:
    """RTRIM(value) without a trim set removes trailing blanks only."""
    return F.rtrim(value)


def upper(value: Column) -> Column:
    return F.upper(value)


def zero_to_null(value: Column) -> Column:
    """IIF(x = 0, NULL, x)."""
    return iif(value == F.lit(0), lit_null(), value)


def get_date_part(value: Column, fmt: str) -> Column:
    fmt = fmt.upper()
    if fmt in ("YYYY", "YYY", "YY", "Y"):
        return F.year(value)
    if fmt in ("MM", "MON", "MONTH"):
        return F.month(value)
    if fmt in ("DD", "DDD", "DY", "DAY"):
        return F.dayofmonth(value)
    raise ValueError(f"unsupported GET_DATE_PART format {fmt}")


def add_to_date_months(value: Column, months: int) -> Column:
    """ADD_TO_DATE(date, 'MM', n). Keeps the time-of-day; if the target month is
    shorter, the day is clamped to the month's last day (Jan 31 -1 -> Dec 31,
    Feb 29 2024 -12 -> Feb 28 2023)."""
    return value + F.expr(f"make_interval(0, {int(months)})")


def to_char_int(value: Column) -> Column:
    """TO_CHAR(integer) -> decimal digits, no padding."""
    return value.cast("bigint").cast("string")


def string_to_number(value: Column, dtype) -> Column:
    """Implicit string -> numeric port conversion (same rules as TO_DECIMAL):
    leading blanks are ignored, the leading numeric part is converted, and a string
    that does not start with a number (e.g. PeopleSoft's blank ' ') becomes 0."""
    num = F.regexp_extract(F.ltrim(value), r"^([+-]?(\d+\.?\d*|\.\d+))", 1)
    return F.when(value.isNull(), F.lit(None)).when(num == "", F.lit(0)).otherwise(num).cast(dtype)
