"""PowerCenter expression-language semantics expressed as Spark Columns.

Every function here exists because the naive Spark translation of the PowerCenter
function is *not* equivalent.  The docstrings record the PowerCenter behaviour that
the implementation is reproducing, so a reviewer can check the port against the
Informatica Transformation Language Reference rather than against intuition.

Where the PowerCenter behaviour is genuinely ambiguous the docstring says so and
states the interpretation that was chosen.
"""

from __future__ import annotations

import re

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

# PowerCenter TRUE/FALSE are the integers 1/0, and comparisons such as
# `PARAM_EXISTS_FLAG = TRUE` in a Router group are integer comparisons.
TRUE = 1
FALSE = 0

# A valid PowerCenter number: optional surrounding blanks, optional sign, digits
# with an optional decimal point, optional exponent.  IS_NUMBER ignores leading and
# trailing blanks but an all-blank or empty string is not a number.
_NUMBER_RE = re.compile(r"^[ \t]*[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?[ \t]*$")
_NUMBER_SQL_RE = r"^[ \t]*[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?[ \t]*$"


def is_number_value(value: str | None) -> bool:
    """Python-level IS_NUMBER, for mapping parameters resolved before the run.

    Mapping parameters ($$PP_NUM and friends) are scalars read from the parameter
    file at session start, not columns, so the parameter-file branch of a mapping
    has to be evaluated in Python to stay faithful.
    """
    if value is None:
        return False
    return bool(_NUMBER_RE.match(value))


def is_number(col: Column) -> Column:
    """IS_NUMBER: 1 / 0, and NULL when the input is NULL.

    The NULL passthrough is the trap: `IS_NUMBER(NULL)` is NULL, not FALSE, so a
    downstream DECODE/IIF falls through to its default rather than its false branch.
    """
    return F.when(col.isNull(), F.lit(None).cast("int")).otherwise(
        F.when(col.rlike(_NUMBER_SQL_RE), F.lit(TRUE)).otherwise(F.lit(FALSE))
    )


def is_date(col: Column, informatica_format: str) -> Column:
    """IS_DATE(value, format): 1 / 0, NULL when the input is NULL.

    Only the fixed-width numeric formats used by these mappings are supported;
    anything else raises rather than silently accepting a wrong format string.
    PowerCenter rejects a value whose length does not match the format, which
    Spark's parser would otherwise accept by truncating, so the length is checked
    explicitly.
    """
    # `try_to_timestamp` rather than `to_date`: under ANSI mode (the default on
    # Databricks and in Spark 4) `to_date` *raises* on an unparseable value, which
    # would abort the job on the very rows IS_DATE exists to screen out.
    parsed = F.try_to_timestamp(col, F.lit(_spark_date_format(informatica_format)))
    return F.when(col.isNull(), F.lit(None).cast("int")).otherwise(
        F.when((F.length(col) == len(informatica_format)) & parsed.isNotNull(), F.lit(TRUE)).otherwise(F.lit(FALSE))
    )


def to_date(col: Column, informatica_format: str) -> Column:
    """TO_DATE(value, format). Invalid input is an error row in PowerCenter; here it
    is NULL, because every call site in the ported mappings guards it with IS_DATE."""
    return F.try_to_timestamp(col, F.lit(_spark_date_format(informatica_format)))


def _spark_date_format(informatica_format: str) -> str:
    try:
        return {"YYYYMMDD": "yyyyMMdd", "MM/DD/YYYY": "MM/dd/yyyy"}[informatica_format]
    except KeyError:
        raise ValueError(f"unsupported PowerCenter date format {informatica_format!r}") from None


def iif(condition: Column, true_value: Column, false_value: Column | None = None) -> Column:
    """IIF(condition, value1 [, value2]).

    Two PowerCenter behaviours that Spark's `when` does not give you:

    * A NULL condition takes the false branch (Spark's `when` also skips to
      `otherwise` on NULL, so this part agrees, but it is asserted by tests).
    * When value2 is omitted the result is the *default value of value1's
      datatype* - 0 for numerics, '' for strings, NULL for date/time - not NULL.
      Callers must therefore pass the datatype-appropriate default explicitly;
      omitting it here means NULL, which is only correct for date/time ports.
    """
    if false_value is None:
        false_value = F.lit(None)
    return F.when(condition.isNotNull() & (condition.cast("boolean")), true_value).otherwise(false_value)


def decode(value: Column, *pairs: Column, default: Column | None = None) -> Column:
    """DECODE(value, search1, result1 [, search2, result2 ...] [, default]).

    Unlike a SQL CASE, PowerCenter's DECODE matches NULL against NULL, so the
    comparison is null-safe.  With no default the result is NULL.
    """
    if len(pairs) % 2:
        raise ValueError("decode() expects search/result pairs")
    result = F.lit(None) if default is None else default
    for search, outcome in reversed(list(zip(pairs[0::2], pairs[1::2]))):
        result = F.when(value.eqNullSafe(search), outcome).otherwise(result)
    return result


def concat_ops(*parts: Column) -> Column:
    """The `||` operator (and CONCAT), which treats NULL as an empty string.

    Spark's `concat` returns NULL if *any* argument is NULL, so a single missing
    lookup value would blank out an entire subject line instead of leaving a gap.
    This is the highest-frequency semantic trap in the estate: `||` appears in
    almost every message-building expression.
    """
    return F.concat(*[F.coalesce(part.cast("string"), F.lit("")) for part in parts])


def to_char(col: Column, informatica_format: str | None = None) -> Column:
    """TO_CHAR for numbers and dates.

    For an integral decimal port PowerCenter emits no decimal point ('12', not
    '12.00'), which a plain `cast('string')` on a DecimalType(p, s>0) would not do.
    NULL in, NULL out - which is what makes `TO_CHAR(x) || 'text'` produce 'text'
    rather than NULL once `||` coalesces it away.
    """
    if informatica_format is not None:
        return F.date_format(col, _spark_datetime_format(informatica_format))
    return F.when(col.isNull(), F.lit(None).cast("string")).otherwise(
        F.regexp_replace(col.cast("string"), r"\.0+$", "")
    )


def _spark_datetime_format(informatica_format: str) -> str:
    try:
        return {
            "YYYYMMDD": "yyyyMMdd",
            "MM/DD/YYYY": "MM/dd/yyyy",
            "MM/DD/YYYY HH24:MI:SS": "MM/dd/yyyy HH:mm:ss",
        }[informatica_format]
    except KeyError:
        raise ValueError(f"unsupported PowerCenter datetime format {informatica_format!r}") from None


def to_decimal(col: Column, precision: int, scale: int = 0) -> Column:
    """TO_DECIMAL(value). Non-numeric input yields NULL here; every call site in the
    ported mappings guards the conversion with IS_NUMBER first."""
    return col.cast(DecimalType(precision, scale))


def lpad(col: Column, length: int, pad: str) -> Column:
    """LPAD. A source string longer than `length` is truncated from the right, and
    NULL input gives NULL - both match Spark's `lpad`, so this is a thin alias kept
    for symmetry with the mapping expressions."""
    return F.lpad(col, length, pad)


def substr(col: Column, start: int, length: int) -> Column:
    """SUBSTR with PowerCenter's 1-based start position."""
    return F.substring(col, start, length)


def trunc_date(col: Column) -> Column:
    """TRUNC(date) with no format argument: truncate to midnight of the same day."""
    return F.date_trunc("day", col)


def add_to_date(col: Column, unit: str, amount: int) -> Column:
    """ADD_TO_DATE(date, unit, amount). Only the units used by these mappings."""
    if unit != "D":
        raise ValueError(f"unsupported ADD_TO_DATE unit {unit!r}")
    return col + F.expr(f"INTERVAL {amount} DAYS")


def enforce_string_precision(col: Column, precision: int) -> Column:
    """Assigning to a string port truncates to the port's precision - silently.

    PowerCenter does not warn when a 300-character expression lands in a
    string(100) port; the value is simply cut.  Spark would keep the whole string
    and the difference only shows up when someone diffs the output files.
    """
    return F.substring(col, 1, precision)
