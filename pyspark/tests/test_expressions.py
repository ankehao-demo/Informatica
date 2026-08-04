"""Tests for the PowerCenter expression semantics the ported mappings rely on.

Each test names the PowerCenter behaviour it pins down; if a future refactor
replaces one of these helpers with the "obvious" Spark equivalent, the test that
fails tells you which mapping breaks.
"""

from __future__ import annotations

from datetime import datetime

import pytest
from pcmigration import expressions as E
from pyspark.sql import functions as F


def evaluate(spark, column, rows=None, schema=None):
    frame = spark.createDataFrame(rows, schema) if rows is not None else spark.range(1)
    return [row[0] for row in frame.select(column.alias("value")).collect()]


class TestIsNumber:
    def test_null_input_returns_null_not_false(self, spark):
        # IS_NUMBER(NULL) is NULL. A DECODE/IIF downstream therefore takes its
        # default branch rather than its false branch.
        rows = [("123",), ("  42  ",), ("-1.5",), ("1e3",), ("12A",), ("",), ("   ",), (None,)]
        assert evaluate(spark, E.is_number(F.col("v")), rows, "v string") == [1, 1, 1, 1, 0, 0, 0, None]

    def test_parameter_values_use_the_same_rule(self):
        assert E.is_number_value("07") is True
        assert E.is_number_value("") is False
        assert E.is_number_value(None) is False
        assert E.is_number_value("PP7") is False


class TestIsDate:
    def test_length_and_calendar_validity(self, spark):
        rows = [("20240131",), ("20240230",), ("2024013",), ("abcdefgh",), (None,)]
        assert evaluate(spark, E.is_date(F.col("v"), "YYYYMMDD"), rows, "v string") == [1, 0, 0, 0, None]


class TestIif:
    def test_null_condition_takes_the_false_branch(self, spark):
        rows = [(1,), (0,), (None,)]
        column = E.iif(F.col("v") == 1, F.lit("yes"), F.lit("no"))
        assert evaluate(spark, column, rows, "v int") == ["yes", "no", "no"]

    def test_comparison_against_null_operand_is_not_true(self, spark):
        # `PP_NUM < 10` with a NULL PP_NUM is NULL, so IIF falls to the false
        # branch - this is what makes the message subject lose its pay period
        # rather than raising.
        rows = [(None,)]
        column = E.iif(F.col("v") < 10, F.lit("small"), F.lit("large"))
        assert evaluate(spark, column, rows, "v int") == ["large"]


class TestDecode:
    def test_first_match_wins_and_default_applies(self, spark):
        rows = [("Dev_",), ("Test",), ("Prod",), ("QA__",)]
        column = E.decode(
            F.col("v"), F.lit("Dev_"), F.lit("Dev: "), F.lit("Test"), F.lit("Test: "), F.lit("Prod"), F.lit("Prod: ")
        )
        assert evaluate(spark, column, rows, "v string") == ["Dev: ", "Test: ", "Prod: ", None]

    def test_null_matches_null_unlike_sql_case(self, spark):
        rows = [(None,)]
        column = E.decode(F.col("v"), F.lit(None).cast("string"), F.lit("matched"), default=F.lit("default"))
        assert evaluate(spark, column, rows, "v string") == ["matched"]


class TestConcatOperator:
    def test_null_operand_becomes_empty_string(self, spark):
        # Spark's concat() would return NULL for the whole expression here.
        rows = [(None,)]
        column = E.concat_ops(F.lit("Pay Period: "), F.col("v"), F.lit("-01"))
        assert evaluate(spark, column, rows, "v string") == ["Pay Period: -01"]
        assert evaluate(spark, F.concat(F.lit("Pay Period: "), F.col("v")), rows, "v string") == [None]


class TestToChar:
    def test_integral_decimal_has_no_decimal_point(self, spark):
        rows = [(7,), (None,)]
        assert evaluate(spark, E.to_char(F.col("v").cast("decimal(2,0)")), rows, "v int") == ["7", None]

    def test_scaled_decimal_keeps_significant_digits(self, spark):
        rows = [("12.50",)]
        assert evaluate(spark, E.to_char(F.col("v").cast("decimal(6,2)")), rows, "v string") == ["12.50"]

    def test_date_format(self, spark):
        rows = [(datetime(2024, 1, 31),)]
        assert evaluate(spark, E.to_char(F.col("v"), "MM/DD/YYYY"), rows, "v timestamp") == ["01/31/2024"]


class TestStringPortPrecision:
    def test_assignment_truncates_silently(self, spark):
        # v_SUBJECT is a string(100) port; PowerCenter cuts the value on
        # assignment without a warning.
        long_value = "x" * 250
        result = evaluate(spark, E.enforce_string_precision(F.lit(long_value), 100))
        assert result == ["x" * 100]


class TestTruncDate:
    def test_session_start_time_is_truncated_to_midnight(self, spark):
        rows = [(datetime(2024, 1, 27, 23, 59, 59),)]
        assert evaluate(spark, E.trunc_date(F.col("v")), rows, "v timestamp") == [datetime(2024, 1, 27)]


class TestUnsupportedFormats:
    def test_unknown_format_raises_rather_than_guessing(self):
        with pytest.raises(ValueError):
            E.is_date(F.lit("x"), "DD-MON-YY")
