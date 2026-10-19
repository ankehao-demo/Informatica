import datetime as dt
from decimal import Decimal

import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipelines.ehrp2biis import infa as I


def _eval(spark, col, rows, schema):
    return [r[0] for r in spark.createDataFrame(rows, schema).select(col).collect()]


def test_is_spaces(spark):
    rows = [(" ",), ("   ",), ("\t",), ("",), (" A",), (None,)]
    got = _eval(spark, I.is_spaces(F.col("x")), rows, "x string")
    assert got == [True, True, True, True, False, None]


def test_concat_treats_null_as_empty_but_all_null_is_null(spark):
    rows = [("HE", "NA"), (None, "NA"), ("HE", None), (None, None)]
    got = _eval(spark, I.concat(F.col("a"), F.col("b")), rows, "a string, b string")
    assert got == ["HENA", "NA", "HE", None]
    got = _eval(spark, I.concat(F.col("a"), ", ", F.col("b")), rows, "a string, b string")
    assert got == ["HE, NA", ", NA", "HE, ", ", "]


def test_iif_null_condition_is_false(spark):
    got = _eval(spark, I.iif(F.col("x") < 40, 3, I.iif(F.col("x") >= 40, 1, 0)), [(10,), (40,), (None,)], "x int")
    assert got == [3, 1, 0]


def test_decode_true_null_condition_falls_through(spark):
    award = I.in_(F.col("noa"), "840") & ~I.in_(F.col("erncd"), "RCR", "RLC")
    col = I.decode_true(award, F.lit("A"), I.in_(F.col("noa"), "817"), F.lit("B"), default=None)
    rows = [("840", "SPA"), ("840", "RCR"), ("840", None), ("817", None), (None, None)]
    assert _eval(spark, col, rows, "noa string, erncd string") == ["A", None, None, "B", None]


def test_decode_value_and_in_with_null(spark):
    rows = [("805",), ("806",), ("999",), (None,)]
    assert _eval(spark, I.decode_value(F.col("x"), "805", "F", "806", "P"), rows, "x string") == ["F", "P", None, None]
    assert _eval(spark, I.in_(F.col("x"), "805"), rows, "x string") == [True, False, False, None]


def test_add_to_date_months_clamps_and_keeps_time(spark):
    rows = [(dt.datetime(2024, 2, 29),), (dt.datetime(2026, 3, 31, 14, 30),), (dt.datetime(2026, 1, 15),)]
    got = _eval(spark, I.add_to_date_months(F.col("d"), -12), rows, "d timestamp")
    assert got == [dt.datetime(2023, 2, 28), dt.datetime(2025, 3, 31, 14, 30), dt.datetime(2025, 1, 15)]


@pytest.mark.parametrize("raw,expected", [(" ", Decimal(0)), ("12", Decimal(12)), ("  7ab", Decimal(7)), ("x9", Decimal(0)), (None, None)])
def test_string_to_number(spark, raw, expected):
    got = _eval(spark, I.string_to_number(F.col("x"), T.DecimalType(5, 0)), [(raw,)], "x string")
    assert got == [expected]


def test_zero_to_null(spark):
    rows = [(Decimal("0.00"),), (Decimal("1.50"),), (None,)]
    assert _eval(spark, I.zero_to_null(F.col("x")), rows, "x decimal(5,2)") == [None, Decimal("1.50"), None]
