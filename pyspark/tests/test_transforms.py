"""Tests for the transformation-shape helpers (Lookup, Router, Update Strategy)."""

from __future__ import annotations

from pcmigration import transforms as T
from pyspark.sql import functions as F


class TestConnectedLookup:
    def test_multiple_matches_do_not_multiply_rows(self, spark):
        # The trap: a plain left join returns two rows here and every downstream
        # count doubles. A PowerCenter Lookup returns exactly one row per input.
        rows = spark.createDataFrame([("A",), ("B",)], "key string")
        lookup = spark.createDataFrame([("A", 1), ("A", 2), ("B", 9)], "k string, v int")

        result = T.connected_lookup(rows, lookup, lambda left, lkp: left["key"] == lkp["k"], {"lkp_v": "v"})

        assert rows.join(lookup, rows["key"] == lookup["k"], "left").count() == 3
        assert sorted(result.collect(), key=lambda r: r["key"]) == [("A", 1), ("B", 9)]

    def test_miss_returns_nulls_and_keeps_the_row(self, spark):
        rows = spark.createDataFrame([("A",), ("Z",)], "key string")
        lookup = spark.createDataFrame([("A", 1)], "k string, v int")

        result = T.connected_lookup(rows, lookup, lambda left, lkp: left["key"] == lkp["k"], {"lkp_v": "v"})

        assert {row["key"]: row["lkp_v"] for row in result.collect()} == {"A": 1, "Z": None}

    def test_lookup_on_the_same_table_the_pipeline_reads(self, spark):
        # Self-lookups are common in this estate (PAY_PERIOD reads PAY_PERIOD);
        # the helper must not raise an ambiguous-column error.
        table = spark.createDataFrame([("A", 1), ("B", 2)], "key string, v int")
        result = T.connected_lookup(table, table, lambda left, lkp: left["key"] == lkp["key"], {"lkp_v": "v"})
        assert result.count() == 2


class TestRouter:
    def test_groups_are_independent_and_default_catches_the_rest(self, spark):
        rows = spark.createDataFrame([(1,), (5,), (20,), (None,)], "v int")

        groups = T.router(
            rows,
            {"SMALL": F.col("v") < 10, "ODD": F.col("v") % 2 == 1},
        )

        assert [r["v"] for r in groups["SMALL"].collect()] == [1, 5]
        assert [r["v"] for r in groups["ODD"].collect()] == [1, 5]
        # 20 matches no group; the NULL row matches nothing either, because a
        # NULL group condition is not a match.
        assert sorted(r["v"] for r in groups["DEFAULT"].collect() if r["v"] is not None) == [20]
        assert groups["DEFAULT"].count() == 2


class TestAggregateAllInput:
    def test_empty_input_still_emits_one_row_with_zero(self, spark):
        rows = spark.createDataFrame([], "v string")
        result = T.aggregate_all_input(rows, [F.count(F.col("v")).alias("n")]).collect()
        assert [row["n"] for row in result] == [0]

    def test_count_of_a_port_ignores_nulls(self, spark):
        rows = spark.createDataFrame([("a",), (None,), ("c",)], "v string")
        result = T.aggregate_all_input(rows, [F.count(F.col("v")).alias("n")]).collect()
        assert [row["n"] for row in result] == [2]


class TestUpdateStrategy:
    def test_rows_without_a_matching_key_are_rejected_not_inserted(self, spark):
        target = spark.createDataFrame([(1, 2024), (2, 2024)], "PP_NUM int, PP_END_YEAR int")
        rows = T.flag_rows(
            spark.createDataFrame([(1, 2024), (9, 2024), (None, 2024)], "PP_NUM int, PP_END_YEAR int"),
            T.DD_UPDATE,
        )

        applied, rejected = T.split_update_strategy(rows, target, ["PP_NUM", "PP_END_YEAR"])

        assert [(r["PP_NUM"], r["PP_END_YEAR"]) for r in applied.collect()] == [(1, 2024)]
        rejected_keys = {(r["PP_NUM"], r["PP_END_YEAR"]) for r in rejected.collect()}
        assert rejected_keys == {(9, 2024), (None, 2024)}
