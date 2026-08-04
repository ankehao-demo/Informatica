"""Tests for the port of m_Pay_Calendar_Set_Pay_Calendar.

The fixtures are three 2024 pay periods with a deliberate calendar gap between
PP 2 (ends 2024-01-27) and PP 3 (starts 2024-02-01), which is what lets the
date-range lookup be tested on both a hit and a miss.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from conftest import make_config
from pcmigration import transforms as T
from pcmigration.jobs import pay_calendar_set_pay_calendar as job

DRIVER_SCHEMA = "PP_NUM decimal(2,0), PP_END_YEAR decimal(4,0)"


def driver_row(spark):
    """What SQ_PAY_PERIOD's `SELECT MAX(...), MAX(...)` yields: one row."""
    return spark.createDataFrame([(Decimal(3), Decimal(2024))], DRIVER_SCHEMA)


def collect_updates(spark, pay_period, **config_overrides):
    config = make_config(mapping_name=job.MAPPING_NAME, **config_overrides)
    updates = job.transform(spark, config, driver_row(spark), pay_period)
    return [row.asDict() for row in updates.collect()]


class TestParameterBranch:
    def test_parameters_matching_an_existing_pay_period_win(self, spark, pay_period):
        rows = collect_updates(spark, pay_period, parameters={"$$PP_NUM": "1", "$$PP_END_YEAR": "2024"})

        assert rows == [
            {
                "PP_NUM": Decimal(1),
                "PP_END_YEAR": Decimal(2024),
                "CURR_PP_FLAG": "Y",
                "PC_UPDATE_STRATEGY": T.DD_UPDATE,
            }
        ]

    def test_parameters_are_ignored_when_they_match_no_row(self, spark, pay_period):
        # $$PP_NUM = 9 does not exist, so lkp_Existing_Pay_Period misses, the
        # flag is FALSE and the Router sends the row down the date branch. The
        # session date (2024-01-20) falls in PP 2.
        rows = collect_updates(spark, pay_period, parameters={"$$PP_NUM": "9", "$$PP_END_YEAR": "2024"})

        assert [(r["PP_NUM"], r["CURR_PP_FLAG"]) for r in rows] == [(Decimal(2), "Y")]

    def test_non_numeric_parameter_becomes_zero_and_falls_through(self, spark, pay_period):
        # IIF(NOT IS_NUMBER($$PP_NUM), 0, ...) - a typo in the parameter file
        # does not fail the session, it silently switches branch.
        rows = collect_updates(spark, pay_period, parameters={"$$PP_NUM": "PP7", "$$PP_END_YEAR": "2024"})

        assert [(r["PP_NUM"], r["CURR_PP_FLAG"]) for r in rows] == [(Decimal(2), "Y")]

    def test_absent_parameter_file_falls_through(self, spark, pay_period):
        rows = collect_updates(spark, pay_period, parameters={})

        assert [(r["PP_NUM"], r["CURR_PP_FLAG"]) for r in rows] == [(Decimal(2), "Y")]


class TestDateBranch:
    def test_boundary_dates_are_inclusive(self, spark, pay_period):
        # The lookup condition is PP_START_DTE <= date AND PP_END_DTE >= date,
        # so the last day of a pay period still resolves to that pay period -
        # but only because TRUNC(SESSSTARTTIME) removed the time of day.
        rows = collect_updates(spark, pay_period, session_start_time=datetime(2024, 1, 27, 22, 15, 0), parameters={})

        assert [r["PP_NUM"] for r in rows] == [Decimal(2)]

    def test_calendar_gap_produces_a_null_key_update(self, spark, pay_period):
        # 2024-01-30 is in the gap. PowerCenter does not fail: the lookup returns
        # NULLs, the Update Strategy still flags DD_UPDATE, and the writer
        # rejects the row to the .bad file. The pay calendar is silently left
        # with no current pay period.
        rows = collect_updates(spark, pay_period, session_start_time=datetime(2024, 1, 30, 6, 0, 0), parameters={})

        assert rows == [
            {
                "PP_NUM": None,
                "PP_END_YEAR": None,
                "CURR_PP_FLAG": "Y",
                "PC_UPDATE_STRATEGY": T.DD_UPDATE,
            }
        ]

    def test_the_null_key_row_is_rejected_rather_than_inserted(self, spark, pay_period):
        config = make_config(
            mapping_name=job.MAPPING_NAME, session_start_time=datetime(2024, 1, 30, 6, 0, 0), parameters={}
        )
        updates = job.transform(spark, config, driver_row(spark), pay_period)

        applied, rejected = T.split_update_strategy(updates, pay_period, list(job.TARGET_KEYS))

        assert applied.count() == 0
        assert rejected.count() == 1


class TestRowCount:
    def test_the_mapping_updates_exactly_one_row(self, spark, pay_period):
        # PAY_PERIOD has three rows and the mapping reads it twice (source and
        # lookup); neither may fan the single driver row out.
        assert len(collect_updates(spark, pay_period, parameters={})) == 1

    def test_duplicate_calendar_rows_still_yield_one_update(self, spark, pay_period):
        overlapping = pay_period.unionByName(pay_period)

        assert len(collect_updates(spark, overlapping, parameters={})) == 1


class TestRouter:
    def test_the_default_group_is_unreachable(self, spark, pay_period):
        config = make_config(mapping_name=job.MAPPING_NAME, parameters={"$$PP_NUM": "1", "$$PP_END_YEAR": "2024"})
        initial = job.exp_initial(driver_row(spark), config)
        flagged = job.exp_determine_parameters_exist(job.lkp_existing_pay_period(initial, pay_period))

        groups = job.rtr_parameter_non_parameter(flagged)

        assert groups["PARAMETERS_EXIST"].count() == 1
        assert groups["PARAMETERS_NOT_EXIST"].count() == 0
        assert groups["DEFAULT"].count() == 0
