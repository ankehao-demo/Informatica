from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest
from pcmigration.config import RunConfig
from pcmigration.schemas import PAY_PERIOD
from pyspark.sql import SparkSession


def _pay_period_row(pp_num: int, start: datetime, end: datetime, pay: datetime, flag: str) -> tuple:
    return (
        Decimal(pp_num),
        Decimal(2024),
        start,
        end,
        Decimal(pp_num),
        Decimal(2024),
        pay,
        flag,
        None,
        None,
    )


PAY_PERIOD_ROWS = [
    _pay_period_row(1, datetime(2024, 1, 1), datetime(2024, 1, 13), datetime(2024, 1, 19), "N"),
    _pay_period_row(2, datetime(2024, 1, 14), datetime(2024, 1, 27), datetime(2024, 2, 2), "Y"),
    _pay_period_row(3, datetime(2024, 2, 1), datetime(2024, 2, 10), datetime(2024, 2, 16), "N"),
]


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pcmigration-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture()
def pay_period(spark: SparkSession):
    """Three 2024 pay periods with a deliberate calendar gap between PP 2 (ends
    2024-01-27) and PP 3 (starts 2024-02-01), and PP 2 flagged current."""
    return spark.createDataFrame(PAY_PERIOD_ROWS, schema=PAY_PERIOD)


@pytest.fixture()
def pay_period_without_current(spark: SparkSession):
    """The calendar as it looks before wf_Pay_Calendar has run: nothing flagged."""
    rows = [row[:7] + ("N",) + row[8:] for row in PAY_PERIOD_ROWS]
    return spark.createDataFrame(rows, schema=PAY_PERIOD)


@pytest.fixture()
def pay_period_current_is_eleven(spark: SparkSession):
    """A two-digit current pay period, for the LPAD branch of the message."""
    rows = [
        _pay_period_row(11, datetime(2024, 5, 19), datetime(2024, 6, 1), datetime(2024, 6, 7), "Y"),
    ]
    return spark.createDataFrame(rows, schema=PAY_PERIOD)


def make_config(**overrides) -> RunConfig:
    defaults = {
        "catalog": "hive_metastore",
        "schema": "hhs_test",
        # SESSSTARTTIME is a naive local timestamp in PowerCenter, as is every
        # date in these mappings; the fixtures keep it that way deliberately.
        "session_start_time": datetime(2024, 1, 20, 6, 30, 15),
        "mapping_name": "m_test",
        "repository_service_name": "Prod_Repo",
    }
    defaults.update(overrides)
    return RunConfig(**defaults)
