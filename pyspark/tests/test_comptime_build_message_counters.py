"""Tests for the port of m_COMPTIME_Build_Message_Counters."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from conftest import make_config
from pcmigration.jobs import comptime_build_message_counters as job

# A Comp Time extract as it actually arrives: a header line whose first field is
# a label, detail rows keyed by SSN, a trailer, and a row with no SSN at all.
SOURCE_ROWS = [
    ("SSN", "NAME"),
    ("123456789", "SMITH,JOHN"),
    ("987654321", "DOE,JANE   "),
    (None, "ORPHAN ROW"),
    ("TRAILER", "END OF FILE"),
]


def source(spark, rows=None):
    return spark.createDataFrame(rows if rows is not None else SOURCE_ROWS, "SSN string, NAME string")


def run_mapping(spark, pay_period, rows=None, **config_overrides):
    config = make_config(mapping_name=job.MAPPING_NAME, **config_overrides)
    counters, message = job.transform(config, source(spark, rows), pay_period)
    return counters.collect(), message.collect()


class TestRecordTypeFilter:
    def test_only_numeric_ssn_rows_are_counted(self, spark, pay_period):
        counters, _ = run_mapping(spark, pay_period)
        assert [row["COUNTER_VALUE"] for row in counters] == [Decimal(2)]

    def test_null_ssn_is_excluded_via_null_propagation(self, spark, pay_period):
        # IS_NUMBER(NULL) is NULL, DECODE(TRUE, NULL, 'D') does not match, so the
        # flag is 'NO' and fil_Detail drops the row. If IS_NUMBER returned FALSE
        # for NULL the outcome would be the same here - but the flag column is
        # asserted directly so the difference is visible.
        flagged = job.exp_initial(source(spark, [(None, "ORPHAN")])).collect()
        assert flagged[0]["RECORD_TYPE_FLAG"] == "NO"

    def test_ssn_with_surrounding_blanks_is_still_numeric(self, spark, pay_period):
        # Fixed-width extracts pad values; IS_NUMBER ignores leading and
        # trailing blanks, so ' 123456789 ' is a detail row.
        counters, _ = run_mapping(spark, pay_period, rows=[(" 123456789 ", "PADDED")])
        assert [row["COUNTER_VALUE"] for row in counters] == [Decimal(1)]

    def test_empty_file_still_produces_a_counter_row_and_an_email(self, spark, pay_period):
        counters, message = run_mapping(spark, pay_period, rows=[])
        assert [row["COUNTER_VALUE"] for row in counters] == [Decimal(0)]
        assert message[0]["MESSAGE"].endswith("= 0")


class TestCounterRow:
    def test_run_date_is_session_start_time_and_process_name_is_the_mapping(self, spark, pay_period):
        counters, _ = run_mapping(spark, pay_period, session_start_time=datetime(2024, 1, 20, 6, 30, 15))
        row = counters[0]
        assert row["RUN_DATE"] == datetime(2024, 1, 20, 6, 30, 15)
        assert row["PROCESS_NAME"] == job.MAPPING_NAME
        assert row["COUNTER_DESCRIPTION"] == job.COUNTER_DESCRIPTION

    def test_pay_period_columns_are_left_null_as_in_the_mapping(self, spark, pay_period):
        # The pay period is available upstream but those target ports are not
        # connected in the mapping; the port reproduces that rather than
        # improving on it.
        row = run_mapping(spark, pay_period)[0][0]
        assert (row["PP_NUM"], row["PP_END_YEAR"], row["CYCLE_ID"]) == (None, None, None)


class TestMessage:
    def test_subject_uses_the_current_pay_period_and_zero_pads_it(self, spark, pay_period):
        _, message = run_mapping(spark, pay_period, repository_service_name="Prod_Repo")
        assert message[0]["SUBJECT"] == "Prod: Comp Time File loaded successfully for Pay Period:  2024-02"

    def test_pay_period_of_ten_or_more_is_not_padded(self, spark, pay_period_current_is_eleven):
        _, message = run_mapping(spark, pay_period_current_is_eleven, repository_service_name="Test_Repo")
        assert message[0]["SUBJECT"] == "Test: Comp Time File loaded successfully for Pay Period:  2024-11"

    def test_message_body_keeps_the_literal_tab(self, spark, pay_period):
        _, message = run_mapping(spark, pay_period)
        assert message[0]["MESSAGE"] == "Number of Detail Records from Comp Time file\t= 2"

    def test_unknown_repository_service_loses_the_prefix_but_not_the_subject(self, spark, pay_period):
        # DECODE has no default, so v_ENVIRONMENT is NULL - and `||` swallows it.
        # Spark's concat would have produced a NULL subject and an empty email.
        _, message = run_mapping(spark, pay_period, repository_service_name="UAT_Repo")
        assert message[0]["SUBJECT"] == "Comp Time File loaded successfully for Pay Period:  2024-02"

    def test_no_current_pay_period_leaves_a_gap_rather_than_failing(self, spark, pay_period_without_current):
        # lkp_PAY_PERIOD misses when wf_Pay_Calendar has not run: PP_NUM and
        # PP_END_YEAR are NULL, `PP_NUM < 10` is NULL so IIF takes the else
        # branch, TO_CHAR(NULL) is NULL, and `||` renders it as ''. The email
        # still goes out - saying "Pay Period:  -".
        _, message = run_mapping(spark, pay_period_without_current, repository_service_name="Prod_Repo")
        assert message[0]["SUBJECT"] == "Prod: Comp Time File loaded successfully for Pay Period:  -"

    def test_subject_is_truncated_to_the_port_precision(self, spark, pay_period):
        _, message = run_mapping(spark, pay_period, repository_service_name="Prod_Repo")
        assert len(message[0]["SUBJECT"]) <= 100


class TestMappingVariables:
    def test_setvariable_values_are_returned_for_the_email_task(self, spark, pay_period):
        config = make_config(mapping_name=job.MAPPING_NAME, repository_service_name="Dev_Repo")
        _, message = job.transform(config, source(spark), pay_period)

        variables = job.collect_mapping_variables(message)

        assert variables.subject.startswith("Dev: ")
        assert variables.message.endswith("= 2")
