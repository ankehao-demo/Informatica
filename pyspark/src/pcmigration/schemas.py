"""Schemas transcribed from the SOURCE / TARGET definitions in the PowerCenter exports.

Precision and scale are carried over verbatim (PAY_PERIOD.PP_NUM is number(2,0), not
an int) because the Oracle precision is part of the contract these mappings were
written against, and because a widened type silently changes rounding behaviour.
"""

from __future__ import annotations

from pyspark.sql.types import DecimalType, StringType, StructField, StructType, TimestampType

# XML/Pay_Calendar -> SOURCE / TARGET "PAY_PERIOD" (Oracle, HISTDBA).
# HOLIDAY_1 / HOLIDAY_2 exist on the source definition but are not used by the
# ported mapping; they are kept so the schema matches the table.
PAY_PERIOD = StructType(
    [
        StructField("PP_NUM", DecimalType(2, 0), nullable=False),
        StructField("PP_END_YEAR", DecimalType(4, 0), nullable=False),
        StructField("PP_START_DTE", TimestampType()),
        StructField("PP_END_DTE", TimestampType()),
        StructField("LV_NUM", DecimalType(2, 0)),
        StructField("LV_YEAR", DecimalType(4, 0)),
        StructField("PAY_DTE", TimestampType()),
        StructField("CURR_PP_FLAG", StringType()),
        StructField("HOLIDAY_1", TimestampType()),
        StructField("HOLIDAY_2", TimestampType()),
    ]
)

# XML/COMPTIME -> SOURCE "U0287D01": a comma-delimited, double-quoted flat file.
# Every field is read as its declared type; SSN is a string(9) and is deliberately
# *not* numeric, which is what the IS_NUMBER(SSN) record-type test relies on.
COMPTIME_FILE = StructType(
    [
        StructField("SSN", StringType()),
        StructField("NAME", StringType()),
        StructField("CURRENT_ACCT", StringType()),
        StructField("CURRENT_ORG", StringType()),
        StructField("FLSA_STATUS", StringType()),
        StructField("COMP_TIME_CUR_BAL", DecimalType(8, 2)),
        StructField("COMP_TIME_YEAR_EARNED", DecimalType(4, 0)),
        StructField("PP_END_DATE", StringType()),
        StructField("DAILY_DATE_EARNED", StringType()),
        StructField("COMP_TIME_RATE", DecimalType(6, 2)),
        StructField("COMP_TIME_HOURS", DecimalType(8, 2)),
        StructField("COMP_TIME_UNDEF", DecimalType(6, 0)),
    ]
)

# XML/COMPTIME -> TARGET "COUNTER_TBL" (Oracle).
COUNTER_TBL = StructType(
    [
        StructField("RUN_DATE", TimestampType()),
        StructField("PROCESS_NAME", StringType()),
        StructField("COUNTER_DESCRIPTION", StringType()),
        StructField("COUNTER_VALUE", DecimalType(15, 0)),
        StructField("PP_END_YEAR", DecimalType(4, 0)),
        StructField("PP_NUM", DecimalType(2, 0)),
        StructField("CYCLE_ID", DecimalType(1, 0)),
    ]
)

# XML/COMPTIME -> TARGET "COMPTIME_MESSAGE_FILE" (flat file consumed by the
# workflow's email task).
COMPTIME_MESSAGE_FILE = StructType(
    [
        StructField("SUBJECT", StringType()),
        StructField("MESSAGE", StringType()),
    ]
)

# Port precisions that the mappings depend on for truncation, from the
# TRANSFORMFIELD / TARGETFIELD definitions.
SUBJECT_PRECISION = 100
MESSAGE_PORT_PRECISION = 300
MESSAGE_TARGET_PRECISION = 300
COUNTER_DESCRIPTION_PRECISION = 200
