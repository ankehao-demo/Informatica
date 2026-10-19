"""The Spark SQL rendered into the mapping spec must compute the same value as the
PySpark implementation used by the job (keeps the documentation honest)."""
import datetime as dt
import json
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from pipelines.ehrp2biis.expressions import CATALOG
from pipelines.ehrp2biis.transform import SPEC_PATH, _spark_type

SKIP = {"v_EVENT_ID", "v_PREVIOUS_YEAR", "o_EVENT_ID", "o_CURRENT_YEAR"}  # window/stateful or alias-only

ROWS = [
    {"GVT_NOA_CODE": "840", "ERNCD": None, "GOAL_AMT": Decimal(5), "UNION_CD": " ", "GVT_XFER_FROM_AGCY": "\t", "STD_HOURS": None,
     "CITY": None, "STATE": "MD", "COMPANY": None, "GVT_SUB_AGENCY": None, "PROBATION_DT": dt.datetime(2024, 2, 29), "HE_AL_BALANCE": Decimal(0)},
    {"GVT_NOA_CODE": "817", "ERNCD": "RCR", "GOAL_AMT": Decimal(7), "UNION_CD": "8AA", "GVT_XFER_FROM_AGCY": "VA", "STD_HOURS": Decimal(40),
     "GVT_WORK_SCHED": "I", "CITY": " ", "STATE": " ", "COMPANY": "HE", "GVT_SUB_AGENCY": "NA", "GVT_PAR_AUTH_D1": "ab  ", "GVT_PAR_AUTH_D1_2": "cd  ",
     "REG_TEMP": "R", "GVT_RETIRE_PLAN": "K", "GVT_TYPE_OF_APPT": "34", "HE_NOA_EXT": "0", "CITIZENSHIP_STATUS_IN": "2"},
    {"GVT_NOA_CODE": "948", "HE_NOA_EXT": "1", "EARNINGS_END_DT": dt.datetime(2027, 1, 31), "STD_HOURS": Decimal("32.5"), "GVT_WORK_SCHED": "P",
     "REG_TEMP": "T", "ACTION": "BON", "GVT_DATE_WRK": None, "ACTION_DT": dt.datetime(2026, 1, 2), "EFFDT": dt.datetime(2026, 1, 5), "EFFSEQ": Decimal(3)},
    {"GVT_NOA_CODE": None},
]


@pytest.fixture(scope="module")
def frame(spark):
    spec = json.loads(SPEC_PATH.read_text())
    from pyspark.sql import types as T

    fields = {}
    for t in spec["transformations"]:
        if t["name"] in ("exp_MAIN2BIIS", "exp_GET_EFFDT_YEAR"):
            for f in t["fields"]:
                if f["porttype"] in ("INPUT", "INPUT/OUTPUT") and f["name"] not in fields:
                    fields[f["name"]] = T.StructField(f["name"], _spark_type(f["datatype"], f["precision"], f["scale"]))
    schema = T.StructType(list(fields.values()))
    data = [tuple(r.get(f.name) for f in schema.fields) for r in ROWS]
    return spark.createDataFrame(data, schema)


@pytest.mark.parametrize("name", sorted(set(CATALOG) - SKIP))
def test_sql_equals_pyspark(frame, name):
    port = CATALOG[name]
    ctx = {"run_ts": F.lit(dt.datetime(2026, 3, 12)).cast("timestamp")}
    sql = port.sql.replace(":run_date", "TIMESTAMP '2026-03-12 00:00:00'")
    if "v_curr_year" in sql:
        sql = sql.replace("v_curr_year", "year(EFFDT)")
    got = frame.select(port.pyspark(F.col, ctx).alias("p"), F.expr(sql).alias("s")).collect()
    for r in got:
        assert r["p"] == r["s"], (name, r)
