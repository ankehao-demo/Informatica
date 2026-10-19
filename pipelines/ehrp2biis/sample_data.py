"""Deterministic synthetic PeopleSoft batch used by the tests and the local demo run.

Each case targets one Informatica semantic that is easy to get wrong in a port
(DECODE truthiness, IS_SPACES, NULL-safe ||, lookup misses, Use Any Value, the
stateful EVENT_ID sequence, ADD_TO_DATE month-end clamping, ...). Columns not set
explicitly get PeopleSoft defaults: ' ' for character fields, 0 for numbers, NULL
for dates.
"""
from __future__ import annotations

import datetime as dt
import decimal
import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from .transform import _spark_type, load_spec

D = decimal.Decimal
DT = dt.datetime

LOOKUP_TABLE_ALIASES = {
    "lkp_PS_GVT_EMPLOYMENT": "PS_GVT_EMPLOYMENT",
    "lkp_PS_GVT_PERS_NID": "PS_GVT_PERS_NID",
    "lkp_PS_GVT_AWD_DATA": "PS_GVT_AWD_DATA",
    "lkp_PS_GVT_EE_DATA_TRK": "PS_GVT_EE_DATA_TRK",
    "lkp_PS_HE_FILL_POS": "PS_HE_FILL_POS",
    "lkp_PS_GVT_CITIZENSHIP": "PS_GVT_CITIZENSHIP",
    "lkp_PS_GVT_PERS_DATA": "PS_GVT_PERS_DATA",
    "lkp_PS_JPM_JP_ITEMS": "PS_JPM_JP_ITEMS",
    "lkp_OLD_SEQUENCE_NUMBER": "SEQUENCE_NUM_TBL",
}
KEYS = ("EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ")


def table_schemas(spec: dict | None = None) -> dict[str, T.StructType]:
    spec = spec or load_spec()
    out: dict[str, T.StructType] = {}
    for s in spec["sources"]:
        out[s["name"]] = T.StructType(
            [T.StructField(f["name"], _spark_type(f["datatype"], f["precision"], f["scale"])) for f in s["fields"]]
        )
    for t in spec["transformations"]:
        if t["type"] != "Lookup Procedure":
            continue
        fields = [f for f in t["fields"] if f["porttype"] in ("LOOKUP", "LOOKUP/OUTPUT")]
        override = t["attributes"].get("Lookup Sql Override", "")
        # columns referenced by an override but not exposed as ports (e.g. JPM_PROFILE_ID)
        names = {f["name"] for f in fields}
        extra = [m for m in re.findall(r"\.(\w+) as", override) if m not in names]
        out[LOOKUP_TABLE_ALIASES[t["name"]]] = T.StructType(
            [T.StructField(f["name"], _spark_type(f["datatype"], f["precision"], f["scale"])) for f in fields]
            + [T.StructField(m, T.StringType()) for m in extra]
        )
    return out


def _default(dtype: T.DataType):
    if isinstance(dtype, T.StringType):
        return " "
    if isinstance(dtype, (T.DecimalType,)):
        return D(0)
    if isinstance(dtype, (T.IntegerType, T.LongType)):
        return 0
    return None


def _row(schema: T.StructType, values: dict) -> tuple:
    unknown = set(values) - {f.name for f in schema.fields}
    if unknown:
        raise KeyError(f"unknown columns {sorted(unknown)}")
    out = []
    for f in schema.fields:
        v = values.get(f.name, _default(f.dataType))
        if isinstance(f.dataType, T.DecimalType) and v is not None and not isinstance(v, D):
            v = D(str(v))
        out.append(v)
    return tuple(out)


def _key(emplid: str, effdt: DT, rcd: int = 0, effseq: int = 0) -> dict:
    return {"EMPLID": emplid, "EMPL_RCD": D(rcd), "EFFDT": effdt, "EFFSEQ": D(effseq)}


# fmt: off
CASES: list[dict] = [
    {"id": "baseline", "key": _key("00010001", DT(2026, 3, 2)),
     "job": {"GVT_NOA_CODE": "702", "COMPANY": "HE", "GVT_SUB_AGENCY": "NA", "STD_HOURS": 40, "GVT_WORK_SCHED": "F",
             "UNION_CD": "8AA", "GVT_XFER_FROM_AGCY": "VA", "GVT_PAR_AUTH_D1": "reg 335.102  ", "GVT_PAR_AUTH_D1_2": "abc   ",
             "HE_AL_BALANCE": "12.50", "HE_SL_BALANCE": 0, "GVT_RETIRE_PLAN": "K", "GVT_TYPE_OF_APPT": "55", "ACTION_DT": DT(2026, 3, 1)},
     "pers": {"CITY": "ROCKVILLE", "STATE": "MD", "LAST_NAME": "SMITH", "FIRST_NAME": "ANN"},
     "nid": {"NATIONAL_ID": "123456789"}, "cit": {"CITIZENSHIP_STATUS": "1"},
     "trk": {"GVT_DATE_WRK": DT(2026, 3, 5)}, "emp": {"PROBATION_DT": DT(2024, 2, 29)}},
    {"id": "award_rcr_excluded", "key": _key("00010002", DT(2026, 3, 3)),
     "job": {"GVT_NOA_CODE": "840", "STD_HOURS": 32, "GVT_WORK_SCHED": "P"},
     "awd": {"ERNCD": "RCR", "GOAL_AMT": "500.00", "GVT_TANG_BEN_AMT": "75.00"}},
    {"id": "award_null_erncd_decode_truthiness", "key": _key("00010003", DT(2026, 3, 3)),
     "job": {"GVT_NOA_CODE": "841", "STD_HOURS": None, "GVT_WORK_SCHED": None},
     "awd": {"ERNCD": None, "GOAL_AMT": "250.00", "GVT_TANG_BEN_AMT": "10.00"}},
    {"id": "award_paid", "key": _key("00010004", DT(2026, 3, 4)),
     "job": {"GVT_NOA_CODE": "879", "GVT_WORK_SCHED": "I", "STD_HOURS": 20},
     "awd": {"ERNCD": "SPA", "GOAL_AMT": "1000.00", "GVT_TANG_BEN_AMT": "90.00"}},
    {"id": "noa_817_second_decode_branch", "key": _key("00010005", DT(2026, 3, 4)),
     "job": {"GVT_NOA_CODE": "817"}, "awd": {"ERNCD": "RCR", "GOAL_AMT": "300.00"}},
    {"id": "blank_and_tab_fields", "key": _key("00010006", DT(2026, 3, 5)),
     "job": {"GVT_NOA_CODE": "721", "UNION_CD": "\t", "GVT_XFER_FROM_AGCY": " ", "COMPANY": None, "GVT_SUB_AGENCY": None,
             "GVT_PAR_AUTH_D1": None, "GVT_PAR_AUTH_D1_2": None},
     "emp": {"GVT_CURR_APT_AUTH1": " ", "GVT_CURR_APT_AUTH2": "ZLM"}},
    {"id": "all_lookups_miss", "key": _key("00010007", DT(2026, 3, 5)),
     "job": {"GVT_NOA_CODE": "100", "GVT_XFER_FROM_AGCY": None},
     "pers": None, "nid": None, "cit": None, "trk": None, "emp": None, "awd": None, "fill": None, "jpm": None},
    {"id": "jpm_multi_match_use_any_value", "key": _key("00010008", DT(2026, 3, 6)),
     "job": {"GVT_NOA_CODE": "815", "HE_NOA_EXT": "0"}, "awd": {"GOAL_AMT": "5000.00", "EARNINGS_END_DT": DT(2027, 3, 6)},
     "jpm": [{"JPM_CAT_ITEM_ID": "13", "MAJOR_CODE": "0701", "JPM_INTEGER_2": 2001},
             {"JPM_CAT_ITEM_ID": "17", "MAJOR_CODE": "1101", "JPM_INTEGER_2": 2010},
             {"JPM_CAT_ITEM_ID": "21", "MAJOR_CODE": "9999", "JPM_INTEGER_2": 2015, "EFF_STATUS": "I"}]},
    {"id": "relocation_948_suffix_1", "key": _key("00010009", DT(2026, 3, 6)),
     "job": {"GVT_NOA_CODE": "948", "HE_NOA_EXT": "1"}, "awd": {"GOAL_AMT": "800.00", "EARNINGS_END_DT": DT(2027, 1, 31)}},
    {"id": "severance_and_buyout", "key": _key("00010010", DT(2026, 3, 9)),
     "job": {"GVT_NOA_CODE": "825", "ACTION": "BON"}, "awd": {"GOAL_AMT": "25000.00"}},
    {"id": "prior_year_event_sequence", "key": _key("00010011", DT(2025, 12, 28)),
     "job": {"GVT_NOA_CODE": "702"}},
    {"id": "prior_year_event_sequence_2", "key": _key("00010012", DT(2025, 12, 29), effseq=1),
     "job": {"GVT_NOA_CODE": "702"}},
    {"id": "year_without_sequence_row_rejected", "key": _key("00010013", DT(2024, 6, 30)),
     "job": {"GVT_NOA_CODE": "702"}},
    {"id": "agency_code_truncated_to_target", "key": _key("00010014", DT(2026, 3, 10)),
     "job": {"GVT_NOA_CODE": "702", "COMPANY": "HHS", "GVT_SUB_AGENCY": "AB"}},
    {"id": "probation_month_end", "key": _key("00010015", DT(2026, 3, 10), rcd=1),
     "job": {"GVT_NOA_CODE": "702"}, "emp": {"PROBATION_DT": DT(2026, 3, 31, 14, 30)}},
]
SEQUENCE_ROWS = [{"EHRP_YEAR": 2025, "EHRP_SEQ_NUMBER": 7100}, {"EHRP_YEAR": 2026, "EHRP_SEQ_NUMBER": 1200}]
# fmt: on

LOOKUP_KEYS = {
    "emp": "PS_GVT_EMPLOYMENT", "nid": "PS_GVT_PERS_NID", "awd": "PS_GVT_AWD_DATA", "trk": "PS_GVT_EE_DATA_TRK",
    "fill": "PS_HE_FILL_POS", "cit": "PS_GVT_CITIZENSHIP", "pers": "PS_GVT_PERS_DATA",
}


def build_tables(spark: SparkSession, cases: list[dict] | None = None) -> dict[str, DataFrame]:
    cases = CASES if cases is None else cases
    schemas = table_schemas()
    rows: dict[str, list[tuple]] = {name: [] for name in schemas}
    for case in cases:
        k = case["key"]
        rows["NWK_NEW_EHRP_ACTIONS_TBL"].append(_row(schemas["NWK_NEW_EHRP_ACTIONS_TBL"], {
            n: v for n, v in k.items() if n in schemas["NWK_NEW_EHRP_ACTIONS_TBL"].fieldNames()}))
        rows["PS_GVT_JOB"].append(_row(schemas["PS_GVT_JOB"], {**k, "ACTION": "PAY", **case.get("job", {})}))
        for short, table in LOOKUP_KEYS.items():
            vals = case.get(short, {})
            if vals is None:
                continue
            fields = schemas[table].fieldNames()
            rows[table].append(_row(schemas[table], {**{n: v for n, v in k.items() if n in fields}, **vals}))
        jpm = case.get("jpm", [])
        if jpm is not None:
            for item in jpm:
                rows["PS_JPM_JP_ITEMS"].append(_row(schemas["PS_JPM_JP_ITEMS"], {
                    "JPM_PROFILE_ID": k["EMPLID"], "JPM_CAT_TYPE": "DEG", "EFF_STATUS": "A", **item}))
    for r in SEQUENCE_ROWS:
        rows["SEQUENCE_NUM_TBL"].append(_row(schemas["SEQUENCE_NUM_TBL"], r))
    return {name: spark.createDataFrame(rows[name], schemas[name]) for name in schemas}
