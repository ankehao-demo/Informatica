"""Catalog of every non-trivial exp_MAIN2BIIS / exp_GET_EFFDT_YEAR port.

Each entry carries:
  * ``sql``     -- Spark SQL equivalent (rendered into the mapping spec; a unit test
                   evaluates it and asserts it equals the PySpark builder)
  * ``pyspark`` -- builder ``f(c, ctx) -> Column`` where ``c(port)`` resolves an input
                   port of the transformation and ``ctx`` holds run parameters.

The Informatica expression text itself is read from mapping_spec.json, and
tests/test_spec_coverage.py fails if an expression in the XML has no entry here.
Ports whose expression is the port itself (pass-through) are not listed.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import Column
from pyspark.sql import functions as F

from . import infa as I

AWARD_NOAS = ("840", "841", "842", "843", "844", "845", "848", "849", "873", "874", "875", "876", "877", "878", "879")
SEVERANCE_NOAS = ("304", "312", "356", "357")
_AWARD_SQL = "GVT_NOA_CODE IN (" + ", ".join(f"'{n}'" for n in AWARD_NOAS) + ")"
_SEV_SQL = "GVT_NOA_CODE IN (" + ", ".join(f"'{n}'" for n in SEVERANCE_NOAS) + ")"
_SPACES = r"'^[ \\t\\n\\r\\f\\x0B]*$'"


def _sql_concat(*parts: str) -> str:
    nulls = " AND ".join(f"{p} IS NULL" for p in parts if not p.startswith("'"))
    body = "concat(" + ", ".join(p if p.startswith("'") else f"coalesce(CAST({p} AS STRING), '')" for p in parts) + ")"
    if all(not p.startswith("'") for p in parts):
        return f"CASE WHEN {nulls} THEN NULL ELSE {body} END"
    return body


@dataclass(frozen=True)
class Port:
    name: str
    sql: str
    pyspark: Callable[..., Column]
    note: str = ""


def _award(c):
    return I.in_(c("GVT_NOA_CODE"), *AWARD_NOAS) & ~I.in_(c("ERNCD"), "RCR", "RLC")


def _zero(name: str, src: str) -> Port:
    return Port(name, f"CASE WHEN {src} = 0 THEN NULL ELSE {src} END", lambda c, ctx, s=src: I.zero_to_null(c(s)))


def _blank_to_null(name: str, src: str) -> Port:
    return Port(
        name,
        f"CASE WHEN {src} RLIKE {_SPACES} THEN NULL ELSE {src} END",
        lambda c, ctx, s=src: I.iif(I.is_spaces(c(s)), None, c(s)),
        "PeopleSoft stores blank CHAR fields as a single space; IS_SPACES(' ') is TRUE -> NULL.",
    )


_ZERO_PORTS = {
    "o_HE_AL_RED_CRED": "HE_AL_RED_CRED", "o_HE_AL_BALANCE": "HE_AL_BALANCE", "o_HE_LUMP_HRS": "HE_LUMP_HRS",
    "o_HE_AL_CARRYOVER": "HE_AL_CARRYOVER", "o_HE_RES_LASTYR": "HE_RES_LASTYR", "o_HE_RES_BALANCE": "HE_RES_BALANCE",
    "o_HE_RES_TWOYRS": "HE_RES_TWOYRS", "o_HE_RES_THREEYRS": "HE_RES_THREEYRS", "o_HE_AL_ACCRUAL": "HE_AL_ACCRUAL",
    "o_HE_AL_TOTAL": "HE_AL_TOTAL", "o_HE_AWOP_SEP": "HE_AWOP_SEP", "o_HE_AWOP_WIGI": "HE_AWOP_WIGI",
    "o_GVT_ANNUITY_OFFSET": "GVT_ANNUITY_OFFSET", "o_HE_SL_RED_CRED": "HE_SL_RED_CRED",
    "o_HE_SL_BALANCE": "HE_SL_BALANCE", "o_HE_FROZEN_SL": "HE_FROZEN_SL", "o_HE_SL_CARRYOVER": "HE_SL_CARRYOVER",
    "o_HE_SL_ACCRUAL": "HE_SL_ACCRUAL", "o_HE_SL_TOTAL": "HE_SL_TOTAL", "o_APPT_LIMIT_NTE_HRS": "GVT_APPT_LIMIT_HRS",
    "YEAR_DEGREE": "JPM_INTEGER_2",
}

_PORTS: list[Port] = [
    # ---- exp_GET_EFFDT_YEAR ---------------------------------------------------------
    Port("v_curr_year", "year(EFFDT)", lambda c, ctx: I.get_date_part(c("EFFDT"), "YYYY")),
    Port("o_CURRENT_YEAR", "v_curr_year", lambda c, ctx: c("v_curr_year")),
    # ---- exp_MAIN2BIIS: event-id sequence (stateful variable ports) -----------------
    Port("v_CURRENT_YEAR", "year(EFFDT)", lambda c, ctx: I.get_date_part(c("EFFDT"), "YYYY")),
    Port(
        "v_EVENT_ID",
        "EHRP_SEQ_NUMBER + row_number() OVER (PARTITION BY year(EFFDT) ORDER BY EFFDT, EMPLID, EMPL_RCD, EFFSEQ)",
        lambda c, ctx: c("__EVENT_ID"),
        "Stateful row-by-row variable. SQ orders by EFFDT, so each year is one contiguous run: "
        "first row = EHRP_SEQ_NUMBER + 1, then +1 per row. Ties on EFFDT are broken by "
        "EMPLID, EMPL_RCD, EFFSEQ (PowerCenter order within a tie is database-dependent).",
    ),
    Port("v_PREVIOUS_YEAR", "lag(year(EFFDT)) OVER (ORDER BY EFFDT, EMPLID, EMPL_RCD, EFFSEQ)", lambda c, ctx: c("__PREV_YEAR")),
    Port("o_EVENT_ID", "v_EVENT_ID", lambda c, ctx: c("__EVENT_ID")),
    # ---- exp_MAIN2BIIS: business logic -----------------------------------------------
    Port("o_AGCY_ASSIGN_CD", _sql_concat("COMPANY", "GVT_SUB_AGENCY"), lambda c, ctx: I.concat(c("COMPANY"), c("GVT_SUB_AGENCY"))),
    Port(
        "o_AGCY_SUBELEMENT_PRIOR_CD",
        f"CASE WHEN (GVT_XFER_FROM_AGCY RLIKE {_SPACES}) OR GVT_XFER_FROM_AGCY IS NULL THEN NULL "
        "ELSE concat(GVT_XFER_FROM_AGCY, '00') END",
        lambda c, ctx: I.iif(
            I.is_spaces(c("GVT_XFER_FROM_AGCY")) | I.isnull(c("GVT_XFER_FROM_AGCY")),
            None,
            I.concat(c("GVT_XFER_FROM_AGCY"), "00"),
        ),
    ),
    Port(
        "o_BASE_HOURS",
        "CASE WHEN GVT_WORK_SCHED = 'I' THEN STD_HOURS ELSE STD_HOURS * 2 END",
        lambda c, ctx: I.iif(c("GVT_WORK_SCHED") == "I", c("STD_HOURS"), c("STD_HOURS") * 2),
        "NULL GVT_WORK_SCHED -> IIF condition NULL -> FALSE -> STD_HOURS * 2.",
    ),
    Port(
        "o_LEG_AUTH_TXT_1",
        "upper(" + _sql_concat("GVT_PAR_AUTH_D1", "rtrim(GVT_PAR_AUTH_D1_2)") + ")",
        lambda c, ctx: I.upper(I.concat(c("GVT_PAR_AUTH_D1"), I.rtrim(c("GVT_PAR_AUTH_D1_2")))),
        "Only the second operand is RTRIM'd; trailing blanks of D1 are kept byte-for-byte.",
    ),
    Port(
        "o_LEG_AUTH_TXT_2",
        "upper(" + _sql_concat("GVT_PAR_AUTH_D2", "rtrim(GVT_PAR_AUTH_D2_2)") + ")",
        lambda c, ctx: I.upper(I.concat(c("GVT_PAR_AUTH_D2"), I.rtrim(c("GVT_PAR_AUTH_D2_2")))),
    ),
    Port(
        "o_PERM_TEMP_POSITION_CD",
        "CAST(CASE WHEN STD_HOURS < 40 THEN 3 WHEN STD_HOURS >= 40 THEN 1 ELSE 0 END AS STRING)",
        lambda c, ctx: I.iif(c("STD_HOURS") < 40, 3, I.iif(c("STD_HOURS") >= 40, 1, 0)).cast("string"),
        "NULL STD_HOURS fails both tests -> '0'.",
    ),
    _blank_to_null("o_UNION_CD", "UNION_CD"),
    Port(
        "o_EMP_RESID_CITY_STATE_NAME",
        _sql_concat("CITY", "', '", "STATE"),
        lambda c, ctx: I.concat(c("CITY"), ", ", c("STATE")),
        "Never NULL: a NULL CITY/STATE is concatenated as ''. Result for a lookup miss is ', '.",
    ),
    Port("o_EFFSEQ", "CAST(CAST(EFFSEQ AS BIGINT) AS STRING)", lambda c, ctx: I.to_char_int(c("EFFSEQ"))),
    Port("v_MONTH_NUMBER", "month(:run_date)", lambda c, ctx: F.month(ctx["run_ts"])),
    Port("v_DAY_NUMBER", "day(:run_date)", lambda c, ctx: F.dayofmonth(ctx["run_ts"])),
    Port("v_MONTH_CHAR", "lpad(month(:run_date), 2, '0')", lambda c, ctx: F.lpad(F.month(ctx["run_ts"]).cast("string"), 2, "0")),
    Port("v_DAY_CHAR", "lpad(day(:run_date), 2, '0')", lambda c, ctx: F.lpad(F.dayofmonth(ctx["run_ts"]).cast("string"), 2, "0")),
    Port(
        "o_LOAD_ID",
        "concat('NK', date_format(:run_date, 'yyyyMMdd'))",
        lambda c, ctx: F.concat(F.lit("NK"), F.date_format(ctx["run_ts"], "yyyyMMdd")),
        "Reconciliation key. SYSDATE is pinned to the run's logical date (Airflow ds) so reruns are stable.",
    ),
    Port(
        "o_LOAD_DATE",
        "date_trunc('DD', :run_date)",
        lambda c, ctx: F.date_trunc("DD", ctx["run_ts"]),
        "trunc(sysdate) -> midnight of the run date. Reconciliation value.",
    ),
    Port("o_LINE_SEQ", "'01N'", lambda c, ctx: F.lit("01N"), "Unconnected port (dead logic)."),
    Port(
        "o_CASH_AWARD_AMT",
        f"CASE WHEN {_AWARD_SQL} AND NOT (ERNCD IN ('RCR', 'RLC')) THEN GOAL_AMT "
        "WHEN GVT_NOA_CODE IN ('817', '889') THEN GOAL_AMT ELSE NULL END",
        lambda c, ctx: I.decode_true(
            _award(c), c("GOAL_AMT"), I.in_(c("GVT_NOA_CODE"), "817", "889"), c("GOAL_AMT"), default=None
        ),
        "DECODE(TRUE,...) truthiness: NULL ERNCD makes NOT IN(...) NULL, so an award NOA with "
        "no ERNCD falls through to the next branch (and finally NULL).",
    ),
    Port(
        "o_CASH_AWARD_BNFT_AMT",
        f"CASE WHEN {_AWARD_SQL} AND NOT (ERNCD IN ('RCR', 'RLC')) THEN GVT_TANG_BEN_AMT ELSE NULL END",
        lambda c, ctx: I.iif(_award(c), c("GVT_TANG_BEN_AMT"), None),
    ),
    Port(
        "o_RECRUITMENT_BONUS_AMT",
        "CASE WHEN GVT_NOA_CODE = '815' OR (GVT_NOA_CODE = '948' AND HE_NOA_EXT = '0') THEN GOAL_AMT ELSE NULL END",
        lambda c, ctx: I.iif(
            (c("GVT_NOA_CODE") == "815") | ((c("GVT_NOA_CODE") == "948") & (c("HE_NOA_EXT") == "0")), c("GOAL_AMT"), None
        ),
    ),
    Port(
        "o_RELOCATION_BONUS_AMT",
        "CASE WHEN GVT_NOA_CODE IN ('816') THEN GOAL_AMT ELSE NULL END",
        lambda c, ctx: I.iif(I.in_(c("GVT_NOA_CODE"), "816"), c("GOAL_AMT"), None),
    ),
    Port(
        "o_TIME_OFF_AWARD_AMT",
        "CASE WHEN GVT_NOA_CODE IN ('846', '847') THEN OTH_HRS ELSE NULL END",
        lambda c, ctx: I.iif(I.in_(c("GVT_NOA_CODE"), "846", "847"), c("OTH_HRS"), None),
    ),
    Port(
        "o_TIME_OFF_GRANTED_HRS",
        "CASE WHEN GVT_NOA_CODE IN ('846', '847') THEN OTH_HRS ELSE NULL END",
        lambda c, ctx: I.iif(I.in_(c("GVT_NOA_CODE"), "846", "847"), c("OTH_HRS"), None),
    ),
    Port(
        "o_CITIZENSHIP_STATUS",
        "CASE WHEN CITIZENSHIP_STATUS_IN = '1' OR CITIZENSHIP_STATUS_IN = '2' THEN '1' ELSE '8' END",
        lambda c, ctx: I.iif((c("CITIZENSHIP_STATUS_IN") == "1") | (c("CITIZENSHIP_STATUS_IN") == "2"), "1", "8"),
        "Lookup miss (NULL) -> '8'.",
    ),
    Port(
        "o_TSP_VESTING_CD",
        "CAST(CASE WHEN GVT_RETIRE_PLAN IN ('K', 'M') THEN CASE WHEN GVT_TYPE_OF_APPT IN ('55', '34', '36', '46', '44') "
        "THEN 2 ELSE 3 END ELSE 0 END AS STRING)",
        lambda c, ctx: I.iif(
            I.in_(c("GVT_RETIRE_PLAN"), "K", "M"), I.iif(I.in_(c("GVT_TYPE_OF_APPT"), "55", "34", "36", "46", "44"), 2, 3), 0
        ).cast("string"),
    ),
    Port(
        "o_PERMANENT_TEMP_POSITION_CD",
        "CASE WHEN REG_TEMP = 'R' AND GVT_WORK_SCHED = 'F' THEN '1' WHEN REG_TEMP IN ('R', 'T') AND GVT_WORK_SCHED IN ('P', 'I') "
        "THEN '3' WHEN REG_TEMP = 'T' AND GVT_WORK_SCHED = 'F' THEN '2' ELSE NULL END",
        lambda c, ctx: I.iif(
            (c("REG_TEMP") == "R") & (c("GVT_WORK_SCHED") == "F"),
            "1",
            I.iif(
                I.in_(c("REG_TEMP"), "R", "T") & I.in_(c("GVT_WORK_SCHED"), "P", "I"),
                "3",
                I.iif((c("REG_TEMP") == "T") & (c("GVT_WORK_SCHED") == "F"), "2", None),
            ),
        ),
        "Unconnected port (dead logic); target PERMANENT_TEMP_POSITION_CD is fed by o_PERM_TEMP_POSITION_CD.",
    ),
    Port(
        "o_EVENT_SUBMITTED_DTE",
        "CASE WHEN GVT_DATE_WRK IS NULL THEN ACTION_DT ELSE GVT_DATE_WRK END",
        lambda c, ctx: I.iif(I.isnull(c("GVT_DATE_WRK")), c("ACTION_DT"), c("GVT_DATE_WRK")),
    ),
    Port(
        "o_RECRUITMENT_EXP_DTE",
        "CASE WHEN GVT_NOA_CODE = '815' OR (GVT_NOA_CODE = '948' AND HE_NOA_EXT = '0') THEN EARNINGS_END_DT ELSE NULL END",
        lambda c, ctx: I.iif(
            (c("GVT_NOA_CODE") == "815") | ((c("GVT_NOA_CODE") == "948") & (c("HE_NOA_EXT") == "0")),
            c("EARNINGS_END_DT"),
            None,
        ),
    ),
    Port(
        "o_RELOCATION_EXP_DTE",
        "CASE WHEN GVT_NOA_CODE = '816' OR (GVT_NOA_CODE = '948' AND HE_NOA_EXT = '1') THEN EARNINGS_END_DT ELSE NULL END",
        lambda c, ctx: I.iif(
            (c("GVT_NOA_CODE") == "816") | ((c("GVT_NOA_CODE") == "948") & (c("HE_NOA_EXT") == "1")),
            c("EARNINGS_END_DT"),
            None,
        ),
    ),
    _blank_to_null("o_GVT_CURR_APT_AUTH1", "GVT_CURR_APT_AUTH1"),
    _blank_to_null("o_GVT_CURR_APT_AUTH2", "GVT_CURR_APT_AUTH2"),
    Port(
        "FEGLI_LIVING_BNFT_CD",
        "CASE GVT_NOA_CODE WHEN '805' THEN 'F' WHEN '806' THEN 'P' ELSE NULL END",
        lambda c, ctx: I.decode_value(c("GVT_NOA_CODE"), "805", "F", "806", "P", default=None),
        "Unconnected port (dead logic); target FEGLI_LIVING_BNFT_CD is fed by GVT_FEGLI_LIVING.",
    ),
    Port(
        "LWOP_START_DATE",
        "CASE WHEN GVT_NOA_CODE IN ('460', '473') THEN EFFDT ELSE NULL END",
        lambda c, ctx: I.iif(I.in_(c("GVT_NOA_CODE"), "460", "473"), c("EFFDT"), None),
    ),
    Port(
        "BUYOUT_EFFDT",
        "CASE WHEN GVT_NOA_CODE = '825' THEN EFFDT ELSE NULL END",
        lambda c, ctx: I.iif(c("GVT_NOA_CODE") == "825", c("EFFDT"), None),
    ),
    Port("o_BUSINESS_TITLE", "upper(BUSINESS_TITLE)", lambda c, ctx: I.upper(c("BUSINESS_TITLE"))),
    Port(
        "o_BUYOUT_AMT",
        "CASE WHEN GVT_NOA_CODE = '825' AND ACTION = 'BON' THEN GOAL_AMT ELSE NULL END",
        lambda c, ctx: I.iif((c("GVT_NOA_CODE") == "825") & (c("ACTION") == "BON"), c("GOAL_AMT"), None),
    ),
    Port(
        "o_SEVERANCE_PAY_AMT",
        f"CASE WHEN {_SEV_SQL} THEN OTH_PAY ELSE NULL END",
        lambda c, ctx: I.iif(I.in_(c("GVT_NOA_CODE"), *SEVERANCE_NOAS), c("OTH_PAY"), None),
    ),
    Port(
        "o_SEVERANCE_TOTAL_AMT",
        f"CASE WHEN {_SEV_SQL} THEN GOAL_AMT ELSE NULL END",
        lambda c, ctx: I.iif(I.in_(c("GVT_NOA_CODE"), *SEVERANCE_NOAS), c("GOAL_AMT"), None),
    ),
    Port(
        "o_SEVERANCE_PAY_START_DTE",
        f"CASE WHEN {_SEV_SQL} THEN EFFDT ELSE NULL END",
        lambda c, ctx: I.iif(I.in_(c("GVT_NOA_CODE"), *SEVERANCE_NOAS), c("EFFDT"), None),
    ),
    Port(
        "o_PROBATION_DT",
        "CASE WHEN PROBATION_DT IS NULL THEN NULL ELSE PROBATION_DT + make_interval(0, -12) END",
        lambda c, ctx: I.iif(I.isnull(c("PROBATION_DT")), None, I.add_to_date_months(c("PROBATION_DT"), -12)),
        "ADD_TO_DATE(...,'MM',-12): day clamped to month end (Feb 29 -> Feb 28), time kept.",
    ),
]
_PORTS += [_zero(k, v) for k, v in _ZERO_PORTS.items()]

CATALOG: dict[str, Port] = {p.name: p for p in _PORTS}
