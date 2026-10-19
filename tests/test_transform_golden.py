"""Golden expectations for the synthetic batch, hand-derived from PowerCenter semantics."""
import datetime as dt
from decimal import Decimal

import pytest

from pipelines.ehrp2biis.sample_data import build_tables
from pipelines.ehrp2biis.transform import Ehrp2BiisTransform

RUN_DATE = dt.date(2026, 3, 12)


@pytest.fixture(scope="module")
def result(spark):
    res = Ehrp2BiisTransform(spark).run(build_tables(spark), RUN_DATE)
    prim = {r["EMP_ID"]: r.asDict() for r in res.targets["NWK_ACTION_PRIMARY_TBL"].collect()}
    sec = {r["EVENT_ID"]: r.asDict() for r in res.targets["NWK_ACTION_SECONDARY_TBL"].collect()}
    trk = {r["EMPLID"]: r.asDict() for r in res.targets["EHRP_RECS_TRACKING_TBL"].collect()}
    rej = [r.asDict() for r in res.rejects.collect()]
    return prim, sec, trk, rej


def test_row_counts_and_rejects(result):
    prim, sec, trk, rej = result
    assert len(prim) == len(sec) == len(trk) == 14
    # 2024 has no SEQUENCE_NUM_TBL row -> EVENT_ID NULL -> NOT NULL violation on all 3 targets (legacy .bad file)
    assert {(r["TARGET_TABLE"], r["EMPLID"]) for r in rej} == {
        ("NWK_ACTION_PRIMARY_TBL", "00010013"), ("NWK_ACTION_SECONDARY_TBL", "00010013"), ("EHRP_RECS_TRACKING_TBL", "00010013")}


def test_load_id_and_load_date(result):
    prim, _, trk, _ = result
    for r in prim.values():
        assert r["LOAD_ID"] == "NK20260312"
        assert r["LOAD_DATE"] == dt.datetime(2026, 3, 12)
        # AS_OF_DAY is VARCHAR2(10): implicit date->string with the default format, truncated to 10
        assert r["AS_OF_DAY"] == "03/12/2026"
    assert {r["LOAD_DATE"] for r in trk.values()} == {dt.datetime(2026, 3, 12)}


def test_event_id_sequence_restarts_per_year(result):
    prim, sec, trk, _ = result
    assert prim["00010011"]["EVENT_ID"] == 7101 and prim["00010012"]["EVENT_ID"] == 7102
    ids_2026 = sorted(r["EVENT_ID"] for r in prim.values() if r["EVENT_EFF_DTE"].year == 2026)
    assert ids_2026 == list(range(1201, 1213))
    assert prim["00010001"]["EVENT_ID"] == 1201  # earliest EFFDT in 2026
    assert set(sec) == {r["EVENT_ID"] for r in prim.values()}
    assert {r["BIIS_EVENT_ID"] for r in trk.values()} == set(sec)


def test_decode_truthiness_cash_award(result):
    prim, _, _, _ = result
    assert prim["00010002"]["CASH_AWARD_AMT"] is None  # 840 + ERNCD RCR excluded
    assert prim["00010003"]["CASH_AWARD_AMT"] is None  # 841 + ERNCD NULL: NOT IN(NULL) is NULL -> no match
    assert prim["00010003"]["CASH_AWARD_BNFT_AMT"] is None
    assert prim["00010004"]["CASH_AWARD_AMT"] == Decimal("1000.0000")
    assert prim["00010004"]["CASH_AWARD_BNFT_AMT"] == Decimal("90.00")
    assert prim["00010005"]["CASH_AWARD_AMT"] == Decimal("300.0000")  # 817: second DECODE branch ignores ERNCD


def test_is_spaces_and_blank_handling(result):
    prim, _, _, _ = result
    b, base = prim["00010006"], prim["00010001"]
    assert b["UNION_CD"] is None  # tab only
    assert b["AGCY_SUBELEMENT_PRIOR_CD"] is None  # ' '
    assert b["CURR_APPT_AUTH1_CD"] is None and b["CURR_APPT_AUTH2_CD"] == "ZLM"
    assert base["UNION_CD"] == "8AA" and base["AGCY_SUBELEMENT_PRIOR_CD"] == "VA00"
    assert prim["00010007"]["AGCY_SUBELEMENT_PRIOR_CD"] is None  # NULL


def test_concat_null_semantics_and_byte_preservation(result):
    prim, _, _, _ = result
    assert prim["00010006"]["AGCY_ASSIGN_CD"] is None  # NULL || NULL
    assert prim["00010006"]["LEGAL_AUTH_TXT"] is None
    assert prim["00010001"]["AGCY_ASSIGN_CD"] == "HENA"
    assert prim["00010001"]["LEGAL_AUTH_TXT"] == "REG 335.102  ABC"  # D1 trailing blanks kept, D1_2 RTRIM'd
    assert prim["00010001"]["EMP_RESID_CITY_ST_NAME"] == "ROCKVILLE, MD"
    assert prim["00010002"]["EMP_RESID_CITY_ST_NAME"] == " ,  "  # PeopleSoft ' ' CITY/STATE kept byte-for-byte
    assert prim["00010007"]["EMP_RESID_CITY_ST_NAME"] == ", "  # lookup miss: NULL || ', ' || NULL
    assert prim["00010014"]["AGCY_ASSIGN_CD"] == "HHSA"  # truncated to VARCHAR2(4)


def test_lookup_semantics(result):
    prim, _, _, _ = result
    miss = prim["00010007"]
    assert miss["SSN"] is None and miss["EMP_LAST_NAME"] is None and miss["US_CITIZENSHIP_CD"] == "8"
    assert prim["00010001"]["SSN"] == "123456789"  # NATIONAL_ID pass-through (no masking in this mapping)
    assert prim["00010001"]["US_CITIZENSHIP_CD"] == "1"
    jpm = prim["00010008"]
    assert (jpm["EDUCATION_LEVEL_CD"], jpm["INSTRUCTIONAL_PROGRAM_CD"]) == ("13", "0701")  # inactive row filtered by override
    assert jpm["YEAR_DEGREE_ATTAINED_DTE"] is not None


def test_numeric_and_date_rules(result):
    prim, sec, _, _ = result
    assert prim["00010001"]["BASE_HRS"] == Decimal("80.00") and prim["00010001"]["PERMANENT_TEMP_POSITION_CD"] == "1"
    assert prim["00010004"]["BASE_HRS"] == Decimal("20.00")  # GVT_WORK_SCHED = 'I'
    assert prim["00010003"]["BASE_HRS"] is None and prim["00010003"]["PERMANENT_TEMP_POSITION_CD"] == "0"
    assert prim["00010001"]["PROB_TRIAL_PERIOD_START_DTE"] == dt.datetime(2023, 2, 28)
    assert prim["00010015"]["PROB_TRIAL_PERIOD_START_DTE"] == dt.datetime(2025, 3, 31, 14, 30)
    assert prim["00010001"]["EVENT_SUBMITTED_DTE"] == dt.datetime(2026, 3, 5)  # GVT_DATE_WRK wins over ACTION_DT
    assert prim["00010001"]["ANN_LV_CUR_BAL_HRS"] == Decimal(13)  # 12.50 into NUMBER(6,0): Oracle rounds half up
    assert prim["00010001"]["SICK_LV_CUR_BAL_HRS"] is None  # 0 -> NULL
    by_emp = {p["EMP_ID"]: sec[p["EVENT_ID"]] for p in prim.values()}
    assert by_emp["00010001"]["TSP_VESTING_CD"] == "2"
    assert by_emp["00010008"]["RECRUITMENT_BONUS_AMT"] is not None
    assert by_emp["00010009"]["RELOCATION_EXP_DTE"] == dt.datetime(2027, 1, 31)
    assert by_emp["00010010"]["BUYOUT_AMT"] == Decimal("25000.00")
    assert by_emp["00010010"]["BUYOUT_EFF_DTE"] == dt.datetime(2026, 3, 9)
