"""Idempotent MERGE writers + dual-run reconciliation harness."""
import datetime as dt

import pytest
from pyspark.sql import functions as F

from pipelines.ehrp2biis.sample_data import CASES, build_tables
from pipelines.ehrp2biis.transform import Ehrp2BiisTransform, load_id_for
from pipelines.ehrp2biis.writers import Store, merge_targets
from pipelines.reconcile.reconcile import reconcile, render_markdown

RUN_DATE = dt.date(2026, 3, 12)
LOAD_ID = load_id_for(RUN_DATE)


def _run(spark, cases=None):
    return Ehrp2BiisTransform(spark).run(build_tables(spark, cases), RUN_DATE).targets


def _counts(store):
    return {t: store.read(t).count() for t in ("NWK_ACTION_PRIMARY_TBL", "NWK_ACTION_SECONDARY_TBL", "EHRP_RECS_TRACKING_TBL")}


def test_merge_is_idempotent_and_replaces_the_slice(spark, tmp_path):
    store = Store(spark, str(tmp_path / "silver"))
    targets = _run(spark)
    merge_targets(store, targets, LOAD_ID, RUN_DATE)
    first = _counts(store)
    merge_targets(store, targets, LOAD_ID, RUN_DATE)  # rerun, same batch
    assert _counts(store) == first == {"NWK_ACTION_PRIMARY_TBL": 14, "NWK_ACTION_SECONDARY_TBL": 14, "EHRP_RECS_TRACKING_TBL": 14}

    # rerun after the source batch lost its last 2026 row -> stale rows leave all three targets
    smaller = [c for c in CASES if c["id"] != "probation_month_end"]
    merge_targets(store, _run(spark, smaller), LOAD_ID, RUN_DATE)
    assert _counts(store) == {"NWK_ACTION_PRIMARY_TBL": 13, "NWK_ACTION_SECONDARY_TBL": 13, "EHRP_RECS_TRACKING_TBL": 13}
    assert store.read("NWK_ACTION_PRIMARY_TBL").filter(F.col("EMP_ID") == "00010015").count() == 0


def test_merge_keeps_other_loads(spark, tmp_path):
    store = Store(spark, str(tmp_path / "silver"))
    merge_targets(store, _run(spark), LOAD_ID, RUN_DATE)
    other = Ehrp2BiisTransform(spark).run(build_tables(spark), dt.date(2026, 3, 13)).targets
    # a different day uses a different sequence snapshot in production; shift ids to avoid overlap here
    other = {t: (df.withColumn("EVENT_ID", F.col("EVENT_ID") + 100000) if "EVENT_ID" in df.columns
                 else df.withColumn("BIIS_EVENT_ID", F.col("BIIS_EVENT_ID") + 100000)) for t, df in other.items()}
    merge_targets(store, other, "NK20260313", dt.date(2026, 3, 13))
    assert store.read("NWK_ACTION_PRIMARY_TBL").groupBy("LOAD_ID").count().count() == 2
    assert _counts(store)["NWK_ACTION_SECONDARY_TBL"] == 28


@pytest.fixture(scope="module")
def outputs(spark):
    # materialize once: production reads shallow table plans, not the whole transform lineage
    return {t: df.localCheckpoint() for t, df in _run(spark).items()}


def test_reconcile_identical_runs_match(outputs):
    reports = reconcile(outputs, outputs, LOAD_ID)
    assert all(r.clean for r in reports)
    assert [r.matched_rows for r in reports] == [14, 14, 14]
    assert "MATCH" in render_markdown(LOAD_ID, reports)


def test_reconcile_reports_row_and_column_differences(spark, outputs):
    legacy = dict(outputs)
    p = outputs["NWK_ACTION_PRIMARY_TBL"]
    # legacy had a trailing blank in one city (byte-level), and one row the new run does not produce
    legacy["NWK_ACTION_PRIMARY_TBL"] = (
        p.withColumn("EMP_RESID_CITY_ST_NAME",
                     F.when(F.col("EMP_ID") == "00010001", F.lit("ROCKVILLE, MD ")).otherwise(F.col("EMP_RESID_CITY_ST_NAME")))
        .unionByName(p.filter(F.col("EMP_ID") == "00010002").withColumn("EMP_ID", F.lit("09999999")))
    )
    t = outputs["EHRP_RECS_TRACKING_TBL"]
    legacy["EHRP_RECS_TRACKING_TBL"] = t.filter(F.col("EMPLID") != "00010004")
    s = outputs["NWK_ACTION_SECONDARY_TBL"]
    legacy["NWK_ACTION_SECONDARY_TBL"] = s.withColumn("TSP_VESTING_CD", F.lit("9"))

    prim, sec, trk = reconcile(legacy, outputs, LOAD_ID)
    assert (prim.legacy_rows, prim.new_rows, prim.only_in_legacy, prim.only_in_new) == (15, 14, 1, 0)
    assert prim.column_mismatches == {"EMP_RESID_CITY_ST_NAME": 1}
    assert any(x.get("legacy") == "'ROCKVILLE, MD '" for x in prim.samples)
    assert sec.column_mismatches == {"TSP_VESTING_CD": 14}
    assert (trk.only_in_legacy, trk.only_in_new) == (0, 1)
    md = render_markdown(LOAD_ID, [prim, sec, trk])
    assert "DIFFERENCES FOUND" in md and "EMP_RESID_CITY_ST_NAME" in md
