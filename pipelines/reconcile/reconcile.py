"""Dual-run reconciliation: legacy (Informatica -> Oracle) vs new (Databricks) output.

Both pipelines consume the same input batch (the bronze snapshot for a LOAD_ID is
the frozen copy of NKNIGHT.NWK_NEW_EHRP_ACTIONS_TBL + PeopleSoft rows). The report
is keyed on LOAD_ID + the target's business primary key:

    NWK_ACTION_PRIMARY_TBL    LOAD_ID, EMP_ID, EMPL_REC_NO, EVENT_EFF_DTE, EFFSEQ
    NWK_ACTION_SECONDARY_TBL  LOAD_ID, EMP_ID, EMPL_REC_NO, EVENT_EFF_DTE, EFFSEQ
                              (resolved through the same side's primary row by EVENT_ID)
    EHRP_RECS_TRACKING_TBL    LOAD_ID ('NK'||LOAD_DATE), EMPLID, EMPL_RCD, EFFDT, EFFSEQ

EVENT_ID itself is compared as a value column. Strings are compared exactly, so a
trailing blank or a ' ' vs NULL difference is reported (byte-level parity).

    python -m pipelines.reconcile.reconcile --run-date 2026-03-12 \
        --legacy oracle|<delta base>|<dir of parquet> --new <delta base> --report-dir out/
Exit code 1 when any difference is found.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

TARGETS = ("NWK_ACTION_PRIMARY_TBL", "NWK_ACTION_SECONDARY_TBL", "EHRP_RECS_TRACKING_TBL")
PRIMARY_KEY = ("EMP_ID", "EMPL_REC_NO", "EVENT_EFF_DTE", "EFFSEQ")
TRACKING_KEY = ("EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ")
SAMPLES = 20


@dataclass
class TableReport:
    table: str
    key: list[str]
    legacy_rows: int
    new_rows: int
    matched_rows: int
    only_in_legacy: int
    only_in_new: int
    duplicate_keys_legacy: int
    duplicate_keys_new: int
    rows_with_mismatch: int
    column_mismatches: dict[str, int] = field(default_factory=dict)
    samples: list[dict] = field(default_factory=list)

    @property
    def clean(self) -> bool:
        return not (self.only_in_legacy or self.only_in_new or self.rows_with_mismatch
                    or self.duplicate_keys_legacy or self.duplicate_keys_new)


def slice_for_load(tables: dict[str, DataFrame], load_id: str) -> dict[str, DataFrame]:
    """Restrict the three targets to one load and attach the reconciliation key."""
    load_date = dt.datetime.strptime(load_id[2:], "%Y%m%d")
    prim = tables["NWK_ACTION_PRIMARY_TBL"].filter(F.col("LOAD_ID") == load_id)
    prim_keys = prim.select(F.col("EVENT_ID").alias("__EID"), *[F.col(k).alias(f"__{k}") for k in PRIMARY_KEY])
    sec = (tables["NWK_ACTION_SECONDARY_TBL"].join(prim_keys, F.col("EVENT_ID") == F.col("__EID"), "inner")
           .drop("__EID").withColumn("LOAD_ID", F.lit(load_id)))
    for k in PRIMARY_KEY:
        sec = sec.withColumnRenamed(f"__{k}", f"KEY_{k}")
    trk = (tables["EHRP_RECS_TRACKING_TBL"].filter(F.col("LOAD_DATE") == F.lit(load_date))
           .withColumn("LOAD_ID", F.lit(load_id)))
    return {"NWK_ACTION_PRIMARY_TBL": prim, "NWK_ACTION_SECONDARY_TBL": sec, "EHRP_RECS_TRACKING_TBL": trk}


def key_columns(table: str) -> list[str]:
    if table == "NWK_ACTION_PRIMARY_TBL":
        return ["LOAD_ID", *PRIMARY_KEY]
    if table == "NWK_ACTION_SECONDARY_TBL":
        return ["LOAD_ID", *[f"KEY_{k}" for k in PRIMARY_KEY]]
    return ["LOAD_ID", *TRACKING_KEY]


def compare_table(table: str, legacy: DataFrame, new: DataFrame) -> TableReport:
    keys = key_columns(table)
    cols = [c for c in legacy.columns if c not in keys and c in new.columns]
    dup_l = legacy.groupBy(*keys).count().filter("count > 1").count()
    dup_n = new.groupBy(*keys).count().filter("count > 1").count()

    # every column is renamed per side so self-comparisons (same lineage) are never ambiguous
    l = legacy.select(*[F.col(k).alias(f"lk__{k}") for k in keys], *[F.col(c).alias(f"l__{c}") for c in cols]).withColumn("__l", F.lit(1))
    n = new.select(*[F.col(k).alias(f"nk__{k}") for k in keys], *[F.col(c).alias(f"n__{c}") for c in cols]).withColumn("__n", F.lit(1))
    j = l.join(n, [F.col(f"lk__{k}").eqNullSafe(F.col(f"nk__{k}")) for k in keys], "full_outer").select(
        *[F.coalesce(F.col(f"lk__{k}"), F.col(f"nk__{k}")).alias(k) for k in keys], "__l", "__n",
        *[f"l__{c}" for c in cols], *[f"n__{c}" for c in cols])
    diff_flags = [(~F.col(f"l__{c}").eqNullSafe(F.col(f"n__{c}"))).cast("int").alias(c) for c in cols]
    both = j.filter("__l = 1 AND __n = 1").select(*keys, *diff_flags, *[f"l__{c}" for c in cols], *[f"n__{c}" for c in cols]).cache()

    sums = both.agg(*[F.sum(c).alias(c) for c in cols]).first().asDict() if cols else {}
    mismatches = {c: int(v) for c, v in sums.items() if v}
    any_diff = F.greatest(*[F.col(c) for c in cols]) if len(cols) > 1 else F.col(cols[0])
    bad_rows = both.filter(any_diff == 1)
    samples = []
    for c in sorted(mismatches):
        for r in both.filter(F.col(c) == 1).select(*keys, f"l__{c}", f"n__{c}").limit(SAMPLES).collect():
            samples.append({"key": {k: str(r[k]) for k in keys}, "column": c,
                            "legacy": repr(r[f"l__{c}"]), "new": repr(r[f"n__{c}"])})
    for side, flag in (("legacy", "__l"), ("new", "__n")):
        other = "__n" if flag == "__l" else "__l"
        for r in j.filter(f"{flag} = 1 AND {other} IS NULL").select(*keys).limit(SAMPLES).collect():
            samples.append({"key": {k: str(r[k]) for k in keys}, "column": None, "only_in": side})
    rep = TableReport(
        table=table, key=keys, legacy_rows=legacy.count(), new_rows=new.count(),
        matched_rows=both.count(), only_in_legacy=j.filter("__n IS NULL").count(),
        only_in_new=j.filter("__l IS NULL").count(), duplicate_keys_legacy=dup_l, duplicate_keys_new=dup_n,
        rows_with_mismatch=bad_rows.count(), column_mismatches=dict(sorted(mismatches.items())), samples=samples)
    both.unpersist()
    return rep


def reconcile(legacy: dict[str, DataFrame], new: dict[str, DataFrame], load_id: str) -> list[TableReport]:
    ls, ns = slice_for_load(legacy, load_id), slice_for_load(new, load_id)
    return [compare_table(t, ls[t], ns[t]) for t in TARGETS]


def render_markdown(load_id: str, reports: list[TableReport]) -> str:
    ok = all(r.clean for r in reports)
    out = [f"# EHRP2BIIS dual-run reconciliation -- {load_id}", "",
           f"**Result: {'MATCH' if ok else 'DIFFERENCES FOUND'}**", "",
           "| Table | Legacy rows | New rows | Matched | Only legacy | Only new | Dup keys (L/N) | Rows w/ mismatch |",
           "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for r in reports:
        out.append(f"| {r.table} | {r.legacy_rows} | {r.new_rows} | {r.matched_rows} | {r.only_in_legacy} | "
                   f"{r.only_in_new} | {r.duplicate_keys_legacy}/{r.duplicate_keys_new} | {r.rows_with_mismatch} |")
    for r in reports:
        out += ["", f"## {r.table}", "", f"Key: `{', '.join(r.key)}`", ""]
        if r.column_mismatches:
            out += ["| Column | Mismatched rows |", "|---|---:|"] + [f"| {c} | {n} |" for c, n in r.column_mismatches.items()]
        else:
            out.append("No column-level mismatches.")
        if r.samples:
            out += ["", "| Key | Column | Legacy | New |", "|---|---|---|---|"]
            for s in r.samples:
                k = ", ".join(s["key"].values())
                if s.get("column"):
                    out.append(f"| {k} | {s['column']} | `{s['legacy']}` | `{s['new']}` |")
                else:
                    out.append(f"| {k} | (row) | {'present' if s['only_in'] == 'legacy' else 'missing'} | "
                               f"{'present' if s['only_in'] == 'new' else 'missing'} |")
    return "\n".join(out) + "\n"


def write_report(load_id: str, reports: list[TableReport], report_dir: str) -> tuple[Path, Path]:
    d = Path(report_dir)
    d.mkdir(parents=True, exist_ok=True)
    md, js = d / f"reconcile_{load_id}.md", d / f"reconcile_{load_id}.json"
    md.write_text(render_markdown(load_id, reports))
    js.write_text(json.dumps({"load_id": load_id, "match": all(r.clean for r in reports),
                              "tables": [asdict(r) for r in reports]}, indent=2, default=str))
    return md, js


def _load_side(spark: SparkSession, spec: str, scope: str) -> dict[str, DataFrame]:
    if spec == "oracle":
        from pipelines.ehrp2biis.job import jdbc_options

        opts = jdbc_options(scope)
        return {t: spark.read.format("jdbc").options(**opts).option("dbtable", f"NKNIGHT.{t}").load() for t in TARGETS}
    from pipelines.ehrp2biis.writers import Store

    base = Path(spec)
    if base.is_dir() and not any(base.glob("*/_delta_log")) and any(base.glob("*/*.parquet")):
        return {t: spark.read.parquet(str(base / t.lower())) for t in TARGETS}  # plain parquet extract of legacy
    store = Store(spark, spec)
    return {t: store.read(t) for t in TARGETS}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run-date", required=True, type=dt.date.fromisoformat)
    p.add_argument("--legacy", required=True, help="'oracle' | Delta base (catalog.schema or path) | parquet dir")
    p.add_argument("--new", required=True, help="Delta base of the new pipeline's silver layer")
    p.add_argument("--secret-scope", default="ehrp2biis")
    p.add_argument("--report-dir", default="reconcile_reports")
    a = p.parse_args(sys.argv[1:] if argv is None else argv)

    from pipelines.ehrp2biis.job import get_spark
    from pipelines.ehrp2biis.transform import load_id_for

    spark = get_spark()
    load_id = load_id_for(a.run_date)
    reports = reconcile(_load_side(spark, a.legacy, a.secret_scope), _load_side(spark, a.new, a.secret_scope), load_id)
    md, js = write_report(load_id, reports, a.report_dir)
    print(render_markdown(load_id, reports))
    print(f"report: {md} {js}")
    return 0 if all(r.clean for r in reports) else 1


if __name__ == "__main__":
    sys.exit(main())
