"""Databricks entry point for the EHRP2BIIS pilot.

    python -m pipelines.ehrp2biis.job <step> --run-date 2026-03-12 [options]

Steps (one Databricks Workflows task each, see main.tf):
    extract    Oracle (JDBC) -> bronze Delta, one immutable snapshot per LOAD_ID
    transform  bronze -> m_EHRP2BIIS_UPDATE -> silver Delta (MERGE), rejects, lookup stats
    publish    silver slice for LOAD_ID -> Oracle NKNIGHT.STG_* staging tables
               (Airflow then swaps the slice into NKNIGHT.NWK_* and runs the afterload)
    sample     load the synthetic batch into bronze (local demo / CI only)

Every step is keyed by LOAD_ID = 'NK' || yyyymmdd(run_date) and is safe to rerun.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .sample_data import build_tables, table_schemas
from .transform import Ehrp2BiisTransform, load_id_for
from .writers import Store, merge_targets

OWNERS = {"NWK_NEW_EHRP_ACTIONS_TBL": "NKNIGHT", "SEQUENCE_NUM_TBL": "NKNIGHT"}  # everything else: EHRP
BATCH_KEYS = "SELECT EMPLID, EMPL_RCD, EFFDT, EFFSEQ FROM NKNIGHT.NWK_NEW_EHRP_ACTIONS_TBL"
SEQ_SNAPSHOT = "EHRP2BIIS_SEQ_SNAPSHOT"
REJECTS = "EHRP2BIIS_REJECTS"
LOOKUP_STATS = "EHRP2BIIS_LOOKUP_MULTIMATCH"


def get_secret(scope: str, key: str) -> str:
    """Databricks secret (scope/key); falls back to env var SCOPE__KEY for local runs."""
    try:
        from pyspark.dbutils import DBUtils  # type: ignore

        return DBUtils(SparkSession.getActiveSession()).secrets.get(scope=scope, key=key)
    except Exception:  # noqa: BLE001 - not on Databricks
        env = f"{scope}__{key}".upper().replace("-", "_")
        if env not in os.environ:
            raise RuntimeError(f"secret {scope}/{key} not available (set {env} locally)")
        return os.environ[env]


def jdbc_options(scope: str) -> dict:
    return {
        "url": get_secret(scope, "oracle-jdbc-url"),
        "user": get_secret(scope, "oracle-user"),
        "password": get_secret(scope, "oracle-password"),
        "driver": "oracle.jdbc.OracleDriver",
        "fetchsize": "10000",
    }


def extract_query(table: str, columns: list[str]) -> str:
    owner = OWNERS.get(table, "EHRP")
    cols = ", ".join(f"t.{c}" for c in columns)
    base = f"SELECT {cols} FROM {owner}.{table} t"
    if table in ("NWK_NEW_EHRP_ACTIONS_TBL", "SEQUENCE_NUM_TBL"):
        return base
    if table == "PS_JPM_JP_ITEMS":
        return base + " WHERE t.JPM_PROFILE_ID IN (SELECT EMPLID FROM NKNIGHT.NWK_NEW_EHRP_ACTIONS_TBL)"
    # Only rows that can satisfy the 4-key lookup condition for today's batch.
    return base + f" WHERE (t.EMPLID, t.EMPL_RCD, t.EFFDT, t.EFFSEQ) IN ({BATCH_KEYS})"


def _with_load_id(df: DataFrame, load_id: str) -> DataFrame:
    return df.withColumn("LOAD_ID", F.lit(load_id))


def step_extract(spark: SparkSession, a) -> dict:
    store, load_id, opts = Store(spark, a.bronze), load_id_for(a.run_date), jdbc_options(a.secret_scope)
    counts = {}
    for table, schema in table_schemas().items():
        df = spark.read.format("jdbc").options(**opts).option("query", extract_query(table, schema.fieldNames())).load()
        df = df.select(*[F.col(f.name).cast(f.dataType) for f in schema.fields])
        store.replace_slice(table, _with_load_id(df, load_id), f"LOAD_ID = '{load_id}'")
        counts[table] = df.count()
    return counts


def step_sample(spark: SparkSession, a) -> dict:
    store, load_id = Store(spark, a.bronze), load_id_for(a.run_date)
    counts = {}
    for table, df in build_tables(spark).items():
        store.replace_slice(table, _with_load_id(df, load_id), f"LOAD_ID = '{load_id}'")
        counts[table] = df.count()
    return counts


def read_bronze(spark: SparkSession, bronze: str, load_id: str) -> dict[str, DataFrame]:
    store = Store(spark, bronze)
    return {t: store.read(t).filter(F.col("LOAD_ID") == load_id).drop("LOAD_ID") for t in table_schemas()}


def pinned_sequence(store: Store, tables: dict[str, DataFrame], load_id: str) -> DataFrame:
    """Freeze SEQUENCE_NUM_TBL per LOAD_ID so EVENT_IDs are identical on every rerun,
    even after the afterload's update_sequence_number_tbl_p has advanced the table."""
    if store.exists(SEQ_SNAPSHOT):
        snap = store.read(SEQ_SNAPSHOT).filter(F.col("LOAD_ID") == load_id)
        if snap.limit(1).count():
            return snap.drop("LOAD_ID")
    seq = tables["SEQUENCE_NUM_TBL"]
    store.replace_slice(SEQ_SNAPSHOT, _with_load_id(seq, load_id), f"LOAD_ID = '{load_id}'")
    return seq


def step_transform(spark: SparkSession, a) -> dict:
    load_id = load_id_for(a.run_date)
    silver = Store(spark, a.silver)
    tables = read_bronze(spark, a.bronze, load_id)
    tables["SEQUENCE_NUM_TBL"] = pinned_sequence(silver, tables, load_id)

    result = Ehrp2BiisTransform(spark).run(tables, a.run_date)
    counts = merge_targets(silver, result.targets, load_id, a.run_date)

    rejects = _with_load_id(result.rejects, load_id)
    silver.replace_slice(REJECTS, rejects, f"LOAD_ID = '{load_id}'")
    stats = None
    for s in result.lookup_stats:
        f = s["frame"].select(F.lit(s["lookup"]).alias("LOOKUP"), F.to_json(F.struct(*[c for c in s["frame"].columns if c.startswith("__k")])).alias("KEY"), F.col("__cnt").alias("MATCHES"))
        stats = f if stats is None else stats.unionByName(f)
    silver.replace_slice(LOOKUP_STATS, _with_load_id(stats, load_id), f"LOAD_ID = '{load_id}'")
    counts["rejects"] = rejects.count()
    counts["lookup_multi_match_keys"] = stats.count()
    if a.max_rejects is not None and counts["rejects"] > a.max_rejects:
        raise SystemExit(f"{counts['rejects']} rejected rows > --max-rejects {a.max_rejects}")
    return counts


STAGING = {
    "NWK_ACTION_PRIMARY_TBL": "NKNIGHT.STG_NWK_ACTION_PRIMARY_TBL",
    "NWK_ACTION_SECONDARY_TBL": "NKNIGHT.STG_NWK_ACTION_SECONDARY_TBL",
    "EHRP_RECS_TRACKING_TBL": "NKNIGHT.STG_EHRP_RECS_TRACKING_TBL",
}


def silver_slice(silver: Store, load_id: str, load_date: dt.date) -> dict[str, DataFrame]:
    prim = silver.read("NWK_ACTION_PRIMARY_TBL").filter(F.col("LOAD_ID") == load_id)
    sec = silver.read("NWK_ACTION_SECONDARY_TBL").join(prim.select("EVENT_ID"), "EVENT_ID", "left_semi")
    trk = silver.read("EHRP_RECS_TRACKING_TBL").filter(F.col("LOAD_DATE") == F.lit(f"{load_date} 00:00:00").cast("timestamp"))
    return {"NWK_ACTION_PRIMARY_TBL": prim, "NWK_ACTION_SECONDARY_TBL": sec, "EHRP_RECS_TRACKING_TBL": trk}


def step_publish(spark: SparkSession, a) -> dict:
    """Overwrite (truncate) the Oracle staging tables with this LOAD_ID's slice."""
    load_id, opts = load_id_for(a.run_date), jdbc_options(a.secret_scope)
    counts = {}
    for target, df in silver_slice(Store(spark, a.silver), load_id, a.run_date).items():
        (df.write.format("jdbc").options(**opts).option("dbtable", STAGING[target])
         .option("truncate", "true").option("batchsize", "10000").mode("overwrite").save())
        counts[target] = df.count()
    return counts


STEPS = {"extract": step_extract, "sample": step_sample, "transform": step_transform, "publish": step_publish}


def parse_args(argv: list[str]):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("step", choices=sorted(STEPS))
    p.add_argument("--run-date", required=True, type=dt.date.fromisoformat, help="logical load date (Airflow ds)")
    p.add_argument("--bronze", default=os.environ.get("EHRP2BIIS_BRONZE", "main.ehrp2biis_bronze"))
    p.add_argument("--silver", default=os.environ.get("EHRP2BIIS_SILVER", "main.ehrp2biis_silver"))
    p.add_argument("--secret-scope", default=os.environ.get("EHRP2BIIS_SECRET_SCOPE", "ehrp2biis"))
    p.add_argument("--max-rejects", type=int, default=None, help="fail the task above this many rejected rows")
    return p.parse_args(argv)


def get_spark() -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    from delta import configure_spark_with_delta_pip

    b = (SparkSession.builder.master("local[2]").appName("ehrp2biis")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false")
         .config("spark.ui.showConsoleProgress", "false")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g")))
    if os.environ.get("SPARK_JARS_REPOSITORIES"):
        b = b.config("spark.jars.repositories", os.environ["SPARK_JARS_REPOSITORIES"])
    spark = configure_spark_with_delta_pip(b).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main(argv: list[str] | None = None) -> int:
    a = parse_args(sys.argv[1:] if argv is None else argv)
    spark = get_spark()
    out = STEPS[a.step](spark, a)
    print(json.dumps({"step": a.step, "load_id": load_id_for(a.run_date), "counts": out}, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
