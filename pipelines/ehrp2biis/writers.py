"""Idempotent Delta writers.

PowerCenter inserts every row ("Treat source rows as" = Insert), so a legacy rerun on
the same day duplicates rows. The port keeps the *result* of a single successful run
but makes reruns safe: each target is MERGEd on a deterministic key and rows of the
same load slice that are no longer produced are deleted.

    NWK_ACTION_PRIMARY_TBL    key (LOAD_ID, EVENT_ID)          slice LOAD_ID = :load_id
    NWK_ACTION_SECONDARY_TBL  key (EVENT_ID)                   slice EVENT_IDs of the primary slice
    EHRP_RECS_TRACKING_TBL    key (LOAD_DATE, BIIS_EVENT_ID)   slice LOAD_DATE = :load_date
"""
from __future__ import annotations

import datetime as dt

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

MERGE_KEYS = {
    "NWK_ACTION_PRIMARY_TBL": ("LOAD_ID", "EVENT_ID"),
    "NWK_ACTION_SECONDARY_TBL": ("EVENT_ID",),
    "EHRP_RECS_TRACKING_TBL": ("LOAD_DATE", "BIIS_EVENT_ID"),
}


class Store:
    """Resolves a logical table name to a Unity Catalog table or a filesystem path.

    ``base`` like ``main.ehrp2biis_silver`` -> catalog tables; ``/tmp/x`` or
    ``dbfs:/...`` -> path-based Delta tables (local tests).
    """

    def __init__(self, spark: SparkSession, base: str):
        self.spark, self.base = spark, base.rstrip("/")
        self.path_mode = "/" in base or ":" in base

    def ref(self, table: str) -> str:
        return f"{self.base}/{table.lower()}" if self.path_mode else f"{self.base}.{table.lower()}"

    def exists(self, table: str) -> bool:
        if self.path_mode:
            return DeltaTable.isDeltaTable(self.spark, self.ref(table))
        return self.spark.catalog.tableExists(self.ref(table))

    def delta(self, table: str) -> DeltaTable:
        return DeltaTable.forPath(self.spark, self.ref(table)) if self.path_mode else DeltaTable.forName(self.spark, self.ref(table))

    def read(self, table: str) -> DataFrame:
        return self.spark.read.format("delta").load(self.ref(table)) if self.path_mode else self.spark.table(self.ref(table))

    def create(self, table: str, df: DataFrame) -> None:
        w = df.write.format("delta").mode("overwrite")
        w.save(self.ref(table)) if self.path_mode else w.saveAsTable(self.ref(table))

    def replace_slice(self, table: str, df: DataFrame, predicate: str) -> None:
        """Overwrite only the rows matching ``predicate`` (Delta replaceWhere)."""
        if not self.exists(table):
            return self.create(table, df)
        w = df.write.format("delta").mode("overwrite").option("replaceWhere", predicate)
        w.save(self.ref(table)) if self.path_mode else w.saveAsTable(self.ref(table))


def _merge(store: Store, table: str, df: DataFrame, slice_condition: str | None) -> None:
    if not store.exists(table):
        store.create(table, df)
        return
    keys = MERGE_KEYS[table]
    cond = " AND ".join(f"t.{k} <=> s.{k}" for k in keys)
    m = store.delta(table).alias("t").merge(df.alias("s"), cond).whenMatchedUpdateAll().whenNotMatchedInsertAll()
    if slice_condition:
        m = m.whenNotMatchedBySourceDelete(condition=slice_condition)
    m.execute()


def merge_targets(store: Store, targets: dict[str, DataFrame], load_id: str, load_date: dt.date) -> dict[str, int]:
    """MERGE the three targets for one load. Safe to run any number of times."""
    primary = "NWK_ACTION_PRIMARY_TBL"
    new_ids = targets[primary].select("EVENT_ID")
    # EVENT_IDs that belonged to this LOAD_ID before the rerun but are not produced now.
    stale_secondary = None
    if store.exists(primary):
        stale_secondary = (
            store.read(primary).filter(F.col("LOAD_ID") == load_id).select("EVENT_ID").distinct()
            .join(new_ids, "EVENT_ID", "left_anti").localCheckpoint()
        )

    _merge(store, "NWK_ACTION_SECONDARY_TBL", targets["NWK_ACTION_SECONDARY_TBL"], None)
    if stale_secondary is not None and store.exists("NWK_ACTION_SECONDARY_TBL"):
        (store.delta("NWK_ACTION_SECONDARY_TBL").alias("t")
         .merge(stale_secondary.alias("s"), "t.EVENT_ID = s.EVENT_ID").whenMatchedDelete().execute())
    _merge(store, primary, targets[primary], f"t.LOAD_ID = '{load_id}'")
    _merge(store, "EHRP_RECS_TRACKING_TBL", targets["EHRP_RECS_TRACKING_TBL"],
           f"t.LOAD_DATE = TIMESTAMP '{load_date.isoformat()} 00:00:00'")
    return {t: df.count() for t, df in targets.items()}
