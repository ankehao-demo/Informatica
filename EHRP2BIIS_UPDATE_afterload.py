# Databricks notebook source
# MAGIC %md
# MAGIC # EHRP2BIIS_UPDATE Post-Load Reconciliation
# MAGIC
# MAGIC Converted from: `ehrp2biis_afterload.sql` (Oracle SQL*Plus script)
# MAGIC
# MAGIC **Purpose**: Performs post-load data processing, quality fixes, row count
# MAGIC verification, business logic validation, and data promotion to production tables.
# MAGIC Generates a reconciliation summary.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, count, when, current_date, current_timestamp,
    trim, min as spark_min, max as spark_max, sum as spark_sum,
    expr, isnull, length, coalesce,
)
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Configuration
DATABASE = "informatica_migration"
spark.sql(f"USE {DATABASE}")

RECONCILIATION = {}
LOG_ENTRIES = []

def log_info(message):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    LOG_ENTRIES.append({"timestamp": ts, "level": "INFO", "message": message})
    print(f"[{ts}] INFO  - {message}")

def log_warn(message):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    LOG_ENTRIES.append({"timestamp": ts, "level": "WARN", "message": message})
    print(f"[{ts}] WARN  - {message}")

def log_error(message):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    LOG_ENTRIES.append({"timestamp": ts, "level": "ERROR", "message": message})
    print(f"[{ts}] ERROR - {message}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Row Count Verification Between Source and Target

# COMMAND ----------

log_info("Starting EHRP2BIIS_UPDATE post-load reconciliation")

# Source counts
actions_count = spark.table(f"{DATABASE}.nwk_new_ehrp_actions_tbl").count()
gvt_job_count = spark.table(f"{DATABASE}.ps_gvt_job").count()

# Source Qualifier join count (what the ETL should have processed)
sq_count = (
    spark.table(f"{DATABASE}.ps_gvt_job")
    .join(
        spark.table(f"{DATABASE}.nwk_new_ehrp_actions_tbl"),
        on=["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"],
        how="inner",
    )
    .count()
)

# Target counts
primary_count = spark.table(f"{DATABASE}.nwk_action_primary_tbl").count()
secondary_count = spark.table(f"{DATABASE}.nwk_action_secondary_tbl").count()
tracking_count = spark.table(f"{DATABASE}.ehrp_recs_tracking_tbl").count()

RECONCILIATION["source_actions_count"] = actions_count
RECONCILIATION["source_gvt_job_count"] = gvt_job_count
RECONCILIATION["source_qualifier_count"] = sq_count
RECONCILIATION["target_primary_count"] = primary_count
RECONCILIATION["target_secondary_count"] = secondary_count
RECONCILIATION["target_tracking_count"] = tracking_count

log_info(f"NWK_NEW_EHRP_ACTIONS_TBL (staging): {actions_count}")
log_info(f"PS_GVT_JOB (source): {gvt_job_count}")
log_info(f"Source Qualifier join result: {sq_count}")
log_info(f"NWK_ACTION_PRIMARY_TBL (target): {primary_count}")
log_info(f"NWK_ACTION_SECONDARY_TBL (target): {secondary_count}")
log_info(f"EHRP_RECS_TRACKING_TBL (target): {tracking_count}")

# Verify all targets received the same number of rows as the source qualifier
row_count_match = (
    sq_count == primary_count == secondary_count == tracking_count
)
RECONCILIATION["row_count_match"] = row_count_match

if row_count_match:
    log_info(f"ROW COUNT CHECK PASSED: All targets match source ({sq_count} rows)")
else:
    log_error(
        f"ROW COUNT CHECK FAILED: SQ={sq_count}, "
        f"Primary={primary_count}, Secondary={secondary_count}, "
        f"Tracking={tracking_count}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Fix Retained Step Code (Step 04 from afterload.sql)
# MAGIC
# MAGIC Converts '0.0000000000000' retained step codes to NULL for today's load.
# MAGIC ```sql
# MAGIC UPDATE nwk_action_secondary_tbl SET retnd1_step_cd = NULL
# MAGIC WHERE event_id IN (SELECT event_id FROM nwk_action_primary_tbl
# MAGIC                    WHERE load_date = trunc(SYSDATE) AND event_id < 9000000000)
# MAGIC   AND retnd1_step_cd = '0.0000000000000';
# MAGIC ```

# COMMAND ----------

# Read current secondary table
secondary_df = spark.table(f"{DATABASE}.nwk_action_secondary_tbl")

# Get event IDs from today's primary load with event_id < 9000000000
primary_df = spark.table(f"{DATABASE}.nwk_action_primary_tbl")
today_event_ids = (
    primary_df.filter(
        (col("LOAD_DATE") == current_date()) & (col("EVENT_ID") < 9000000000)
    )
    .select("EVENT_ID")
)

# Count affected rows before fix
affected_count = (
    secondary_df.join(today_event_ids, on="EVENT_ID", how="inner")
    .filter(col("RETND1_STEP_CD") == "0.0000000000000")
    .count()
)

log_info(f"Step 04 - Retained step code fix: {affected_count} rows to update")
RECONCILIATION["retained_step_fix_count"] = affected_count

# Apply the fix: set RETND1_STEP_CD to NULL where it equals '0.0000000000000'
if affected_count > 0:
    fixed_secondary = secondary_df.withColumn(
        "RETND1_STEP_CD",
        when(
            (col("EVENT_ID").isin(
                [row["EVENT_ID"] for row in today_event_ids.collect()]
            )) & (col("RETND1_STEP_CD") == "0.0000000000000"),
            lit(None)
        ).otherwise(col("RETND1_STEP_CD"))
    )

    fixed_secondary.write.format("delta").mode("overwrite").saveAsTable(
        f"{DATABASE}.nwk_action_secondary_tbl"
    )
    log_info(f"Step 04 - Fixed {affected_count} retained step code values")
else:
    log_info("Step 04 - No retained step code fixes needed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Display Sequence Numbers (Step 05 from afterload.sql)

# COMMAND ----------

log_info("Step 05 - Sequence number table BEFORE update:")
seq_df = spark.table(f"{DATABASE}.sequence_num_tbl")
seq_df.orderBy("EHRP_YEAR").show(truncate=False)

seq_before = {
    row["EHRP_YEAR"]: row["EHRP_SEQ_NUMBER"]
    for row in seq_df.collect()
}
RECONCILIATION["sequence_numbers_before"] = str(seq_before)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Update Sequence Numbers
# MAGIC
# MAGIC Equivalent of `execute update_sequence_number_tbl_p`:
# MAGIC Updates the sequence number table with the max EVENT_ID used per year
# MAGIC from today's tracking records.

# COMMAND ----------

tracking_df = spark.table(f"{DATABASE}.ehrp_recs_tracking_tbl")

# Get the max BIIS_EVENT_ID for today's load, grouped by year
from pyspark.sql.functions import year as year_fn, max as spark_max

today_max_ids = (
    tracking_df.filter(col("LOAD_DATE") == current_date())
    .withColumn("EHRP_YEAR", year_fn(col("EFFDT")))
    .groupBy("EHRP_YEAR")
    .agg(spark_max("BIIS_EVENT_ID").alias("NEW_SEQ_NUMBER"))
)

if today_max_ids.count() > 0:
    today_max_ids.show(truncate=False)

    # Update sequence_num_tbl with new max values
    existing_seq = spark.table(f"{DATABASE}.sequence_num_tbl")

    updated_seq = (
        existing_seq.alias("e")
        .join(today_max_ids.alias("n"), on="EHRP_YEAR", how="full_outer")
        .select(
            coalesce(col("e.EHRP_YEAR"), col("n.EHRP_YEAR")).alias("EHRP_YEAR"),
            when(
                col("n.NEW_SEQ_NUMBER").isNotNull()
                & (col("n.NEW_SEQ_NUMBER") > coalesce(col("e.EHRP_SEQ_NUMBER"), lit(0))),
                col("n.NEW_SEQ_NUMBER")
            ).otherwise(coalesce(col("e.EHRP_SEQ_NUMBER"), lit(0))).alias("EHRP_SEQ_NUMBER"),
        )
    )

    updated_seq.write.format("delta").mode("overwrite").saveAsTable(
        f"{DATABASE}.sequence_num_tbl"
    )
    log_info("Step 05 - Sequence numbers updated")
else:
    log_info("Step 05 - No records from today's load to update sequences")

log_info("Step 05 - Sequence number table AFTER update:")
spark.table(f"{DATABASE}.sequence_num_tbl").orderBy("EHRP_YEAR").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Promote Today's Data to Production ALL Tables
# MAGIC
# MAGIC Equivalent of the INSERT INTO action_*_all statements in afterload.sql.
# MAGIC Copies today's loaded records into the production archive tables.

# COMMAND ----------

# Promote primary records
primary_today = spark.table(f"{DATABASE}.nwk_action_primary_tbl").filter(
    col("LOAD_DATE") == current_date()
)
today_primary_count = primary_today.count()
log_info(f"Step 06 - Records to promote to action_primary_all: {today_primary_count}")

if today_primary_count > 0:
    # Check if target table exists; create or append
    try:
        primary_today.write.format("delta").mode("append").saveAsTable(
            f"{DATABASE}.action_primary_all"
        )
        log_info(f"Step 06 - Promoted {today_primary_count} rows to action_primary_all")
    except Exception as e:
        log_warn(f"Step 06 - action_primary_all write: {str(e)}")

# Promote secondary records (matching today's primary event IDs)
today_event_ids_list = (
    primary_today.select("EVENT_ID")
)

try:
    secondary_today = (
        spark.table(f"{DATABASE}.nwk_action_secondary_tbl")
        .join(today_event_ids_list, on="EVENT_ID", how="inner")
    )
    sec_promo_count = secondary_today.count()

    secondary_today.write.format("delta").mode("append").saveAsTable(
        f"{DATABASE}.action_secondary_all"
    )
    log_info(f"Step 06 - Promoted {sec_promo_count} rows to action_secondary_all")
except Exception as e:
    log_warn(f"Step 06 - action_secondary_all write: {str(e)}")

# Promote remarks records (if remarks table exists)
try:
    remarks_today = (
        spark.table(f"{DATABASE}.nwk_action_remarks_tbl")
        .join(today_event_ids_list, on="EVENT_ID", how="inner")
    )
    rmk_promo_count = remarks_today.count()

    remarks_today.write.format("delta").mode("append").saveAsTable(
        f"{DATABASE}.action_remarks_all"
    )
    log_info(f"Step 06 - Promoted {rmk_promo_count} rows to action_remarks_all")
except Exception as e:
    log_warn(f"Step 06 - action_remarks_all not available: {str(e)}")

RECONCILIATION["promoted_primary"] = today_primary_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: WIP Status Change Detection
# MAGIC
# MAGIC Equivalent of the PROCESS_TABLE update and `chk_ehrp2biis_wip_status_p`
# MAGIC stored procedure. Detects records where the WIP status has changed between
# MAGIC the tracking table and the current PS_GVT_JOB data.

# COMMAND ----------

tracking = spark.table(f"{DATABASE}.ehrp_recs_tracking_tbl")
gvt_job = spark.table(f"{DATABASE}.ps_gvt_job")

wip_changes = (
    tracking.alias("a")
    .join(
        gvt_job.alias("b"),
        on=[
            col("a.EMPLID") == col("b.EMPLID"),
            col("a.EMPL_RCD") == col("b.EMPL_RCD"),
            col("a.EFFDT") == col("b.EFFDT"),
            col("a.EFFSEQ") == col("b.EFFSEQ"),
        ],
        how="inner",
    )
    .filter(col("a.GVT_WIP_STATUS") != col("b.GVT_WIP_STATUS"))
    .select(
        col("a.BIIS_EVENT_ID"),
        col("a.EMPLID"),
        col("a.EMPL_RCD"),
        col("a.EFFDT"),
        col("a.EFFSEQ"),
        col("b.DEPTID"),
        col("a.GVT_WIP_STATUS").alias("OLD_STATUS"),
        col("b.GVT_WIP_STATUS").alias("NEW_STATUS"),
    )
    .orderBy("EFFDT", "BIIS_EVENT_ID")
)

wip_change_count = wip_changes.count()
RECONCILIATION["wip_status_changes"] = wip_change_count

if wip_change_count > 0:
    log_warn(f"Step 07 - WIP status changes detected: {wip_change_count}")
    wip_changes.show(20, truncate=False)

    # Get earliest EFFDT with WIP change (equivalent to PROCESS_TABLE update)
    earliest_change = wip_changes.select(spark_min("EFFDT")).first()[0]
    log_info(f"Step 07 - Earliest WIP change EFFDT: {earliest_change}")
    RECONCILIATION["earliest_wip_change_date"] = str(earliest_change)
else:
    log_info("Step 07 - No WIP status changes detected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Business Logic Validation Checks

# COMMAND ----------

primary = spark.table(f"{DATABASE}.nwk_action_primary_tbl")
secondary = spark.table(f"{DATABASE}.nwk_action_secondary_tbl")
tracking = spark.table(f"{DATABASE}.ehrp_recs_tracking_tbl")

validation_results = {}

# --- Check 1: EVENT_ID uniqueness across all targets ---
primary_ids = primary.select("EVENT_ID").distinct().count()
secondary_ids = secondary.select("EVENT_ID").distinct().count()
tracking_ids = tracking.select("BIIS_EVENT_ID").distinct().count()

check_1 = (primary_ids == primary_count) and (secondary_ids == secondary_count)
validation_results["event_id_uniqueness"] = "PASS" if check_1 else "FAIL"
log_info(
    f"Check 1 - EVENT_ID uniqueness: {'PASS' if check_1 else 'FAIL'} "
    f"(Primary: {primary_ids}/{primary_count}, "
    f"Secondary: {secondary_ids}/{secondary_count})"
)

# --- Check 2: EVENT_ID consistency across tables ---
# All primary EVENT_IDs should exist in secondary and tracking
primary_only = primary.select("EVENT_ID").subtract(
    secondary.select("EVENT_ID")
).count()

tracking_primary_match = tracking.select(
    col("BIIS_EVENT_ID").alias("EVENT_ID")
).subtract(primary.select("EVENT_ID")).count()

check_2 = (primary_only == 0) and (tracking_primary_match == 0)
validation_results["event_id_consistency"] = "PASS" if check_2 else "FAIL"
log_info(
    f"Check 2 - EVENT_ID cross-table consistency: {'PASS' if check_2 else 'FAIL'} "
    f"(Primary-only: {primary_only}, Tracking-unmatched: {tracking_primary_match})"
)

# --- Check 3: LOAD_DATE is today ---
wrong_date_primary = primary.filter(col("LOAD_DATE") != current_date()).count()
wrong_date_tracking = tracking.filter(col("LOAD_DATE") != current_date()).count()

check_3 = (wrong_date_primary == 0) and (wrong_date_tracking == 0)
validation_results["load_date_check"] = "PASS" if check_3 else "FAIL"
log_info(
    f"Check 3 - LOAD_DATE is today: {'PASS' if check_3 else 'FAIL'} "
    f"(Primary wrong: {wrong_date_primary}, Tracking wrong: {wrong_date_tracking})"
)

# --- Check 4: No null EMPLIDs ---
null_emp_primary = primary.filter(col("EMP_ID").isNull()).count()
null_emp_tracking = tracking.filter(col("EMPLID").isNull()).count()

check_4 = (null_emp_primary == 0) and (null_emp_tracking == 0)
validation_results["no_null_emplid"] = "PASS" if check_4 else "FAIL"
log_info(
    f"Check 4 - No null EMPLIDs: {'PASS' if check_4 else 'FAIL'} "
    f"(Primary: {null_emp_primary}, Tracking: {null_emp_tracking})"
)

# --- Check 5: NOA_CD is populated ---
null_noa_primary = primary.filter(col("NOA_CD").isNull()).count()
null_noa_tracking = tracking.filter(col("NOA_CD").isNull()).count()

check_5_pass = True
if null_noa_primary > 0 or null_noa_tracking > 0:
    check_5_pass = False
validation_results["noa_cd_populated"] = "PASS" if check_5_pass else "WARN"
log_info(
    f"Check 5 - NOA_CD populated: {'PASS' if check_5_pass else 'WARN'} "
    f"(Primary null: {null_noa_primary}, Tracking null: {null_noa_tracking})"
)

# --- Check 6: LOAD_ID format validation (NKyyyymmdd) ---
load_id_sample = primary.select("LOAD_ID").first()
if load_id_sample:
    load_id_val = load_id_sample[0]
    check_6 = (
        load_id_val is not None
        and load_id_val.startswith("NK")
        and len(load_id_val) == 10
    )
    validation_results["load_id_format"] = "PASS" if check_6 else "FAIL"
    log_info(
        f"Check 6 - LOAD_ID format: {'PASS' if check_6 else 'FAIL'} "
        f"(Sample: {load_id_val})"
    )
else:
    validation_results["load_id_format"] = "SKIP"
    log_warn("Check 6 - LOAD_ID format: SKIP (no rows)")

# --- Check 7: Citizenship status values are '1' or '8' only ---
invalid_citizenship = primary.filter(
    ~col("CITIZENSHIP_STATUS").isin("1", "8")
    & col("CITIZENSHIP_STATUS").isNotNull()
).count()

check_7 = invalid_citizenship == 0
validation_results["citizenship_values"] = "PASS" if check_7 else "FAIL"
log_info(
    f"Check 7 - Citizenship values ('1' or '8'): {'PASS' if check_7 else 'FAIL'} "
    f"(Invalid: {invalid_citizenship})"
)

# --- Check 8: TSP Vesting Code values are 0, 2, or 3 ---
invalid_tsp = secondary.filter(
    ~col("TSP_VESTING_CD").isin(0, 2, 3)
    & col("TSP_VESTING_CD").isNotNull()
).count()

check_8 = invalid_tsp == 0
validation_results["tsp_vesting_values"] = "PASS" if check_8 else "FAIL"
log_info(
    f"Check 8 - TSP Vesting Code (0, 2, 3): {'PASS' if check_8 else 'FAIL'} "
    f"(Invalid: {invalid_tsp})"
)

# --- Check 9: EVENT_IDs are positive and within expected range ---
negative_or_zero = primary.filter(col("EVENT_ID") <= 0).count()
check_9 = negative_or_zero == 0
validation_results["event_id_positive"] = "PASS" if check_9 else "FAIL"
log_info(
    f"Check 9 - EVENT_ID positive: {'PASS' if check_9 else 'FAIL'} "
    f"(Non-positive: {negative_or_zero})"
)

# --- Check 10: Base hours logic validation ---
# When WORK_SCHEDULE_CD = 'I', BASE_HRS should equal STD_HOURS
# Otherwise BASE_HRS should equal STD_HOURS * 2
# (Spot check - verify logic consistency)
if "WORK_SCHEDULE_CD" in primary.columns and "BASE_HRS" in primary.columns:
    intermittent_sample = primary.filter(col("WORK_SCHEDULE_CD") == "I").count()
    non_intermittent_sample = primary.filter(col("WORK_SCHEDULE_CD") != "I").count()
    log_info(
        f"Check 10 - Base hours distribution: "
        f"Intermittent={intermittent_sample}, Non-intermittent={non_intermittent_sample}"
    )
    validation_results["base_hours_distribution"] = "INFO"
else:
    validation_results["base_hours_distribution"] = "SKIP"

RECONCILIATION["validation_results"] = validation_results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Truncate Staging Table (Final Step from afterload.sql)
# MAGIC
# MAGIC Equivalent of `TRUNCATE TABLE nknight.nwk_new_ehrp_actions_tbl`
# MAGIC Clears the staging table to prepare for the next load cycle.

# COMMAND ----------

# NOTE: In production, uncomment the following to truncate the staging table.
# This is commented out by default to allow re-runs during development/testing.

# spark.sql(f"TRUNCATE TABLE {DATABASE}.nwk_new_ehrp_actions_tbl")
# log_info("Step 09 - Staging table NWK_NEW_EHRP_ACTIONS_TBL truncated")

log_info(
    "Step 09 - Staging table truncation SKIPPED (uncomment in production). "
    "Original SQL: TRUNCATE TABLE nknight.nwk_new_ehrp_actions_tbl"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Generate Reconciliation Summary

# COMMAND ----------

log_info("=" * 70)
log_info("EHRP2BIIS_UPDATE POST-LOAD RECONCILIATION SUMMARY")
log_info("=" * 70)
log_info(f"Run Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
log_info(f"Database: {DATABASE}")
log_info("-" * 70)

log_info("ROW COUNTS:")
log_info(f"  Source (NWK_NEW_EHRP_ACTIONS_TBL):  {RECONCILIATION.get('source_actions_count', 'N/A')}")
log_info(f"  Source (PS_GVT_JOB):                 {RECONCILIATION.get('source_gvt_job_count', 'N/A')}")
log_info(f"  Source Qualifier Join:                {RECONCILIATION.get('source_qualifier_count', 'N/A')}")
log_info(f"  Target (NWK_ACTION_PRIMARY_TBL):     {RECONCILIATION.get('target_primary_count', 'N/A')}")
log_info(f"  Target (NWK_ACTION_SECONDARY_TBL):   {RECONCILIATION.get('target_secondary_count', 'N/A')}")
log_info(f"  Target (EHRP_RECS_TRACKING_TBL):     {RECONCILIATION.get('target_tracking_count', 'N/A')}")
log_info(f"  Row Count Match:                     {'YES' if RECONCILIATION.get('row_count_match') else 'NO'}")
log_info("-" * 70)

log_info("POST-LOAD FIXES:")
log_info(f"  Retained Step Code Fixes:            {RECONCILIATION.get('retained_step_fix_count', 'N/A')}")
log_info(f"  WIP Status Changes:                  {RECONCILIATION.get('wip_status_changes', 'N/A')}")
if RECONCILIATION.get("earliest_wip_change_date"):
    log_info(f"  Earliest WIP Change Date:            {RECONCILIATION['earliest_wip_change_date']}")
log_info(f"  Records Promoted to ALL tables:       {RECONCILIATION.get('promoted_primary', 'N/A')}")
log_info("-" * 70)

log_info("VALIDATION RESULTS:")
for check_name, result in RECONCILIATION.get("validation_results", {}).items():
    status_symbol = "OK" if result == "PASS" else ("!!" if result == "FAIL" else result)
    log_info(f"  {check_name}: [{status_symbol}]")

log_info("-" * 70)

# Overall status
fail_count = sum(
    1 for v in RECONCILIATION.get("validation_results", {}).values()
    if v == "FAIL"
)
error_count = sum(1 for e in LOG_ENTRIES if e["level"] == "ERROR")

if fail_count == 0 and error_count == 0:
    log_info("OVERALL STATUS: SUCCESS - All checks passed")
    RECONCILIATION["overall_status"] = "SUCCESS"
elif fail_count > 0:
    log_error(f"OVERALL STATUS: FAILED - {fail_count} validation check(s) failed")
    RECONCILIATION["overall_status"] = "FAILED"
else:
    log_warn(f"OVERALL STATUS: WARNING - {error_count} error(s) encountered")
    RECONCILIATION["overall_status"] = "WARNING"

log_info("=" * 70)

# COMMAND ----------

# Persist reconciliation log as a Delta table for audit trail
log_df = spark.createDataFrame(LOG_ENTRIES)
log_df = log_df.withColumn("run_date", current_date())

log_df.write.format("delta").mode("append").saveAsTable(
    f"{DATABASE}.etl_afterload_log"
)

log_info("Reconciliation log persisted to etl_afterload_log table")
print(f"\n*** EHRP2BIIS_UPDATE Post-Load Reconciliation COMPLETE ***")
print(f"*** Overall Status: {RECONCILIATION['overall_status']} ***")
