# Databricks notebook source
# MAGIC %md
# MAGIC # EHRP2BIIS_UPDATE Pre-Load Validation
# MAGIC
# MAGIC Converted from: `ehrp2biis_preload` (ksh shell script)
# MAGIC
# MAGIC **Purpose**: Validates source table availability and row counts before the main ETL
# MAGIC pipeline runs. Checks that the staging/driver table has records to process and
# MAGIC logs pre-load statistics.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit, current_timestamp, current_date
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Configuration
DATABASE = "informatica_migration"
LOG_ENTRIES = []

def log_info(message):
    """Append a log entry with timestamp."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    LOG_ENTRIES.append({"timestamp": ts, "level": "INFO", "message": message})
    print(f"[{ts}] INFO  - {message}")

def log_error(message):
    """Append an error log entry with timestamp."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    LOG_ENTRIES.append({"timestamp": ts, "level": "ERROR", "message": message})
    print(f"[{ts}] ERROR - {message}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Verify Database Exists

# COMMAND ----------

log_info(f"Starting EHRP2BIIS_UPDATE pre-load validation")
log_info(f"Target database: {DATABASE}")

try:
    spark.sql(f"USE {DATABASE}")
    log_info(f"Database '{DATABASE}' is accessible")
except Exception as e:
    log_error(f"Database '{DATABASE}' is not accessible: {str(e)}")
    raise RuntimeError(f"Pre-load FAILED: Database '{DATABASE}' not found") from e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Validate Source Tables Exist and Are Accessible

# COMMAND ----------

# All source and lookup tables required by the ETL pipeline
REQUIRED_TABLES = [
    # Source tables
    "nwk_new_ehrp_actions_tbl",   # Staging/driver table
    "ps_gvt_job",                  # Main PeopleSoft Government Job table
    # Lookup tables
    "ps_gvt_employment",
    "ps_gvt_pers_nid",
    "ps_gvt_awd_data",
    "ps_gvt_ee_data_trk",
    "ps_he_fill_pos",
    "ps_gvt_citizenship",
    "ps_gvt_pers_data",
    "ps_jpm_jp_items",
    "sequence_num_tbl",
    # Target tables
    "nwk_action_primary_tbl",
    "nwk_action_secondary_tbl",
    "ehrp_recs_tracking_tbl",
]

tables_in_db = [
    row.tableName
    for row in spark.sql(f"SHOW TABLES IN {DATABASE}").collect()
]

missing_tables = []
for table in REQUIRED_TABLES:
    if table in tables_in_db:
        log_info(f"Table '{DATABASE}.{table}' exists")
    else:
        log_error(f"Table '{DATABASE}.{table}' is MISSING")
        missing_tables.append(table)

if missing_tables:
    raise RuntimeError(
        f"Pre-load FAILED: Missing tables: {', '.join(missing_tables)}"
    )

log_info("All required tables are present")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Validate Staging/Driver Table Has Records

# COMMAND ----------

# The NWK_NEW_EHRP_ACTIONS_TBL is the staging/driver table that identifies
# which employee records need to be processed in the current run.
# This is the equivalent of the Oracle step01 pre-load check.

staging_df = spark.table(f"{DATABASE}.nwk_new_ehrp_actions_tbl")
staging_count = staging_df.count()

log_info(f"NWK_NEW_EHRP_ACTIONS_TBL row count: {staging_count}")

if staging_count == 0:
    log_error("Staging table NWK_NEW_EHRP_ACTIONS_TBL has 0 records - nothing to process")
    raise RuntimeError(
        "Pre-load FAILED: NWK_NEW_EHRP_ACTIONS_TBL is empty. "
        "No employee records staged for processing."
    )

log_info(f"Staging table has {staging_count} records ready for processing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Source Table Row Counts and Statistics

# COMMAND ----------

# Collect row counts for all source and lookup tables
source_stats = {}
for table in REQUIRED_TABLES:
    try:
        row_count = spark.table(f"{DATABASE}.{table}").count()
        source_stats[table] = row_count
        log_info(f"Row count for {table}: {row_count}")
    except Exception as e:
        log_error(f"Failed to count rows in {table}: {str(e)}")
        source_stats[table] = -1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Validate Source Qualifier Join Will Produce Results

# COMMAND ----------

# Verify that the inner join between NWK_NEW_EHRP_ACTIONS_TBL and PS_GVT_JOB
# will produce results (matching the Source Qualifier SQL override)
actions_df = spark.table(f"{DATABASE}.nwk_new_ehrp_actions_tbl")
gvt_job_df = spark.table(f"{DATABASE}.ps_gvt_job")

join_count = actions_df.join(
    gvt_job_df,
    on=["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"],
    how="inner"
).count()

log_info(f"Source Qualifier join result count: {join_count}")

if join_count == 0:
    log_error(
        "Source Qualifier join produces 0 rows. "
        "NWK_NEW_EHRP_ACTIONS_TBL records do not match PS_GVT_JOB."
    )
    raise RuntimeError(
        "Pre-load FAILED: Source Qualifier join returns 0 rows. "
        "Check that NWK_NEW_EHRP_ACTIONS_TBL keys exist in PS_GVT_JOB."
    )

log_info(f"Source Qualifier join will produce {join_count} records for ETL")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Validate Sequence Number Table

# COMMAND ----------

# The SEQUENCE_NUM_TBL stores the last-used event ID sequence number per year.
# The ETL uses this as the starting point for EVENT_ID generation.
seq_df = spark.table(f"{DATABASE}.sequence_num_tbl")
seq_count = seq_df.count()

log_info(f"SEQUENCE_NUM_TBL has {seq_count} rows")

if seq_count > 0:
    seq_df.orderBy("EHRP_YEAR").show(truncate=False)
else:
    log_error("SEQUENCE_NUM_TBL is empty - Event ID generation may fail")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Pre-Load Summary

# COMMAND ----------

log_info("=" * 60)
log_info("EHRP2BIIS_UPDATE PRE-LOAD VALIDATION SUMMARY")
log_info("=" * 60)
log_info(f"Database: {DATABASE}")
log_info(f"Validation Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
log_info(f"Staging Records (NWK_NEW_EHRP_ACTIONS_TBL): {staging_count}")
log_info(f"Source Qualifier Join Result Count: {join_count}")
log_info("-" * 60)
log_info("Source/Lookup Table Row Counts:")
for table, cnt in source_stats.items():
    status = "OK" if cnt > 0 else ("EMPTY" if cnt == 0 else "ERROR")
    log_info(f"  {table}: {cnt} [{status}]")
log_info("-" * 60)

error_count = sum(1 for entry in LOG_ENTRIES if entry["level"] == "ERROR")
if error_count > 0:
    log_error(f"Pre-load completed with {error_count} error(s)")
else:
    log_info("Pre-load validation completed successfully - ready for ETL")

# COMMAND ----------

# Persist pre-load log as a Delta table for audit trail
log_df = spark.createDataFrame(LOG_ENTRIES)
log_df = log_df.withColumn("run_date", current_date())

log_df.write.format("delta").mode("append").saveAsTable(
    f"{DATABASE}.etl_preload_log"
)

log_info("Pre-load log persisted to etl_preload_log table")
print("\n*** EHRP2BIIS_UPDATE Pre-Load Validation COMPLETE ***")
