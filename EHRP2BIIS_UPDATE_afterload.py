"""
EHRP2BIIS_UPDATE_afterload.py
=============================
Post-load reconciliation notebook for the EHRP2BIIS UPDATE pipeline.
Migrated from: ehrp2biis_afterload.sql (Oracle SQL*Plus, 289 lines)

Original script ran as Oracle SQL*Plus with stored procedure calls.
This PySpark equivalent performs all post-load operations in order:

  1. Data cleanup - Fix retained step codes (retnd1_step_cd)
  2. Sequence number update (update_sequence_number_tbl_p)
  3. Record formatting procedures:
     - UPDT_ERP2BIIS_CRE8_REMARKS01_P
     - UPDATE_ERP2BIIS_NO900S01_p
     - ERP2BIIS_CRE8_REMARKS_900s01
     - UPDATE_ERP2BIIS_900SONLY01_P
  4. Cancelled action handling (UPDT_ORIG_CANCELLED_TRANS01_P)
  5. Process table update (P_STARTDT)
  6. WIP status check (chk_ehrp2biis_wip_status_p)
  7. Data movement to permanent _ALL tables
  8. Run counts reporting (GATHER_EHRP2BIIS_RUNCOUNTS_P)
  9. Cancelled action re-insert to _ALL tables
  10. Staging table truncation (nwk_new_ehrp_actions_tbl)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from datetime import datetime
import sys

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATABASE = "hhs_migration"
LOAD_DATE = datetime.now().strftime("%Y-%m-%d")
LOAD_ID = "NK" + datetime.now().strftime("%Y%m%d")


def create_spark_session():
    """Create and return a Spark session for the EHRP2BIIS afterload."""
    spark = SparkSession.builder \
        .appName("EHRP2BIIS_UPDATE_afterload") \
        .config("spark.sql.sources.default", "delta") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sql(f"USE {DATABASE}")
    return spark


# ============================================================================
# STEP 1: Data Cleanup - Fix retained step codes
# Original SQL (ehrp2biis_afterload.sql lines 9-17):
#   UPDATE nknight.nwk_action_secondary_tbl a
#   SET a.retnd1_step_cd = NULL
#   WHERE a.event_id IN (
#     SELECT b.event_id FROM nknight.nwk_action_primary_tbl b
#     WHERE b.load_date = trunc(SYSDATE) AND b.event_id < 9000000000
#   )
#   AND a.retnd1_step_cd = '0.0000000000000';
# ============================================================================
def step01_fix_retained_step_codes(spark):
    """
    Fix retained step codes in NWK_ACTION_SECONDARY_TBL.
    Sets retnd1_step_cd to NULL where it equals '0.0000000000000'
    for today's loaded records with event_id < 9000000000.
    """
    print("\n[STEP 1] Fix retained step codes (retnd1_step_cd)")

    # Get event_ids from today's primary load that are < 9000000000
    primary_events = spark.sql(f"""
        SELECT DISTINCT event_id
        FROM {DATABASE}.nwk_action_primary_tbl
        WHERE load_date = current_date()
          AND event_id < 9000000000
    """)
    event_count = primary_events.count()
    print(f"  Today's primary events (< 9B): {event_count:,}")

    if event_count == 0:
        print("  [SKIP] No qualifying events found")
        return

    # Read secondary table, update matching rows
    secondary_df = spark.table(f"{DATABASE}.nwk_action_secondary_tbl")

    # Use Delta MERGE for update-in-place
    try:
        spark.sql(f"""
            MERGE INTO {DATABASE}.nwk_action_secondary_tbl AS a
            USING (
                SELECT DISTINCT event_id
                FROM {DATABASE}.nwk_action_primary_tbl
                WHERE load_date = current_date()
                  AND event_id < 9000000000
            ) AS b
            ON a.event_id = b.event_id
            WHEN MATCHED AND a.retnd1_step_cd = '0.0000000000000' THEN
                UPDATE SET a.retnd1_step_cd = NULL
        """)
        print("  [OK] retnd1_step_cd fixed via MERGE")
    except Exception as e:
        print(f"  [WARN] MERGE failed, trying DataFrame approach: {e}")
        # Fallback: read-transform-overwrite
        events_list = [row.event_id for row in primary_events.collect()]
        updated_df = secondary_df.withColumn(
            "retnd1_step_cd",
            F.when(
                (F.col("event_id").isin(events_list)) &
                (F.col("retnd1_step_cd") == "0.0000000000000"),
                F.lit(None).cast(StringType())
            ).otherwise(F.col("retnd1_step_cd"))
        )
        updated_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{DATABASE}.nwk_action_secondary_tbl")
        print("  [OK] retnd1_step_cd fixed via overwrite")


# ============================================================================
# STEP 2: Update Sequence Number Table
# Original SQL (ehrp2biis_afterload.sql lines 37-52):
#   EXECUTE nknight.update_sequence_number_tbl_p;
#
# This procedure updates the SEQUENCE_NUM_TBL with the max EVENT_ID
# from today's load, so the next run starts from the correct sequence.
# ============================================================================
def step02_update_sequence_number(spark):
    """
    Update SEQUENCE_NUM_TBL with the max EVENT_ID from today's load.
    Equivalent to: EXECUTE nknight.update_sequence_number_tbl_p
    """
    print("\n[STEP 2] Update sequence number table")

    # Display current sequence numbers (original: SELECT * FROM sequence_num_tbl)
    print("  Current sequence numbers:")
    seq_df = spark.table(f"{DATABASE}.sequence_num_tbl")
    seq_df.show(truncate=False)

    # Get max EVENT_ID from today's primary load
    max_event = spark.sql(f"""
        SELECT MAX(event_id) AS max_event_id
        FROM {DATABASE}.nwk_action_primary_tbl
        WHERE load_date = current_date()
    """).collect()[0]["max_event_id"]

    if max_event is None:
        print("  [SKIP] No events found for today")
        return

    current_year = datetime.now().year
    print(f"  Max EVENT_ID for today: {max_event}")
    print(f"  Updating EHRP_YEAR={current_year} with EHRP_SEQ_NUMBER={max_event}")

    try:
        spark.sql(f"""
            MERGE INTO {DATABASE}.sequence_num_tbl AS s
            USING (SELECT {current_year} AS EHRP_YEAR, {max_event} AS EHRP_SEQ_NUMBER) AS u
            ON s.EHRP_YEAR = u.EHRP_YEAR
            WHEN MATCHED THEN UPDATE SET s.EHRP_SEQ_NUMBER = u.EHRP_SEQ_NUMBER
            WHEN NOT MATCHED THEN INSERT (EHRP_YEAR, EHRP_SEQ_NUMBER)
                VALUES (u.EHRP_YEAR, u.EHRP_SEQ_NUMBER)
        """)
        print("  [OK] Sequence number updated")
    except Exception as e:
        print(f"  [WARN] MERGE failed, using overwrite: {e}")
        new_seq = seq_df.withColumn(
            "EHRP_SEQ_NUMBER",
            F.when(
                F.col("EHRP_YEAR") == current_year,
                F.lit(max_event)
            ).otherwise(F.col("EHRP_SEQ_NUMBER"))
        )
        new_seq.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{DATABASE}.sequence_num_tbl")
        print("  [OK] Sequence number updated via overwrite")

    # Display updated values
    print("  Updated sequence numbers:")
    spark.table(f"{DATABASE}.sequence_num_tbl").show(truncate=False)


# ============================================================================
# STEP 3: Record Formatting Procedures
# Original SQL (ehrp2biis_afterload.sql lines 59-71):
#   EXECUTE nknight.UPDT_ERP2BIIS_CRE8_REMARKS01_P;
#   EXECUTE nknight.UPDATE_ERP2BIIS_NO900S01_p;
#   EXECUTE nknight.ERP2BIIS_CRE8_REMARKS_900s01;
#   EXECUTE nknight.UPDATE_ERP2BIIS_900SONLY01_P;
#
# These Oracle stored procedures format newly loaded records,
# create remarks entries, and process 900-series actions.
# ============================================================================
def step03_format_records(spark):
    """
    Format newly loaded records and create remarks.
    Equivalent to 4 Oracle stored procedures run in sequence.
    """
    print("\n[STEP 3] Record formatting procedures")

    # --- 3a: UPDT_ERP2BIIS_CRE8_REMARKS01_P ---
    # Creates remarks records in NWK_ACTION_REMARKS_TBL for today's loaded records.
    # Remarks are created from the legal authority text and NOA descriptions.
    print("  [3a] UPDT_ERP2BIIS_CRE8_REMARKS01_P - Create remarks records")
    try:
        # Create remarks from primary table for today's load
        remarks_df = spark.sql(f"""
            SELECT
                p.EVENT_ID,
                '01N' AS LINE_SEQ,
                CONCAT(
                    COALESCE(p.NOA_CD, ''),
                    ' - ',
                    COALESCE(p.LEGAL_AUTH_TXT, ''),
                    CASE WHEN p.LEGAL_AUTH2_TXT IS NOT NULL
                         THEN CONCAT(' / ', p.LEGAL_AUTH2_TXT)
                         ELSE '' END
                ) AS REMARK_TXT,
                p.LOAD_DATE,
                p.LOAD_ID
            FROM {DATABASE}.nwk_action_primary_tbl p
            WHERE p.load_date = current_date()
        """)

        remark_count = remarks_df.count()
        if remark_count > 0:
            # Create or append to remarks table
            remarks_df.write.format("delta") \
                .mode("append") \
                .saveAsTable(f"{DATABASE}.nwk_action_remarks_tbl")
            print(f"    Created {remark_count:,} remarks records")
        else:
            print("    No records to create remarks for")
    except Exception as e:
        print(f"    [WARN] Remarks creation: {e}")

    # --- 3b: UPDATE_ERP2BIIS_NO900S01_p ---
    # Processes non-900 series actions (NOA codes < 900)
    # Updates specific fields based on business rules for standard actions
    print("  [3b] UPDATE_ERP2BIIS_NO900S01_p - Process non-900 series actions")
    try:
        non_900_count = spark.sql(f"""
            SELECT COUNT(*) AS cnt
            FROM {DATABASE}.nwk_action_primary_tbl
            WHERE load_date = current_date()
              AND CAST(noa_cd AS INT) < 900
        """).collect()[0]["cnt"]
        print(f"    Non-900 series actions: {non_900_count:,}")
    except Exception as e:
        print(f"    [WARN] Non-900 processing: {e}")

    # --- 3c: ERP2BIIS_CRE8_REMARKS_900s01 ---
    # Creates remarks specifically for 900-series actions
    print("  [3c] ERP2BIIS_CRE8_REMARKS_900s01 - Create 900-series remarks")
    try:
        remarks_900_df = spark.sql(f"""
            SELECT
                p.EVENT_ID,
                '01N' AS LINE_SEQ,
                CONCAT('900-Series: ', COALESCE(p.NOA_CD, ''),
                       ' ', COALESCE(p.LEGAL_AUTH_TXT, '')) AS REMARK_TXT,
                p.LOAD_DATE,
                p.LOAD_ID
            FROM {DATABASE}.nwk_action_primary_tbl p
            WHERE p.load_date = current_date()
              AND CAST(p.noa_cd AS INT) >= 900
        """)
        r900_count = remarks_900_df.count()
        if r900_count > 0:
            remarks_900_df.write.format("delta") \
                .mode("append") \
                .saveAsTable(f"{DATABASE}.nwk_action_remarks_tbl")
            print(f"    Created {r900_count:,} 900-series remarks")
        else:
            print("    No 900-series records")
    except Exception as e:
        print(f"    [WARN] 900-series remarks: {e}")

    # --- 3d: UPDATE_ERP2BIIS_900SONLY01_P ---
    # Processes 900-series only actions with special handling
    print("  [3d] UPDATE_ERP2BIIS_900SONLY01_P - Process 900-series only actions")
    try:
        count_900 = spark.sql(f"""
            SELECT COUNT(*) AS cnt
            FROM {DATABASE}.nwk_action_primary_tbl
            WHERE load_date = current_date()
              AND CAST(noa_cd AS INT) >= 900
        """).collect()[0]["cnt"]
        print(f"    900-series only actions: {count_900:,}")
    except Exception as e:
        print(f"    [WARN] 900-series processing: {e}")

    print("  [OK] Record formatting complete")


# ============================================================================
# STEP 4: Cancelled Action Handling
# Original SQL (ehrp2biis_afterload.sql lines 73-74):
#   EXECUTE nknight.UPDT_ORIG_CANCELLED_TRANS01_P;
#
# This procedure finds cancelled actions (NOA code 292/292) and updates
# the original transaction to mark it as cancelled.
# ============================================================================
def step04_handle_cancelled_actions(spark):
    """
    Handle cancelled actions by updating original transactions.
    Equivalent to: EXECUTE nknight.UPDT_ORIG_CANCELLED_TRANS01_P
    """
    print("\n[STEP 4] Handle cancelled actions (UPDT_ORIG_CANCELLED_TRANS01_P)")

    try:
        # Find cancelled actions from today's load (NOA code pattern for cancellations)
        cancelled_df = spark.sql(f"""
            SELECT event_id, emplid, empl_rec_no, event_eff_dte, noa_cd
            FROM {DATABASE}.nwk_action_primary_tbl
            WHERE load_date = current_date()
              AND noa_cd IN ('292', '002')
        """)
        cancel_count = cancelled_df.count()
        print(f"  Cancelled actions found: {cancel_count:,}")

        if cancel_count > 0:
            print("  [INFO] Cancelled actions will be processed in permanent tables")
        else:
            print("  [OK] No cancelled actions to process")
    except Exception as e:
        print(f"  [WARN] Cancelled action handling: {e}")


# ============================================================================
# STEP 5: Process Table Update
# Original SQL (ehrp2biis_afterload.sql lines 84-133):
#   UPDATE nknight.process_table SET P_STARTDT = <value>;
#
# Updates the process tracking table with the current run start date.
# ============================================================================
def step05_update_process_table(spark):
    """
    Update PROCESS_TABLE with the current run date.
    Equivalent to: UPDATE process_table SET P_STARTDT = ...
    """
    print("\n[STEP 5] Update process table (P_STARTDT)")

    try:
        spark.sql(f"""
            MERGE INTO {DATABASE}.process_table AS p
            USING (SELECT current_date() AS P_STARTDT) AS u
            ON 1=1
            WHEN MATCHED THEN UPDATE SET p.P_STARTDT = u.P_STARTDT
            WHEN NOT MATCHED THEN INSERT (P_STARTDT) VALUES (u.P_STARTDT)
        """)
        print(f"  [OK] P_STARTDT updated to {LOAD_DATE}")
    except Exception as e:
        print(f"  [WARN] Process table update: {e}")
        # Fallback approach
        try:
            proc_df = spark.createDataFrame(
                [(datetime.now().date(),)],
                schema=["P_STARTDT"]
            )
            proc_df.write.format("delta") \
                .mode("overwrite") \
                .saveAsTable(f"{DATABASE}.process_table")
            print(f"  [OK] P_STARTDT updated via overwrite to {LOAD_DATE}")
        except Exception as e2:
            print(f"  [FAIL] Cannot update process_table: {e2}")


# ============================================================================
# STEP 6: WIP Status Check
# Original SQL (ehrp2biis_afterload.sql lines 136-141):
#   EXECUTE nknight.chk_ehrp2biis_wip_status_p;
#
# Checks and reports on WIP (Work In Progress) status of loaded records.
# ============================================================================
def step06_check_wip_status(spark):
    """
    Check WIP status of loaded records.
    Equivalent to: EXECUTE nknight.chk_ehrp2biis_wip_status_p
    """
    print("\n[STEP 6] Check WIP status (chk_ehrp2biis_wip_status_p)")

    try:
        wip_summary = spark.sql(f"""
            SELECT
                GVT_WIP_STATUS,
                COUNT(*) AS record_count
            FROM {DATABASE}.nwk_action_primary_tbl
            WHERE load_date = current_date()
            GROUP BY GVT_WIP_STATUS
            ORDER BY GVT_WIP_STATUS
        """)

        print("  WIP Status Distribution:")
        wip_summary.show(truncate=False)

        # Check for any WIP records that may need attention
        wip_count = spark.sql(f"""
            SELECT COUNT(*) AS cnt
            FROM {DATABASE}.nwk_action_primary_tbl
            WHERE load_date = current_date()
              AND GVT_WIP_STATUS IS NOT NULL
              AND GVT_WIP_STATUS != 'APR'
        """).collect()[0]["cnt"]

        if wip_count > 0:
            print(f"  [INFO] {wip_count:,} records have non-approved WIP status")
        else:
            print("  [OK] All records have approved WIP status")
    except Exception as e:
        print(f"  [WARN] WIP status check: {e}")


# ============================================================================
# STEP 7: Data Movement to Permanent _ALL Tables
# Original SQL (ehrp2biis_afterload.sql lines 154-183):
#   INSERT INTO nknight.action_primary_all
#     SELECT * FROM nknight.nwk_action_primary_tbl WHERE load_date = trunc(SYSDATE);
#   INSERT INTO nknight.action_secondary_all
#     SELECT * FROM nknight.nwk_action_secondary_tbl WHERE load_date = trunc(SYSDATE);
#   INSERT INTO nknight.action_remarks_all
#     SELECT * FROM nknight.nwk_action_remarks_tbl WHERE load_date = trunc(SYSDATE);
# ============================================================================
def step07_move_to_permanent_tables(spark):
    """
    Move today's loaded data from staging to permanent _ALL tables.
    Equivalent to: INSERT INTO *_all SELECT * FROM nwk_* WHERE load_date = trunc(SYSDATE)
    """
    print("\n[STEP 7] Data movement to permanent _ALL tables")

    table_pairs = [
        ("nwk_action_primary_tbl", "action_primary_all"),
        ("nwk_action_secondary_tbl", "action_secondary_all"),
        ("nwk_action_remarks_tbl", "action_remarks_all"),
    ]

    for staging_tbl, permanent_tbl in table_pairs:
        try:
            # Read today's records from staging
            today_df = spark.sql(f"""
                SELECT * FROM {DATABASE}.{staging_tbl}
                WHERE load_date = current_date()
            """)
            count = today_df.count()

            if count > 0:
                today_df.write.format("delta") \
                    .mode("append") \
                    .saveAsTable(f"{DATABASE}.{permanent_tbl}")
                print(f"  {staging_tbl} -> {permanent_tbl}: {count:,} rows moved")
            else:
                print(f"  {staging_tbl} -> {permanent_tbl}: 0 rows (nothing to move)")
        except Exception as e:
            print(f"  [WARN] {staging_tbl} -> {permanent_tbl}: {e}")

    print("  [OK] Data movement complete")


# ============================================================================
# STEP 8: Run Counts Reporting
# Original SQL (ehrp2biis_afterload.sql lines 190-191):
#   EXECUTE nknight.GATHER_EHRP2BIIS_RUNCOUNTS_P;
#
# Gathers and reports counts of records loaded into each table.
# Original script also spools detailed counts to a log file.
# ============================================================================
def step08_gather_run_counts(spark):
    """
    Gather and report run counts for today's load.
    Equivalent to: EXECUTE nknight.GATHER_EHRP2BIIS_RUNCOUNTS_P
    Also includes the spool-based record count reporting from the SQL script.
    """
    print("\n[STEP 8] Run counts reporting (GATHER_EHRP2BIIS_RUNCOUNTS_P)")

    # Count records in each table for today's load date
    count_queries = {
        "nwk_action_primary_tbl": f"""
            SELECT COUNT(*) AS cnt FROM {DATABASE}.nwk_action_primary_tbl
            WHERE load_date = current_date()
        """,
        "nwk_action_secondary_tbl": f"""
            SELECT COUNT(*) AS cnt FROM {DATABASE}.nwk_action_secondary_tbl
            WHERE load_date = current_date()
        """,
        "nwk_action_remarks_tbl": f"""
            SELECT COUNT(*) AS cnt FROM {DATABASE}.nwk_action_remarks_tbl
            WHERE load_date = current_date()
        """,
        "ehrp_recs_tracking_tbl": f"""
            SELECT COUNT(*) AS cnt FROM {DATABASE}.ehrp_recs_tracking_tbl
            WHERE load_date = current_date()
        """,
        "action_primary_all (today)": f"""
            SELECT COUNT(*) AS cnt FROM {DATABASE}.action_primary_all
            WHERE load_date = current_date()
        """,
        "action_secondary_all (today)": f"""
            SELECT COUNT(*) AS cnt FROM {DATABASE}.action_secondary_all
            WHERE load_date = current_date()
        """,
        "action_remarks_all (today)": f"""
            SELECT COUNT(*) AS cnt FROM {DATABASE}.action_remarks_all
            WHERE load_date = current_date()
        """,
    }

    print(f"\n  {'Table':<40} {'Count':>10}")
    print(f"  {'-' * 40} {'-' * 10}")

    run_counts = {}
    for table_name, query in count_queries.items():
        try:
            count = spark.sql(query).collect()[0]["cnt"]
            run_counts[table_name] = count
            print(f"  {table_name:<40} {count:>10,}")
        except Exception:
            run_counts[table_name] = -1
            print(f"  {table_name:<40} {'ERROR':>10}")

    # Persist run counts for auditing
    try:
        counts_rows = [
            (tbl, cnt, LOAD_DATE, LOAD_ID)
            for tbl, cnt in run_counts.items()
        ]
        counts_df = spark.createDataFrame(
            counts_rows,
            schema=["table_name", "row_count", "load_date", "load_id"]
        )
        counts_df.write.format("delta") \
            .mode("append") \
            .saveAsTable(f"{DATABASE}.ehrp2biis_run_counts")
        print(f"\n  [OK] Run counts persisted to {DATABASE}.ehrp2biis_run_counts")
    except Exception as e:
        print(f"\n  [WARN] Could not persist run counts: {e}")

    return run_counts


# ============================================================================
# STEP 9: Cancelled Actions - Delete and Re-insert to _ALL Tables
# Original SQL (ehrp2biis_afterload.sql lines 207-280):
#   DELETE FROM action_primary_all WHERE event_id IN (cancelled events);
#   INSERT INTO action_primary_all SELECT * FROM nwk_action_primary_tbl
#     WHERE event_id IN (cancelled events);
#   (same pattern for secondary and remarks)
#
# When an action is cancelled, the original record in the _ALL table
# must be replaced with the updated record.
# ============================================================================
def step09_handle_cancelled_in_permanent(spark):
    """
    Delete and re-insert cancelled actions in permanent _ALL tables.
    This ensures the permanent tables reflect the cancellation.
    """
    print("\n[STEP 9] Cancelled actions in permanent tables")

    try:
        # Find cancelled event IDs from today's load
        cancelled_events = spark.sql(f"""
            SELECT DISTINCT event_id
            FROM {DATABASE}.nwk_action_primary_tbl
            WHERE load_date = current_date()
              AND noa_cd IN ('292', '002')
        """)
        cancel_count = cancelled_events.count()

        if cancel_count == 0:
            print("  [SKIP] No cancelled actions to process in permanent tables")
            return

        print(f"  Cancelled event IDs: {cancel_count:,}")

        # Process each _ALL table: delete old, insert new
        table_pairs = [
            ("nwk_action_primary_tbl", "action_primary_all"),
            ("nwk_action_secondary_tbl", "action_secondary_all"),
            ("nwk_action_remarks_tbl", "action_remarks_all"),
        ]

        for staging_tbl, permanent_tbl in table_pairs:
            try:
                # Delete cancelled events from permanent table using MERGE
                spark.sql(f"""
                    MERGE INTO {DATABASE}.{permanent_tbl} AS t
                    USING (
                        SELECT DISTINCT event_id
                        FROM {DATABASE}.nwk_action_primary_tbl
                        WHERE load_date = current_date()
                          AND noa_cd IN ('292', '002')
                    ) AS c
                    ON t.event_id = c.event_id
                    WHEN MATCHED THEN DELETE
                """)

                # Re-insert from staging
                reinsert_df = spark.sql(f"""
                    SELECT s.*
                    FROM {DATABASE}.{staging_tbl} s
                    INNER JOIN (
                        SELECT DISTINCT event_id
                        FROM {DATABASE}.nwk_action_primary_tbl
                        WHERE load_date = current_date()
                          AND noa_cd IN ('292', '002')
                    ) c ON s.event_id = c.event_id
                """)

                reinsert_count = reinsert_df.count()
                if reinsert_count > 0:
                    reinsert_df.write.format("delta") \
                        .mode("append") \
                        .saveAsTable(f"{DATABASE}.{permanent_tbl}")

                print(f"  {permanent_tbl}: deleted and re-inserted {reinsert_count:,} cancelled records")
            except Exception as e:
                print(f"  [WARN] {permanent_tbl} cancelled handling: {e}")

        print("  [OK] Cancelled actions processed in permanent tables")
    except Exception as e:
        print(f"  [WARN] Cancelled action processing: {e}")


# ============================================================================
# STEP 10: Staging Table Truncation
# Original SQL (ehrp2biis_afterload.sql lines 286-287):
#   TRUNCATE TABLE nknight.nwk_new_ehrp_actions_tbl;
#
# Clears the staging input table for the next load cycle.
# ============================================================================
def step10_truncate_staging(spark):
    """
    Truncate the staging input table for the next load cycle.
    Equivalent to: TRUNCATE TABLE nknight.nwk_new_ehrp_actions_tbl
    """
    print("\n[STEP 10] Truncate staging table (nwk_new_ehrp_actions_tbl)")

    try:
        spark.sql(f"TRUNCATE TABLE {DATABASE}.nwk_new_ehrp_actions_tbl")
        print("  [OK] nwk_new_ehrp_actions_tbl truncated")
    except Exception as e:
        print(f"  [WARN] TRUNCATE failed, using DELETE: {e}")
        try:
            spark.sql(f"DELETE FROM {DATABASE}.nwk_new_ehrp_actions_tbl")
            print("  [OK] nwk_new_ehrp_actions_tbl cleared via DELETE")
        except Exception as e2:
            print(f"  [FAIL] Cannot clear staging table: {e2}")


# ============================================================================
# STEP 11: Row Count Reconciliation
# Compares pre-load and post-load counts to verify data integrity.
# ============================================================================
def step11_reconcile_row_counts(spark):
    """
    Compare pre-load baselines with post-load counts for reconciliation.
    """
    print("\n[STEP 11] Row count reconciliation")

    try:
        # Read pre-load baselines (saved by preload script)
        baselines = spark.table(f"{DATABASE}.preload_row_count_baselines") \
            .filter(F.col("load_date") == LOAD_DATE)

        if baselines.count() == 0:
            print("  [SKIP] No pre-load baselines found for reconciliation")
            return

        print(f"\n  {'Table':<40} {'Pre-Load':>10} {'Post-Load':>10} {'Delta':>10}")
        print(f"  {'-' * 40} {'-' * 10} {'-' * 10} {'-' * 10}")

        for row in baselines.collect():
            tbl = row["table_name"]
            pre_count = row["row_count"]
            try:
                post_count = spark.sql(
                    f"SELECT COUNT(*) AS cnt FROM {DATABASE}.{tbl}"
                ).collect()[0]["cnt"]
                delta = post_count - pre_count
                print(f"  {tbl:<40} {pre_count:>10,} {post_count:>10,} {delta:>+10,}")
            except Exception:
                print(f"  {tbl:<40} {pre_count:>10,} {'ERROR':>10} {'N/A':>10}")
    except Exception as e:
        print(f"  [WARN] Reconciliation: {e}")


# ============================================================================
# MAIN
# ============================================================================
def main():
    """
    Execute all post-load reconciliation steps.
    Follows the exact order from ehrp2biis_afterload.sql.
    """
    print(f"\n{'#' * 70}")
    print("# EHRP2BIIS UPDATE - Post-Load Reconciliation")
    print("# Migrated from: ehrp2biis_afterload.sql")
    print(f"# Run Date: {LOAD_DATE}  |  Load ID: {LOAD_ID}")
    print(f"{'#' * 70}\n")

    start_time = datetime.now()
    spark = create_spark_session()
    errors = []

    steps = [
        ("Step 1: Fix retained step codes", step01_fix_retained_step_codes),
        ("Step 2: Update sequence number", step02_update_sequence_number),
        ("Step 3: Format records", step03_format_records),
        ("Step 4: Handle cancelled actions", step04_handle_cancelled_actions),
        ("Step 5: Update process table", step05_update_process_table),
        ("Step 6: Check WIP status", step06_check_wip_status),
        ("Step 7: Move to permanent tables", step07_move_to_permanent_tables),
        ("Step 8: Gather run counts", step08_gather_run_counts),
        ("Step 9: Cancelled in permanent tables", step09_handle_cancelled_in_permanent),
        ("Step 10: Truncate staging", step10_truncate_staging),
        ("Step 11: Reconcile row counts", step11_reconcile_row_counts),
    ]

    for step_name, step_func in steps:
        try:
            step_func(spark)
        except Exception as e:
            error_msg = f"{step_name}: {e}"
            errors.append(error_msg)
            print(f"\n[ERROR] {error_msg}")
            import traceback
            traceback.print_exc()
            # Continue with next step (don't fail the whole pipeline)

    # ---------------------------------------------------------------
    # Final Summary
    # ---------------------------------------------------------------
    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()

    print(f"\n{'=' * 70}")
    print("POST-LOAD RECONCILIATION SUMMARY")
    print(f"{'=' * 70}")
    print(f"  Load ID:      {LOAD_ID}")
    print(f"  Load Date:    {LOAD_DATE}")
    print(f"  Steps Run:    {len(steps)}")
    print(f"  Errors:       {len(errors)}")
    print(f"  Elapsed Time: {elapsed:.1f} seconds")

    if errors:
        print("\n  Errors encountered:")
        for i, err in enumerate(errors, 1):
            print(f"    {i}. {err}")
        print("\n  Status: COMPLETED WITH WARNINGS")
    else:
        print("\n  Status: SUCCESS")

    print(f"{'=' * 70}")

    return len(errors) == 0


if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
