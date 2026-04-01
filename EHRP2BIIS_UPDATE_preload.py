"""
EHRP2BIIS_UPDATE_preload.py
===========================
Pre-load validation notebook for the EHRP2BIIS UPDATE pipeline.
Migrated from: ehrp2biis_preload (ksh) and actstage_load (ksh)

Original scripts connected to Oracle via sqlplus, ran pre-load SQL validation
(step01), and checked for errors before the Informatica session executed.

This PySpark equivalent performs:
  1. Spark session / environment validation
  2. Source table existence and accessibility checks
  3. Row count baseline capture
  4. Data quality pre-checks on key source columns
  5. Lookup table validation
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime
import sys

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATABASE = "hhs_migration"
LOAD_DATE = datetime.now().strftime("%Y-%m-%d")
LOAD_ID = "NK" + datetime.now().strftime("%Y%m%d")

# Source tables (from Informatica Source Qualifier SQ_PS_GVT_JOB)
SOURCE_TABLES = [
    "nwk_new_ehrp_actions_tbl",
    "ps_gvt_job",
]

# Lookup tables used by the 9 Informatica lookup transformations
LOOKUP_TABLES = [
    "sequence_num_tbl",         # lkp_OLD_SEQUENCE_NUMBER
    "ps_gvt_employment",        # lkp_PS_GVT_EMPLOYMENT
    "ps_gvt_pers_nid",          # lkp_PS_GVT_PERS_NID
    "ps_gvt_awd_data",          # lkp_PS_GVT_AWD_DATA
    "ps_gvt_ee_data_trk",       # lkp_PS_GVT_EE_DATA_TRK
    "ps_he_fill_pos",           # lkp_PS_HE_FILL_POS
    "ps_gvt_citizenship",       # lkp_PS_GVT_CITIZENSHIP
    "ps_gvt_pers_data",         # lkp_PS_GVT_PERS_DATA
    "ps_jpm_jp_items",          # lkp_PS_JPM_JP_ITEMS
]

# Target tables (Informatica targets)
TARGET_TABLES = [
    "nwk_action_primary_tbl",
    "nwk_action_secondary_tbl",
    "ehrp_recs_tracking_tbl",
]

# Post-load permanent tables
PERMANENT_TABLES = [
    "action_primary_all",
    "action_secondary_all",
    "action_remarks_all",
    "process_table",
]

ALL_REQUIRED_TABLES = SOURCE_TABLES + LOOKUP_TABLES + TARGET_TABLES + PERMANENT_TABLES


def create_spark_session():
    """Create and return a Spark session for the EHRP2BIIS pre-load validation."""
    spark = SparkSession.builder \
        .appName("EHRP2BIIS_UPDATE_preload") \
        .config("spark.sql.sources.default", "delta") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


def validate_environment(spark):
    """
    Validate Spark session and database connectivity.
    Equivalent to the SETENV sourcing + Oracle connect in ehrp2biis_preload.
    """
    print("=" * 70)
    print(f"EHRP2BIIS Pre-Load Validation  |  {LOAD_DATE}")
    print("=" * 70)

    # Verify Spark session is alive
    try:
        spark.sql("SELECT 1 AS health_check").collect()
        print("[OK] Spark session is active.")
    except Exception as e:
        print(f"[FAIL] Spark session is not responsive: {e}")
        raise

    # Verify target database exists
    try:
        databases = [row.databaseName for row in spark.sql("SHOW DATABASES").collect()]
        if DATABASE in databases:
            print(f"[OK] Database '{DATABASE}' exists.")
        else:
            print(f"[FAIL] Database '{DATABASE}' does not exist. Available: {databases}")
            raise RuntimeError(f"Database '{DATABASE}' not found.")
    except Exception as e:
        print(f"[FAIL] Error checking databases: {e}")
        raise

    # Set the current database
    spark.sql(f"USE {DATABASE}")
    print(f"[OK] Using database '{DATABASE}'.")


def check_table_existence(spark):
    """
    Verify all required tables exist and are accessible.
    Equivalent to the implicit Oracle table access checks in step01.
    """
    print("\n--- Table Existence Checks ---")
    available_tables = [
        row.tableName
        for row in spark.sql(f"SHOW TABLES IN {DATABASE}").collect()
    ]
    available_lower = [t.lower() for t in available_tables]

    missing = []
    for tbl in ALL_REQUIRED_TABLES:
        if tbl.lower() in available_lower:
            print(f"  [OK] {DATABASE}.{tbl}")
        else:
            print(f"  [MISSING] {DATABASE}.{tbl}")
            missing.append(tbl)

    if missing:
        print(f"\n[WARN] Missing tables ({len(missing)}): {missing}")
        print("       The ETL may fail if these are required at runtime.")
    else:
        print(f"\n[OK] All {len(ALL_REQUIRED_TABLES)} required tables are present.")

    return missing


def capture_row_count_baselines(spark):
    """
    Capture baseline row counts for source and target tables.
    These are compared post-load for reconciliation.
    """
    print("\n--- Row Count Baselines ---")
    baselines = {}

    for tbl in SOURCE_TABLES + TARGET_TABLES + PERMANENT_TABLES:
        try:
            count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {DATABASE}.{tbl}").collect()[0]["cnt"]
            baselines[tbl] = count
            print(f"  {DATABASE}.{tbl}: {count:,} rows")
        except Exception as e:
            baselines[tbl] = -1
            print(f"  {DATABASE}.{tbl}: ERROR - {e}")

    # Persist baselines for afterload reconciliation
    try:
        baseline_rows = [(tbl, cnt, LOAD_DATE, LOAD_ID) for tbl, cnt in baselines.items()]
        baseline_df = spark.createDataFrame(
            baseline_rows,
            schema=["table_name", "row_count", "load_date", "load_id"]
        )
        baseline_df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(f"{DATABASE}.preload_row_count_baselines")
        print(f"\n[OK] Baselines persisted to {DATABASE}.preload_row_count_baselines")
    except Exception as e:
        print(f"\n[WARN] Could not persist baselines: {e}")

    return baselines


def validate_source_data_quality(spark):
    """
    Run data quality checks on key source columns.
    Mirrors the implicit validation that the Oracle step01 script performed.
    """
    print("\n--- Source Data Quality Checks ---")
    errors = []

    # ---------------------------------------------------------------
    # 1. NWK_NEW_EHRP_ACTIONS_TBL: key columns must be non-null
    #    (EMPLID, EMPL_RCD, EFFDT, EFFSEQ are all marked NOTNULL in XML)
    # ---------------------------------------------------------------
    try:
        actions_df = spark.table(f"{DATABASE}.nwk_new_ehrp_actions_tbl")
        total = actions_df.count()
        print(f"\n  nwk_new_ehrp_actions_tbl total rows: {total:,}")

        if total == 0:
            msg = "  [WARN] nwk_new_ehrp_actions_tbl is EMPTY - nothing to process."
            print(msg)
            errors.append(msg)
        else:
            # Null checks on key columns
            null_counts = actions_df.select(
                F.sum(F.when(F.col("EMPLID").isNull(), 1).otherwise(0)).alias("null_emplid"),
                F.sum(F.when(F.col("EMPL_RCD").isNull(), 1).otherwise(0)).alias("null_empl_rcd"),
                F.sum(F.when(F.col("EFFDT").isNull(), 1).otherwise(0)).alias("null_effdt"),
                F.sum(F.when(F.col("EFFSEQ").isNull(), 1).otherwise(0)).alias("null_effseq"),
            ).collect()[0]

            for col_name in ["null_emplid", "null_empl_rcd", "null_effdt", "null_effseq"]:
                val = null_counts[col_name]
                if val > 0:
                    msg = f"  [FAIL] {col_name.replace('null_', '').upper()} has {val} NULL rows"
                    print(msg)
                    errors.append(msg)
                else:
                    print(f"  [OK] {col_name.replace('null_', '').upper()} - no NULLs")

            # Check for duplicate keys
            dup_count = actions_df.groupBy("EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ") \
                .count() \
                .filter(F.col("count") > 1) \
                .count()
            if dup_count > 0:
                msg = f"  [WARN] {dup_count} duplicate key combinations in nwk_new_ehrp_actions_tbl"
                print(msg)
                errors.append(msg)
            else:
                print("  [OK] No duplicate key combinations")
    except Exception as e:
        msg = f"  [FAIL] Cannot read nwk_new_ehrp_actions_tbl: {e}"
        print(msg)
        errors.append(msg)

    # ---------------------------------------------------------------
    # 2. PS_GVT_JOB: verify join keys exist and table is accessible
    # ---------------------------------------------------------------
    try:
        gvt_job_df = spark.table(f"{DATABASE}.ps_gvt_job")
        gvt_count = gvt_job_df.count()
        print(f"\n  ps_gvt_job total rows: {gvt_count:,}")

        if gvt_count == 0:
            msg = "  [FAIL] ps_gvt_job is EMPTY"
            print(msg)
            errors.append(msg)
        else:
            print("  [OK] ps_gvt_job has data")
    except Exception as e:
        msg = f"  [FAIL] Cannot read ps_gvt_job: {e}"
        print(msg)
        errors.append(msg)

    # ---------------------------------------------------------------
    # 3. SEQUENCE_NUM_TBL: verify sequence numbers exist for current year
    #    (lkp_OLD_SEQUENCE_NUMBER needs EHRP_YEAR = current year)
    # ---------------------------------------------------------------
    try:
        current_year = datetime.now().year
        seq_df = spark.table(f"{DATABASE}.sequence_num_tbl")
        seq_count = seq_df.filter(F.col("EHRP_YEAR") == current_year).count()
        if seq_count == 0:
            msg = f"  [WARN] No sequence entry for year {current_year} in sequence_num_tbl"
            print(msg)
            errors.append(msg)
        else:
            seq_row = seq_df.filter(F.col("EHRP_YEAR") == current_year).collect()[0]
            print(f"  [OK] Sequence for year {current_year}: EHRP_SEQ_NUMBER = {seq_row['EHRP_SEQ_NUMBER']}")
    except Exception as e:
        msg = f"  [WARN] Cannot verify sequence_num_tbl: {e}"
        print(msg)
        errors.append(msg)

    # ---------------------------------------------------------------
    # 4. Verify source join will produce rows
    #    (SQ_PS_GVT_JOB Source Qualifier joins on EMPLID, EMPL_RCD, EFFDT, EFFSEQ)
    # ---------------------------------------------------------------
    try:
        join_preview_sql = f"""
            SELECT COUNT(*) AS join_count
            FROM {DATABASE}.ps_gvt_job j
            INNER JOIN {DATABASE}.nwk_new_ehrp_actions_tbl a
                ON a.EMPLID = j.EMPLID
                AND a.EMPL_RCD = j.EMPL_RCD
                AND a.EFFDT = j.EFFDT
                AND a.EFFSEQ = j.EFFSEQ
        """
        join_count = spark.sql(join_preview_sql).collect()[0]["join_count"]
        print(f"\n  Source join preview (SQ_PS_GVT_JOB): {join_count:,} matching rows")
        if join_count == 0:
            msg = "  [FAIL] Source join produces 0 rows - ETL will have nothing to process"
            print(msg)
            errors.append(msg)
        else:
            print("  [OK] Source join produces data")
    except Exception as e:
        msg = f"  [WARN] Cannot preview source join: {e}"
        print(msg)
        errors.append(msg)

    return errors


def validate_lookup_tables(spark):
    """
    Validate that lookup tables have data and expected key columns.
    Each lookup corresponds to an Informatica lkp_* transformation.
    """
    print("\n--- Lookup Table Validation ---")
    warnings = []

    lookup_checks = [
        # (table, key_columns, description, optional_filter)
        ("sequence_num_tbl", ["EHRP_YEAR"], "lkp_OLD_SEQUENCE_NUMBER", None),
        ("ps_gvt_employment", ["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"],
         "lkp_PS_GVT_EMPLOYMENT", None),
        ("ps_gvt_pers_nid", ["EMPLID"], "lkp_PS_GVT_PERS_NID", None),
        ("ps_gvt_awd_data", ["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"],
         "lkp_PS_GVT_AWD_DATA", None),
        ("ps_gvt_ee_data_trk", ["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"],
         "lkp_PS_GVT_EE_DATA_TRK", None),
        ("ps_he_fill_pos", ["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"],
         "lkp_PS_HE_FILL_POS", None),
        ("ps_gvt_citizenship", ["EMPLID"], "lkp_PS_GVT_CITIZENSHIP", None),
        ("ps_gvt_pers_data", ["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"],
         "lkp_PS_GVT_PERS_DATA", None),
        # lkp_PS_JPM_JP_ITEMS uses SQL override filtering jpm_cat_type='DEG' AND eff_status='A'
        ("ps_jpm_jp_items", ["JPM_PROFILE_ID"], "lkp_PS_JPM_JP_ITEMS",
         "jpm_cat_type = 'DEG' AND eff_status = 'A'"),
    ]

    for tbl, keys, lkp_name, where_clause in lookup_checks:
        try:
            df = spark.table(f"{DATABASE}.{tbl}")
            if where_clause:
                filtered = df.filter(where_clause)
                cnt = filtered.count()
                print(f"  {lkp_name} ({tbl}, filtered): {cnt:,} rows")
            else:
                cnt = df.count()
                print(f"  {lkp_name} ({tbl}): {cnt:,} rows")

            if cnt == 0:
                msg = f"  [WARN] {lkp_name} lookup table is empty"
                print(msg)
                warnings.append(msg)
        except Exception as e:
            msg = f"  [WARN] Cannot read {tbl} for {lkp_name}: {e}"
            print(msg)
            warnings.append(msg)

    return warnings


def validate_process_table(spark):
    """
    Check PROCESS_TABLE state, similar to the afterload P_STARTDT logic.
    This ensures the afterload script can operate correctly.
    """
    print("\n--- Process Table State ---")
    try:
        proc_df = spark.table(f"{DATABASE}.process_table")
        proc_rows = proc_df.collect()
        if len(proc_rows) == 0:
            print("  [WARN] process_table is empty - afterload may not function correctly")
        else:
            for row in proc_rows:
                print(f"  P_STARTDT = {row.get('P_STARTDT', 'N/A')}")
            print("  [OK] process_table has data")
    except Exception as e:
        print(f"  [WARN] Cannot check process_table: {e}")


def main():
    """
    Main entry point - runs all pre-load validations.
    Mirrors the ehrp2biis_preload ksh script flow:
      1. Environment setup (SETENV equivalent)
      2. Oracle connection + SQL execution (step01 equivalent)
      3. Error checking (grep ERROR equivalent)
      4. Notification (mailx equivalent -> print/log)
    """
    print(f"\n{'#' * 70}")
    print("# EHRP2BIIS UPDATE - Pre-Load Validation")
    print(f"# Run Date: {LOAD_DATE}  |  Load ID: {LOAD_ID}")
    print(f"{'#' * 70}\n")

    spark = create_spark_session()
    all_errors = []

    try:
        # Step 1: Environment validation (SETENV + Oracle connect equivalent)
        validate_environment(spark)

        # Step 2: Table existence checks
        check_table_existence(spark)

        # Step 3: Row count baselines
        capture_row_count_baselines(spark)

        # Step 4: Source data quality (step01 SQL equivalent)
        dq_errors = validate_source_data_quality(spark)
        all_errors.extend(dq_errors)

        # Step 5: Lookup table validation
        validate_lookup_tables(spark)

        # Step 6: Process table state check
        validate_process_table(spark)

    except Exception as e:
        all_errors.append(f"CRITICAL: {e}")
        print(f"\n[CRITICAL] Pre-load validation failed: {e}")

    # ---------------------------------------------------------------
    # Final summary (equivalent to ksh error-check + mailx notification)
    # ---------------------------------------------------------------
    print(f"\n{'=' * 70}")
    print("PRE-LOAD VALIDATION SUMMARY")
    print(f"{'=' * 70}")

    if all_errors:
        print(f"\n[RESULT] Validation completed with {len(all_errors)} error(s):")
        for i, err in enumerate(all_errors, 1):
            print(f"  {i}. {err}")
        # Original ksh: mailx -s "EHRP2BIIS Preload script did not complete successfully"
        print("\n[ACTION] Review errors above before proceeding with ETL.")
        print("         Equivalent to: 'EHRP2BIIS Preload script did not complete successfully'")
    else:
        # Original ksh: mailx -s "EHRP2BIIS Preload script completed successfully"
        print("\n[RESULT] All validations PASSED.")
        print("         Equivalent to: 'EHRP2BIIS Preload script completed successfully'")
        print("         Equivalent to: 'Action Staging Records load completed successfully'")

    print(f"\nPre-load validation finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 70}")

    return len(all_errors) == 0


if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1)
