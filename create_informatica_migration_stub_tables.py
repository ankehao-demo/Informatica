"""
create_informatica_migration_stub_tables.py
=============================================
Creates stub/test tables in the informatica_migration database for validation
of the EHRP2BIIS_UPDATE pipeline.

Purpose:
  Session 7 (INVOKE - Run EHRP2BIIS_UPDATE Job) failed because the
  pre_load notebook could not find the required tables.
  This notebook creates all 18 required tables with correct schemas
  (derived from the Informatica PowerCenter XML export) and inserts
  minimal synthetic test data so the pipeline can run end-to-end.

Tables created (18 total):
  Source tables (2):
    - nwk_new_ehrp_actions_tbl
    - ps_gvt_job

  Lookup tables (9):
    - sequence_num_tbl
    - ps_gvt_employment
    - ps_gvt_pers_nid
    - ps_gvt_awd_data
    - ps_gvt_ee_data_trk
    - ps_he_fill_pos
    - ps_gvt_citizenship
    - ps_gvt_pers_data
    - ps_jpm_jp_items

  Target tables (3):
    - nwk_action_primary_tbl
    - nwk_action_secondary_tbl
    - ehrp_recs_tracking_tbl

  Permanent / post-load tables (4):
    - action_primary_all
    - action_secondary_all
    - action_remarks_all
    - process_table

Usage:
  Upload to Databricks workspace and run as a notebook, or execute via:
    databricks workspace import --overwrite --language PYTHON \\
        --file create_informatica_migration_stub_tables.py \\
        /pipelines/EHRP2BIIS_UPDATE/create_informatica_migration_stub_tables
    databricks runs submit --json '{"run_name":"create_stub_tables",
        "notebook_task":{"notebook_path":
        "/pipelines/EHRP2BIIS_UPDATE/create_informatica_migration_stub_tables"}}'
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, DateType,
)
from datetime import datetime, date

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATABASE = "informatica_migration"

# Test data constants - a single synthetic employee record
TEST_EMPLID = "T00000001"
TEST_EMPL_RCD = 0
TEST_EFFDT = date(2026, 4, 1)
TEST_EFFSEQ = 0
CURRENT_YEAR = datetime.now().year


def create_spark_session():
    """Create Spark session with Delta and Hive support."""
    spark = SparkSession.builder \
        .appName("create_informatica_migration_stub_tables") \
        .config("spark.sql.sources.default", "delta") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


def ensure_database(spark):
    """Create the hhs_migration database if it does not exist."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    spark.sql(f"USE {DATABASE}")
    print(f"[OK] Database '{DATABASE}' ready.")


# ============================================================================
# Source Tables
# ============================================================================

def create_nwk_new_ehrp_actions_tbl(spark):
    """
    Source table: NWK_NEW_EHRP_ACTIONS_TBL
    Schema from XML: 4 columns (EMPLID, EMPL_RCD, EFFDT, EFFSEQ)
    This is the action staging table that drives the pipeline.
    """
    table = f"{DATABASE}.nwk_new_ehrp_actions_tbl"
    print(f"\n--- Creating {table} ---")

    schema = StructType([
        StructField("EMPLID", StringType(), False),
        StructField("EMPL_RCD", IntegerType(), False),
        StructField("EFFDT", DateType(), False),
        StructField("EFFSEQ", IntegerType(), False),
    ])

    data = [
        (TEST_EMPLID, TEST_EMPL_RCD, TEST_EFFDT, TEST_EFFSEQ),
        ("T00000002", 0, TEST_EFFDT, 0),
    ]

    df = spark.createDataFrame(data, schema)
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table)
    print(f"  [OK] {table}: {df.count()} rows")


def create_ps_gvt_job(spark):
    """
    Source table: PS_GVT_JOB
    Schema from XML: 155 columns from Oracle EHRP schema.
    The SQ_PS_GVT_JOB Source Qualifier joins this with NWK_NEW_EHRP_ACTIONS_TBL
    on EMPLID, EMPL_RCD, EFFDT, EFFSEQ.

    We create the full set of columns referenced by the ETL notebook.
    """
    table = f"{DATABASE}.ps_gvt_job"
    print(f"\n--- Creating {table} ---")

    # Build rows as dicts for readability - includes all columns used by ETL
    row1 = {
        # Join keys
        "EMPLID": TEST_EMPLID,
        "EMPL_RCD": TEST_EMPL_RCD,
        "EFFDT": TEST_EFFDT,
        "EFFSEQ": TEST_EFFSEQ,
        # Columns used in ETL (from exp_MAIN2BIIS and write targets)
        "DEPTID": "HHS00100",
        "JOBCODE": "0301",
        "POSITION_NBR": "00012345",
        "POSITION_OVERRIDE": "N",
        "POSN_CHANGE_RECORD": "N",
        "EMPL_STATUS": "A",
        "ACTION": "PAY",
        "ACTION_DT": TEST_EFFDT,
        "ACTION_REASON": "SAL",
        "LOCATION": "110100000",
        "TAX_LOCATION_CD": "DC",
        "JOB_ENTRY_DT": date(2025, 1, 1),
        "DEPT_ENTRY_DT": date(2025, 1, 1),
        "POSITION_ENTRY_DT": date(2025, 1, 1),
        "SHIFT": "N",
        "REG_TEMP": "R",
        "FULL_PART_TIME": "F",
        "COMPANY": "HHS",
        "PAYGROUP": "MN1",
        "STD_HOURS": 80.0,
        "SAL_ADMIN_PLAN": "GS",
        "GRADE": "13",
        "GRADE_ENTRY_DT": date(2024, 1, 1),
        "STEP": 5,
        "STEP_ENTRY_DT": date(2024, 6, 1),
        "COMPRATE": 112015.000000,
        "ANNUAL_RT": 112015.000,
        "MONTHLY_RT": 9334.580,
        "HOURLY_RT": 53.853365,
        "ANNL_BENEF_BASE_RT": 112015.000,
        "CURRENCY_CD": "USD",
        "BUSINESS_UNIT": "HHS01",
        "SETID_DEPT": "HHS01",
        "SETID_JOBCODE": "HHS01",
        "SETID_LOCATION": "HHS01",
        "SETID_SALARY": "HHS01",
        "FLSA_STATUS": "E",
        "GVT_WIP_STATUS": "APR",
        "GVT_STATUS_TYPE": "CUR",
        "GVT_NOA_CODE": "894",
        "GVT_LEG_AUTH_1": "ZLM",
        "GVT_PAR_AUTH_D1": "Reg 531.XXX ",
        "GVT_PAR_AUTH_D1_2": "",
        "GVT_LEG_AUTH_2": "",
        "GVT_PAR_AUTH_D2": "",
        "GVT_PAR_AUTH_D2_2": "",
        "GVT_WORK_SCHED": "F",
        "GVT_SUB_AGENCY": "00",
        "GVT_PAY_RATE_DETER": "0",
        "GVT_STEP": "05",
        "GVT_RTND_PAY_PLAN": "",
        "GVT_RTND_GRADE": "",
        "GVT_RTND_STEP": 0,
        "GVT_PAY_BASIS": "PA",
        "GVT_COMPRATE": 112015.000000,
        "GVT_LOCALITY_ADJ": 32817.00,
        "GVT_HRLY_RT_NO_LOC": 53.853365,
        "GVT_XFER_FROM_AGCY": "",
        "GVT_RETIRE_PLAN": "K",
        "GVT_ANN_IND": "N",
        "GVT_FEGLI": "BA",
        "GVT_ANNUITY_OFFSET": 0.00,
        "GVT_CSRS_FROZN_SVC": "0000",
        "GVT_PREV_RET_COVRG": "5",
        "GVT_FERS_COVERAGE": "6",
        "GVT_TYPE_OF_APPT": "10",
        "GVT_POI": "1101",
        "GVT_POSN_OCCUPIED": "1",
        "GVT_LEO_POSITION": "0",
        "GVT_PAY_PLAN": "GS",
        "UNION_CD": "",
        "BARG_UNIT": "8888",
        "HE_NOA_EXT": "0",
        "HE_AL_CARRYOVER": 80.00,
        "HE_AL_ACCRUAL": 40.00,
        "HE_AL_RED_CRED": 0.00,
        "HE_AL_TOTAL": 16.00,
        "HE_AL_BALANCE": 104.00,
        "HE_SL_CARRYOVER": 520.00,
        "HE_SL_ACCRUAL": 40.00,
        "HE_SL_RED_CRED": 0.00,
        "HE_SL_TOTAL": 8.00,
        "HE_SL_BALANCE": 552.00,
        "HE_RES_LASTYR": 0.00,
        "HE_RES_TWOYRS": 0.00,
        "HE_RES_THREEYRS": 0.00,
        "HE_RES_BALANCE": 0.00,
        "HE_LUMP_HRS": 0,
        "HE_AWOP_SEP": 0.00,
        "HE_AWOP_WIGI": 0.00,
        "HE_REG_MILITARY": 0,
        "HE_SPC_MILITARY": 0,
        "HE_FROZEN_SL": 0.00,
        "HE_TSPA_SUB_YR": 0.00,
        "HE_PP_UDED_AMT": 0.00,
        "HE_EMP_UDED_AMT": 0.00,
        "HE_GVT_UDED_AMT": 0.00,
        "HE_NO_TSP_PAYPER": 0,
        "HE_TLTR_NO": 0,
        "HE_UDED_PAY_CD": "",
        "HE_TSP_CANC_CD": "",
        "REPORTS_TO": "00054321",
        "SUPERVISOR_ID": "S00000001",
    }

    row2 = dict(row1)
    row2["EMPLID"] = "T00000002"

    df = spark.createDataFrame([row1, row2])
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table)
    print(f"  [OK] {table}: {df.count()} rows")


# ============================================================================
# Lookup Tables
# ============================================================================

def create_sequence_num_tbl(spark):
    """
    Lookup table: SEQUENCE_NUM_TBL
    Used by lkp_OLD_SEQUENCE_NUMBER (join on EHRP_YEAR = current year).
    Returns EHRP_SEQ_NUMBER (the starting event ID for the year).
    """
    table = f"{DATABASE}.sequence_num_tbl"
    print(f"\n--- Creating {table} ---")

    schema = StructType([
        StructField("EHRP_YEAR", IntegerType(), False),
        StructField("EHRP_SEQ_NUMBER", LongType(), False),
    ])

    data = [(CURRENT_YEAR, 1000000)]
    df = spark.createDataFrame(data, schema)
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table)
    print(f"  [OK] {table}: {df.count()} rows")


def create_ps_gvt_employment(spark):
    """
    Lookup table: PS_GVT_EMPLOYMENT
    Used by lkp_PS_GVT_EMPLOYMENT (join on EMPLID, EMPL_RCD, EFFDT, EFFSEQ).
    Returns hire dates, SCD dates, BUSINESS_TITLE, etc.
    """
    table = f"{DATABASE}.ps_gvt_employment"
    print(f"\n--- Creating {table} ---")

    schema = StructType([
        StructField("EMPLID", StringType(), False),
        StructField("EMPL_RCD", IntegerType(), False),
        StructField("EFFDT", DateType(), False),
        StructField("EFFSEQ", IntegerType(), False),
        StructField("HIRE_DT", DateType(), True),
        StructField("REHIRE_DT", DateType(), True),
        StructField("CMPNY_SENIORITY_DT", DateType(), True),
        StructField("SERVICE_DT", DateType(), True),
        StructField("TERMINATION_DT", DateType(), True),
        StructField("BUSINESS_TITLE", StringType(), True),
        StructField("PROBATION_DT", DateType(), True),
        StructField("SETID", StringType(), True),
        StructField("GVT_SCD_RETIRE", DateType(), True),
        StructField("GVT_SCD_TSP", DateType(), True),
        StructField("GVT_SCD_LEO", DateType(), True),
        StructField("GVT_SCD_SEVPAY", DateType(), True),
        StructField("GVT_SEVPAY_PRV_WKS", IntegerType(), True),
        StructField("GVT_WGI_STATUS", StringType(), True),
        StructField("GVT_WGI_DUE_DATE", DateType(), True),
        StructField("GVT_TEMP_PRO_EXPIR", DateType(), True),
        StructField("GVT_SABBATIC_EXPIR", DateType(), True),
        StructField("GVT_CURR_APT_AUTH1", StringType(), True),
        StructField("GVT_CURR_APT_AUTH2", StringType(), True),
        StructField("GVT_RTND_GRADE_BEG", DateType(), True),
        StructField("GVT_RTND_GRADE_EXP", DateType(), True),
        StructField("TEMP_GVT_EFFDT", DateType(), True),
        StructField("OTH_PAY", DoubleType(), True),
        StructField("GVT_CNV_BEGIN_DATE", DateType(), True),
        StructField("GVT_TEMP_PSN_EXPIR", DateType(), True),
        StructField("GVT_APPT_LIMIT_DYS", IntegerType(), True),
        StructField("GVT_SPEP", IntegerType(), True),
        StructField("GVT_SUPV_PROB_DT", DateType(), True),
        StructField("GVT_APPT_EXPIR_DT", DateType(), True),
        StructField("LAST_INCREASE_DT", DateType(), True),
        StructField("GVT_COMP_LVL_PERM", StringType(), True),
        StructField("GVT_DETAIL_EXPIRES", DateType(), True),
        StructField("GVT_TENURE", StringType(), True),
    ])

    data = [
        (TEST_EMPLID, TEST_EMPL_RCD, TEST_EFFDT, TEST_EFFSEQ,
         date(2020, 3, 15), None, date(2020, 3, 15), date(2020, 3, 15),
         None, "IT Specialist", date(2021, 3, 15), "HHS01",
         date(2020, 3, 15), date(2020, 3, 15), None, date(2020, 3, 15),
         0, "E", date(2027, 3, 15), None, None, "ZLM", "",
         None, None, None, 0.0, date(2020, 3, 15),
         None, 0, 0, None, None, date(2025, 3, 15),
         "0001", None, "1"),
        ("T00000002", 0, TEST_EFFDT, 0,
         date(2022, 7, 1), None, date(2022, 7, 1), date(2022, 7, 1),
         None, "Program Analyst", date(2023, 7, 1), "HHS01",
         date(2022, 7, 1), date(2022, 7, 1), None, date(2022, 7, 1),
         0, "E", date(2027, 7, 1), None, None, "ZLM", "",
         None, None, None, 0.0, date(2022, 7, 1),
         None, 0, 0, None, None, date(2025, 7, 1),
         "0002", None, "1"),
    ]

    df = spark.createDataFrame(data, schema)
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table)
    print(f"  [OK] {table}: {df.count()} rows")


def create_ps_gvt_pers_nid(spark):
    """
    Lookup table: PS_GVT_PERS_NID
    Used by lkp_PS_GVT_PERS_NID (join on EMPLID, EMPL_RCD, EFFDT, EFFSEQ).
    Returns NATIONAL_ID (SSN).
    """
    table = f"{DATABASE}.ps_gvt_pers_nid"
    print(f"\n--- Creating {table} ---")

    schema = StructType([
        StructField("EMPLID", StringType(), False),
        StructField("EMPL_RCD", IntegerType(), False),
        StructField("EFFDT", DateType(), False),
        StructField("EFFSEQ", IntegerType(), False),
        StructField("NATIONAL_ID", StringType(), True),
    ])

    data = [
        (TEST_EMPLID, TEST_EMPL_RCD, TEST_EFFDT, TEST_EFFSEQ, "000-00-0001"),
        ("T00000002", 0, TEST_EFFDT, 0, "000-00-0002"),
    ]

    df = spark.createDataFrame(data, schema)
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table)
    print(f"  [OK] {table}: {df.count()} rows")


def create_ps_gvt_awd_data(spark):
    """
    Lookup table: PS_GVT_AWD_DATA
    Used by lkp_PS_GVT_AWD_DATA (join on EMPLID, EMPL_RCD, EFFDT, EFFSEQ).
    Returns OTH_PAY, OTH_HRS, GOAL_AMT, EARNINGS_END_DT, ERNCD, etc.
    """
    table = f"{DATABASE}.ps_gvt_awd_data"
    print(f"\n--- Creating {table} ---")

    schema = StructType([
        StructField("EMPLID", StringType(), False),
        StructField("EMPL_RCD", IntegerType(), False),
        StructField("EFFDT", DateType(), False),
        StructField("EFFSEQ", IntegerType(), False),
        StructField("OTH_PAY", DoubleType(), True),
        StructField("OTH_HRS", DoubleType(), True),
        StructField("GOAL_AMT", DoubleType(), True),
        StructField("EARNINGS_END_DT", DateType(), True),
        StructField("GVT_TANG_BEN_AMT", DoubleType(), True),
        StructField("GVT_INTANG_BEN_AMT", DoubleType(), True),
        StructField("ERNCD", StringType(), True),
    ])

    data = [
        (TEST_EMPLID, TEST_EMPL_RCD, TEST_EFFDT, TEST_EFFSEQ,
         0.0, 0.0, 0.0, None, 0.0, 0.0, ""),
        ("T00000002", 0, TEST_EFFDT, 0,
         0.0, 0.0, 0.0, None, 0.0, 0.0, ""),
    ]

    df = spark.createDataFrame(data, schema)
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table)
    print(f"  [OK] {table}: {df.count()} rows")


def create_ps_gvt_ee_data_trk(spark):
    """
    Lookup table: PS_GVT_EE_DATA_TRK
    Used by lkp_PS_GVT_EE_DATA_TRK with SQL Override that selects DISTINCT.
    Returns GVT_DATE_WRK.
    """
    table = f"{DATABASE}.ps_gvt_ee_data_trk"
    print(f"\n--- Creating {table} ---")

    schema = StructType([
        StructField("EMPLID", StringType(), False),
        StructField("EMPL_RCD", IntegerType(), False),
        StructField("EFFDT", DateType(), False),
        StructField("EFFSEQ", IntegerType(), False),
        StructField("GVT_WIP_STATUS", StringType(), True),
        StructField("GVT_DATE_WRK", DateType(), True),
    ])

    data = [
        (TEST_EMPLID, TEST_EMPL_RCD, TEST_EFFDT, TEST_EFFSEQ, "APR", TEST_EFFDT),
        ("T00000002", 0, TEST_EFFDT, 0, "APR", TEST_EFFDT),
    ]

    df = spark.createDataFrame(data, schema)
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table)
    print(f"  [OK] {table}: {df.count()} rows")


def create_ps_he_fill_pos(spark):
    """
    Lookup table: PS_HE_FILL_POS
    Used by lkp_PS_HE_FILL_POS (join on EMPLID, EMPL_RCD, EFFDT, EFFSEQ).
    Returns HE_FILL_POSITION.
    """
    table = f"{DATABASE}.ps_he_fill_pos"
    print(f"\n--- Creating {table} ---")

    schema = StructType([
        StructField("EMPLID", StringType(), False),
        StructField("EMPL_RCD", IntegerType(), False),
        StructField("EFFDT", DateType(), False),
        StructField("EFFSEQ", IntegerType(), False),
        StructField("HE_FILL_POSITION", StringType(), True),
    ])

    data = [
        (TEST_EMPLID, TEST_EMPL_RCD, TEST_EFFDT, TEST_EFFSEQ, "Y"),
        ("T00000002", 0, TEST_EFFDT, 0, "N"),
    ]

    df = spark.createDataFrame(data, schema)
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table)
    print(f"  [OK] {table}: {df.count()} rows")


def create_ps_gvt_citizenship(spark):
    """
    Lookup table: PS_GVT_CITIZENSHIP
    Used by lkp_PS_GVT_CITIZENSHIP (join on EMPLID, EMPL_RCD, EFFDT, EFFSEQ).
    Returns CITIZENSHIP_STATUS.
    """
    table = f"{DATABASE}.ps_gvt_citizenship"
    print(f"\n--- Creating {table} ---")

    schema = StructType([
        StructField("EMPLID", StringType(), False),
        StructField("EMPL_RCD", IntegerType(), False),
        StructField("EFFDT", DateType(), False),
        StructField("EFFSEQ", IntegerType(), False),
        StructField("CITIZENSHIP_STATUS", StringType(), True),
    ])

    data = [
        (TEST_EMPLID, TEST_EMPL_RCD, TEST_EFFDT, TEST_EFFSEQ, "1"),
        ("T00000002", 0, TEST_EFFDT, 0, "1"),
    ]

    df = spark.createDataFrame(data, schema)
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table)
    print(f"  [OK] {table}: {df.count()} rows")


def create_ps_gvt_pers_data(spark):
    """
    Lookup table: PS_GVT_PERS_DATA
    Used by lkp_PS_GVT_PERS_DATA with SQL Override joining on
    EMPLID, EMPL_RCD, EFFDT, EFFSEQ.
    Returns personal data: names, address, demographics.
    """
    table = f"{DATABASE}.ps_gvt_pers_data"
    print(f"\n--- Creating {table} ---")

    data = [
        {
            "EMPLID": TEST_EMPLID,
            "EMPL_RCD": TEST_EMPL_RCD,
            "EFFDT": TEST_EFFDT,
            "EFFSEQ": TEST_EFFSEQ,
            "LAST_NAME": "TESTLAST",
            "FIRST_NAME": "TESTFIRST",
            "MIDDLE_NAME": "M",
            "ADDRESS1": "123 Test Street",
            "CITY": "Washington",
            "STATE": "DC",
            "POSTAL": "20001",
            "GEO_CODE": "1100000000",
            "SEX": "M",
            "BIRTHDATE": date(1985, 6, 15),
            "MILITARY_STATUS": "N",
            "GVT_CRED_MIL_SVCE": "00000000",
            "GVT_MILITARY_COMP": "",
            "GVT_MIL_RESRVE_CAT": "",
            "GVT_VET_PREF_APPT": "0",
            "ETHNIC_GROUP": "WH",
            "GVT_DISABILITY_CD": "05",
        },
        {
            "EMPLID": "T00000002",
            "EMPL_RCD": 0,
            "EFFDT": TEST_EFFDT,
            "EFFSEQ": 0,
            "LAST_NAME": "TESTLAST2",
            "FIRST_NAME": "TESTFIRST2",
            "MIDDLE_NAME": "A",
            "ADDRESS1": "456 Test Ave",
            "CITY": "Bethesda",
            "STATE": "MD",
            "POSTAL": "20814",
            "GEO_CODE": "2400000000",
            "SEX": "F",
            "BIRTHDATE": date(1990, 11, 20),
            "MILITARY_STATUS": "N",
            "GVT_CRED_MIL_SVCE": "00000000",
            "GVT_MILITARY_COMP": "",
            "GVT_MIL_RESRVE_CAT": "",
            "GVT_VET_PREF_APPT": "0",
            "ETHNIC_GROUP": "AS",
            "GVT_DISABILITY_CD": "05",
        },
    ]

    df = spark.createDataFrame(data)
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table)
    print(f"  [OK] {table}: {df.count()} rows")


def create_ps_jpm_jp_items(spark):
    """
    Lookup table: PS_JPM_JP_ITEMS
    Used by lkp_PS_JPM_JP_ITEMS with SQL Override:
      WHERE jpm_cat_type = 'DEG' AND eff_status = 'A'
    Join on JPM_PROFILE_ID = EMPLID.
    Returns JPM_CAT_ITEM_ID (degree code), JPM_INTEGER_2 (year), MAJOR_CODE.
    """
    table = f"{DATABASE}.ps_jpm_jp_items"
    print(f"\n--- Creating {table} ---")

    data = [
        {
            "JPM_PROFILE_ID": TEST_EMPLID,
            "JPM_CAT_TYPE": "DEG",
            "EFF_STATUS": "A",
            "JPM_CAT_ITEM_ID": "MA",
            "JPM_INTEGER_2": 2018,
            "MAJOR_CODE": "1101",
        },
        {
            "JPM_PROFILE_ID": "T00000002",
            "JPM_CAT_TYPE": "DEG",
            "EFF_STATUS": "A",
            "JPM_CAT_ITEM_ID": "BA",
            "JPM_INTEGER_2": 2012,
            "MAJOR_CODE": "4501",
        },
    ]

    df = spark.createDataFrame(data)
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table)
    print(f"  [OK] {table}: {df.count()} rows")


# ============================================================================
# Target Tables (empty with correct schema for ETL writes)
# ============================================================================

def create_nwk_action_primary_tbl(spark):
    """
    Target table: NWK_ACTION_PRIMARY_TBL
    260 columns in the XML definition.
    Created empty - the ETL notebook appends rows here.
    """
    table = f"{DATABASE}.nwk_action_primary_tbl"
    print(f"\n--- Creating {table} ---")

    schema = StructType([
        StructField("EVENT_ID", LongType(), False),
        StructField("AGCY_ASSIGN_CD", StringType(), True),
        StructField("AGCY_SUBELEMENT_CD", StringType(), True),
        StructField("AGCY_SUBELEMENT_PRIOR_CD", StringType(), True),
        StructField("ANN_LV_CRDT_REDUCTN_HRS", DoubleType(), True),
        StructField("ANN_LV_CUR_BAL_HRS", DoubleType(), True),
        StructField("ANN_LV_LUMP_SUM_PAID_HRS", DoubleType(), True),
        StructField("ANN_LV_PRIOR_YEAR_BAL_HRS", DoubleType(), True),
        StructField("ANN_LV_RESTORED_BAL_LV_HRS", DoubleType(), True),
        StructField("ANN_LV_RESTORED_BAL1_HRS", DoubleType(), True),
        StructField("ANN_LV_RESTORED_BAL2_HRS", DoubleType(), True),
        StructField("ANN_LV_RESTORED_BAL3_HRS", DoubleType(), True),
        StructField("ANN_LV_YTD_ACCRD_HRS", DoubleType(), True),
        StructField("ANN_LV_YTD_USED_HRS", DoubleType(), True),
        StructField("ANN_SALARY_RATE_AMT", DoubleType(), True),
        StructField("ANNUITANT_IND_CD", StringType(), True),
        StructField("APPT_LMT_NTE_HRS", DoubleType(), True),
        StructField("APPT_NTE_DTE", DateType(), True),
        StructField("APPT_TYPE_CD", StringType(), True),
        StructField("BARGAINING_UNIT_CD", StringType(), True),
        StructField("BASE_HRS", DoubleType(), True),
        StructField("BIRTH_DTE", DateType(), True),
        StructField("CASH_AWARD_AMT", DoubleType(), True),
        StructField("CASH_AWARD_BNFT_AMT", DoubleType(), True),
        StructField("DUTY_STATION_CD", StringType(), True),
        StructField("EMP_FIRST_NAME", StringType(), True),
        StructField("EMP_LAST_NAME", StringType(), True),
        StructField("EMP_MID_INIT", StringType(), True),
        StructField("EMP_RESID_CITY_ST_NAME", StringType(), True),
        StructField("EMP_RESID_GEOGPHCL_LOC_CD", StringType(), True),
        StructField("EMP_RESID_POSTAL_CD", StringType(), True),
        StructField("EMP_RESID_STREET_NAME", StringType(), True),
        StructField("EVENT_EFF_DTE", DateType(), True),
        StructField("EVENT_SUBMITTED_DTE", DateType(), True),
        StructField("FEGLI_LIVING_BNFT_CD", StringType(), True),
        StructField("FEGLI_CD", StringType(), True),
        StructField("FERS_COV_CD", StringType(), True),
        StructField("FLSA_CATGRY_CD", StringType(), True),
        StructField("FROZEN_SERVICE_PERIOD", StringType(), True),
        StructField("GRADE_CD", StringType(), True),
        StructField("HANDICAP_CD", StringType(), True),
        StructField("HRLY_RATE_AMT", DoubleType(), True),
        StructField("LEGAL_AUTH_CD", StringType(), True),
        StructField("LEGAL_AUTH_TXT", StringType(), True),
        StructField("LEGAL_AUTH2_CD", StringType(), True),
        StructField("LEGAL_AUTH2_TXT", StringType(), True),
        StructField("LOCALITY_PAY_AMT", DoubleType(), True),
        StructField("LWOP_AWOP_WGI_HRS", DoubleType(), True),
        StructField("NOA_CD", StringType(), True),
        StructField("NOA_SUFFIX_CD", StringType(), True),
        StructField("OCCUPATION_CD", StringType(), True),
        StructField("PAY_BASIS_CD", StringType(), True),
        StructField("PAY_PLAN_CD", StringType(), True),
        StructField("PERMANENT_TEMP_POSITION_CD", StringType(), True),
        StructField("PERSONNEL_OFFICE_ID_CD", StringType(), True),
        StructField("POSITION_NUM", StringType(), True),
        StructField("POSITION_OCCUPIED_CD", StringType(), True),
        StructField("POSITION_TITLE_NAME", StringType(), True),
        StructField("PRD_CD", StringType(), True),
        StructField("PREV_RETMT_COV_CD", StringType(), True),
        StructField("PROB_TRIAL_PERIOD_START_DTE", DateType(), True),
        StructField("RACE_NATL_ORIGIN_CD", StringType(), True),
        StructField("RECRUITMENT_BONUS_AMT", DoubleType(), True),
        StructField("RECRUITMENT_EXP_DTE", DateType(), True),
        StructField("RELOCATION_BONUS_AMT", DoubleType(), True),
        StructField("RELOCATION_EXP_DTE", DateType(), True),
        StructField("RETMT_PLAN_CD", StringType(), True),
        StructField("SCHLD_ANN_SALARY_AMT", DoubleType(), True),
        StructField("SCHLD_HRLY_RATE_AMT", DoubleType(), True),
        StructField("SEVERANCE_PAY_AMT", DoubleType(), True),
        StructField("SEVERANCE_PAY_START_DTE", DateType(), True),
        StructField("SEVERANCE_PAY_TOT_AMT", DoubleType(), True),
        StructField("SEX_CD", StringType(), True),
        StructField("SICK_LV_CRDT_REDUCTN_HRS", DoubleType(), True),
        StructField("SICK_LV_CUR_BAL_HRS", DoubleType(), True),
        StructField("SICK_LV_FERS_ELECT_BAL_HRS", DoubleType(), True),
        StructField("SICK_LV_PRIOR_YEAR_BAL_HRS", DoubleType(), True),
        StructField("SICK_LV_YTD_ACCRD_HRS", DoubleType(), True),
        StructField("SICK_LV_YTD_USED_HRS", DoubleType(), True),
        StructField("SSN", StringType(), True),
        StructField("STEP_CD", StringType(), True),
        StructField("TIME_OFF_AWARD_AMT", DoubleType(), True),
        StructField("TIME_OFF_GRANTED_HRS", DoubleType(), True),
        StructField("UNION_CD", StringType(), True),
        StructField("US_CITIZENSHIP_CD", StringType(), True),
        StructField("VETERANS_PREFERENCE_CD", StringType(), True),
        StructField("VETERANS_STATUS_CD", StringType(), True),
        StructField("WORK_SCHEDULE_CD", StringType(), True),
        StructField("YEAR_DEGREE_ATTAINED_DTE", IntegerType(), True),
        StructField("LOAD_ID", StringType(), True),
        StructField("LOAD_DATE", DateType(), True),
        StructField("EFFSEQ", StringType(), True),
        StructField("EMPL_REC_NO", IntegerType(), True),
        StructField("EHRP_TYPE_ACTION", StringType(), True),
        StructField("EHRP_ACTION_REASON", StringType(), True),
        StructField("GVT_WIP_STATUS", StringType(), True),
        StructField("GVT_STATUS_TYPE", StringType(), True),
        StructField("EHRP_POSITION_NUMBER", StringType(), True),
        StructField("CURR_APPT_AUTH1_CD", StringType(), True),
        StructField("CURR_APPT_AUTH2_CD", StringType(), True),
        StructField("LEO_POSITION_CD", StringType(), True),
        StructField("REPORTS_TO", StringType(), True),
        StructField("POSITION_ENTRY_DT", DateType(), True),
        StructField("SETID", StringType(), True),
        StructField("FEGLI_LIVING_BNFT_REMAIN_AMT", DoubleType(), True),
        StructField("EMP_EOD_DTE", DateType(), True),
        StructField("RETMT_SCD_DTE", DateType(), True),
        StructField("TSP_SCD_DTE", DateType(), True),
        StructField("CAREER_START_DTE", DateType(), True),
        StructField("LEO_SCD_DT", DateType(), True),
        StructField("EMP_GRADE_START_DTE", DateType(), True),
        StructField("WGI_START_DTE", DateType(), True),
        StructField("BUYOUT_AMT", DoubleType(), True),
        StructField("BUYOUT_EFF_DTE", DateType(), True),
        StructField("LWOP_START_DTE", DateType(), True),
        StructField("POSITION_CHANGE_END_DTE", DateType(), True),
        StructField("APPT_LMT_NTE_90DAY_CD", StringType(), True),
        StructField("SPECIAL_PROGRAM_CD", IntegerType(), True),
        StructField("SUPERVSRY_MGRL_PROB_START_DTE", DateType(), True),
        StructField("AWOP_WGI_START_DTE", DateType(), True),
        StructField("COMPETITIVE_LEVEL_CD", StringType(), True),
        StructField("SUSPENSION_END_DTE", DateType(), True),
        StructField("TEMP_PROMTN_EXP_DTE", DateType(), True),
        StructField("TENURE_CD", StringType(), True),
        StructField("WGI_STATUS_CD", StringType(), True),
        StructField("LV_SCD_DTE", DateType(), True),
        StructField("FILLING_POSITION_CD", StringType(), True),
        StructField("TSP_VESTING_CD", StringType(), True),
        StructField("TSP_EMP_PP_UND_DED_AMT", DoubleType(), True),
        StructField("TSP_EMP_PP_UND_DED_PYMT_COUNT", IntegerType(), True),
        StructField("TSP_PAY_SUBJ_YTD_AMT", DoubleType(), True),
        StructField("TSP_EMP_UND_DED_OUTSTNDG_AMT", DoubleType(), True),
        StructField("TSP_GOVT_UND_DED_OUTSTNDG_AMT", DoubleType(), True),
        StructField("TSP_UND_DED_LTR_NUM", IntegerType(), True),
        StructField("TSP_UND_DED_OPTION_CD", StringType(), True),
        StructField("TSP_UND_DED_STOP_OPTION_CD", StringType(), True),
        StructField("MIL_SRVC_BRANCH_CD", StringType(), True),
        StructField("MIL_SRVC_BRANCH_COMPONENT_CD", StringType(), True),
        StructField("CRDTBL_MIL_SRVC_PERIOD", StringType(), True),
        StructField("MIL_LV_CUR_FY_HRS", IntegerType(), True),
        StructField("MIL_LV_EMERG_CUR_FY_HRS", IntegerType(), True),
        StructField("AWOP_YTD_HRS", DoubleType(), True),
        StructField("RETND1_PAY_PLAN_CD", StringType(), True),
        StructField("RETND1_GRADE_CD", StringType(), True),
        StructField("RETND1_STEP_CD", StringType(), True),
        StructField("SAL_ADMIN_PLAN", StringType(), True),
        StructField("PAYGROUP", StringType(), True),
        StructField("LINE_SEQ", StringType(), True),
        StructField("REG_TEMP", StringType(), True),
        StructField("AUTHENTICATION_DTE", DateType(), True),
        StructField("EDUCATION_LEVEL_CD", StringType(), True),
        StructField("INSTRUCTIONAL_PROGRAM_CD", StringType(), True),
    ])

    empty_df = spark.createDataFrame([], schema)
    empty_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table)
    print(f"  [OK] {table}: created (empty)")


def create_nwk_action_secondary_tbl(spark):
    """
    Target table: NWK_ACTION_SECONDARY_TBL
    209 columns in XML definition, most are NULL/unmapped.
    Created empty - the ETL notebook appends rows here.
    """
    table = f"{DATABASE}.nwk_action_secondary_tbl"
    print(f"\n--- Creating {table} ---")

    # Include all columns written by write_nwk_action_secondary in ETL
    schema = StructType([
        StructField("EVENT_ID", LongType(), False),
        StructField("TSP_VESTING_CD", StringType(), True),
        StructField("RETND1_PAY_PLAN_CD", StringType(), True),
        StructField("RETND1_GRADE_CD", StringType(), True),
        StructField("RETND1_STEP_CD", StringType(), True),
        StructField("RETND1_EFF_DTE", DateType(), True),
        StructField("RETND1_EXP_DTE", DateType(), True),
        StructField("TSP_EMP_PP_UND_DED_AMT", DoubleType(), True),
        StructField("TSP_EMP_PP_UND_DED_PYMT_COUNT", IntegerType(), True),
        StructField("TSP_PAY_SUBJ_YTD_AMT", DoubleType(), True),
        StructField("TSP_EMP_UND_DED_OUTSTNDG_AMT", DoubleType(), True),
        StructField("TSP_GOVT_UND_DED_OUTSTNDG_AMT", DoubleType(), True),
        StructField("TSP_UND_DED_LTR_NUM", IntegerType(), True),
        StructField("TSP_UND_DED_OPTION_CD", StringType(), True),
        StructField("TSP_UND_DED_STOP_OPTION_CD", StringType(), True),
        StructField("MIL_SRVC_BRANCH_CD", StringType(), True),
        StructField("MIL_SRVC_BRANCH_COMPONENT_CD", StringType(), True),
        StructField("CRDTBL_MIL_SRVC_PERIOD", StringType(), True),
        StructField("MIL_LV_CUR_FY_HRS", IntegerType(), True),
        StructField("MIL_LV_EMERG_CUR_FY_HRS", IntegerType(), True),
        StructField("AWOP_YTD_HRS", DoubleType(), True),
        StructField("EMP_RESID_CITY_ST_NAME", StringType(), True),
        StructField("EMP_RESID_STREET_NAME", StringType(), True),
        StructField("LOAD_ID", StringType(), True),
        StructField("LOAD_DATE", DateType(), True),
    ])

    empty_df = spark.createDataFrame([], schema)
    empty_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table)
    print(f"  [OK] {table}: created (empty)")


def create_ehrp_recs_tracking_tbl(spark):
    """
    Target table: EHRP_RECS_TRACKING_TBL
    10 columns.  Created empty.
    """
    table = f"{DATABASE}.ehrp_recs_tracking_tbl"
    print(f"\n--- Creating {table} ---")

    schema = StructType([
        StructField("EMPLID", StringType(), True),
        StructField("EMPL_RCD", IntegerType(), True),
        StructField("EFFDT", DateType(), True),
        StructField("EFFSEQ", IntegerType(), True),
        StructField("EVENT_SUBMITTED_DT", DateType(), True),
        StructField("GVT_WIP_STATUS", StringType(), True),
        StructField("NOA_CD", StringType(), True),
        StructField("NOA_SUFFIX_CD", StringType(), True),
        StructField("BIIS_EVENT_ID", LongType(), False),
        StructField("LOAD_DATE", DateType(), True),
    ])

    empty_df = spark.createDataFrame([], schema)
    empty_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table)
    print(f"  [OK] {table}: created (empty)")


# ============================================================================
# Permanent / Post-load Tables
# ============================================================================

def create_action_primary_all(spark):
    """
    Permanent table: ACTION_PRIMARY_ALL
    Same schema as NWK_ACTION_PRIMARY_TBL.
    The afterload script inserts today's records here.
    """
    table = f"{DATABASE}.action_primary_all"
    print(f"\n--- Creating {table} ---")

    # Reuse the same schema as NWK_ACTION_PRIMARY_TBL
    primary_schema = spark.table(f"{DATABASE}.nwk_action_primary_tbl").schema
    empty_df = spark.createDataFrame([], primary_schema)
    empty_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table)
    print(f"  [OK] {table}: created (empty)")


def create_action_secondary_all(spark):
    """
    Permanent table: ACTION_SECONDARY_ALL
    Same schema as NWK_ACTION_SECONDARY_TBL.
    """
    table = f"{DATABASE}.action_secondary_all"
    print(f"\n--- Creating {table} ---")

    secondary_schema = spark.table(f"{DATABASE}.nwk_action_secondary_tbl").schema
    empty_df = spark.createDataFrame([], secondary_schema)
    empty_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table)
    print(f"  [OK] {table}: created (empty)")


def create_action_remarks_all(spark):
    """
    Permanent table: ACTION_REMARKS_ALL
    Also doubles as NWK_ACTION_REMARKS_TBL (created by afterload step 3).
    """
    table = f"{DATABASE}.action_remarks_all"
    print(f"\n--- Creating {table} ---")

    schema = StructType([
        StructField("EVENT_ID", LongType(), True),
        StructField("LINE_SEQ", StringType(), True),
        StructField("REMARK_TXT", StringType(), True),
        StructField("LOAD_DATE", DateType(), True),
        StructField("LOAD_ID", StringType(), True),
    ])

    empty_df = spark.createDataFrame([], schema)
    empty_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table)

    # Also create the staging remarks table with same schema
    staging_table = f"{DATABASE}.nwk_action_remarks_tbl"
    empty_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(staging_table)

    print(f"  [OK] {table}: created (empty)")
    print(f"  [OK] {staging_table}: created (empty)")


def create_process_table(spark):
    """
    Permanent table: PROCESS_TABLE
    Tracks the last load date (P_STARTDT).
    """
    table = f"{DATABASE}.process_table"
    print(f"\n--- Creating {table} ---")

    schema = StructType([
        StructField("P_STARTDT", DateType(), True),
    ])

    data = [(date(2026, 3, 31),)]
    df = spark.createDataFrame(data, schema)
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(table)
    print(f"  [OK] {table}: {df.count()} rows")


# ============================================================================
# Main
# ============================================================================

def main():
    """Create all stub tables and print a summary."""
    print("=" * 70)
    print("EHRP2BIIS_UPDATE - Stub Table Creation for Validation")
    print(f"Run Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    spark = create_spark_session()

    # Step 1: Ensure database
    ensure_database(spark)

    # Step 2: Source tables (with test data)
    create_nwk_new_ehrp_actions_tbl(spark)
    create_ps_gvt_job(spark)

    # Step 3: Lookup tables (with matching test data)
    create_sequence_num_tbl(spark)
    create_ps_gvt_employment(spark)
    create_ps_gvt_pers_nid(spark)
    create_ps_gvt_awd_data(spark)
    create_ps_gvt_ee_data_trk(spark)
    create_ps_he_fill_pos(spark)
    create_ps_gvt_citizenship(spark)
    create_ps_gvt_pers_data(spark)
    create_ps_jpm_jp_items(spark)

    # Step 4: Target tables (empty with correct schema)
    create_nwk_action_primary_tbl(spark)
    create_nwk_action_secondary_tbl(spark)
    create_ehrp_recs_tracking_tbl(spark)

    # Step 5: Permanent/post-load tables
    create_action_primary_all(spark)
    create_action_secondary_all(spark)
    create_action_remarks_all(spark)  # Also creates nwk_action_remarks_tbl
    create_process_table(spark)

    # Step 6: Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    tables = [
        row.tableName
        for row in spark.sql(f"SHOW TABLES IN {DATABASE}").collect()
    ]
    print(f"Total tables in {DATABASE}: {len(tables)}")
    for t in sorted(tables):
        count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {DATABASE}.{t}").collect()[0]["cnt"]
        print(f"  {DATABASE}.{t}: {count} rows")

    print("\n[DONE] All 18 stub tables created successfully.")
    print("       The EHRP2BIIS_UPDATE job can now be re-run for validation.")
    print("=" * 70)

    return True


# ---------------------------------------------------------------------------
# Databricks notebook entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    success = main()
    if not success:
        import sys
        sys.exit(1)
