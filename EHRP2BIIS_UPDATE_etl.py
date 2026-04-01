"""
EHRP2BIIS_UPDATE_etl.py
=======================
Main ETL transformation notebook for the EHRP2BIIS UPDATE pipeline.
Migrated from: Informatica PowerCenter mapping m_EHRP2BIIS_UPDATE

Pipeline overview (from analysis_report.md):
  Sources:  NWK_NEW_EHRP_ACTIONS_TBL, PS_GVT_JOB
  Targets:  EHRP_RECS_TRACKING_TBL, NWK_ACTION_PRIMARY_TBL, NWK_ACTION_SECONDARY_TBL

Data Flow:
  PS_GVT_JOB ----+
                  +--> SQ_PS_GVT_JOB --> exp_MAIN2BIIS --+--> NWK_ACTION_PRIMARY_TBL
  NWK_NEW_EHRP_  |                                       +--> NWK_ACTION_SECONDARY_TBL
  ACTIONS_TBL ---+                                       +--> EHRP_RECS_TRACKING_TBL

  Lookup Tables (9):
    SEQUENCE_NUM_TBL, PS_GVT_EMPLOYMENT, PS_GVT_PERS_NID,
    PS_GVT_AWD_DATA, PS_GVT_EE_DATA_TRK, PS_HE_FILL_POS,
    PS_GVT_CITIZENSHIP, PS_GVT_PERS_DATA, PS_JPM_JP_ITEMS

Technical:
  - Standard PySpark (no DLT, no Unity Catalog)
  - Delta format writes to hhs_migration database
  - Broadcast joins for small lookup tables
  - Inline assertions after critical transformations
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DateType
from pyspark.sql.window import Window
from datetime import datetime

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATABASE = "hhs_migration"
LOAD_DATE = datetime.now().strftime("%Y-%m-%d")
CURRENT_YEAR = datetime.now().year
CURRENT_MONTH = datetime.now().month
CURRENT_DAY = datetime.now().day
# Load ID format from exp_MAIN2BIIS: 'NK' || YYYY || MM || DD
MONTH_CHAR = str(CURRENT_MONTH).zfill(2)
DAY_CHAR = str(CURRENT_DAY).zfill(2)
LOAD_ID = f"NK{CURRENT_YEAR}{MONTH_CHAR}{DAY_CHAR}"


def create_spark_session():
    """Create and return a Spark session for the EHRP2BIIS ETL."""
    spark = SparkSession.builder \
        .appName("EHRP2BIIS_UPDATE_etl") \
        .config("spark.sql.sources.default", "delta") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sql(f"USE {DATABASE}")
    return spark


# ============================================================================
# STEP 1: Read Source Tables
# Informatica Source Qualifier: SQ_PS_GVT_JOB
# SQL Override joins NWK_NEW_EHRP_ACTIONS_TBL with PS_GVT_JOB on
#   EMPLID, EMPL_RCD, EFFDT, EFFSEQ
# ORDER BY PS_GVT_JOB.EFFDT
# ============================================================================
def read_source_tables(spark):
    """
    Read and join source tables, implementing the SQ_PS_GVT_JOB Source Qualifier.

    Informatica Source Qualifier SQL Override:
        SELECT PS_GVT_JOB.EMPLID, PS_GVT_JOB.EMPL_RCD, PS_GVT_JOB.EFFDT, ...
        FROM PS_GVT_JOB, NWK_NEW_EHRP_ACTIONS_TBL
        WHERE NWK_NEW_EHRP_ACTIONS_TBL.EMPLID = PS_GVT_JOB.EMPLID
          AND NWK_NEW_EHRP_ACTIONS_TBL.EMPL_RCD = PS_GVT_JOB.EMPL_RCD
          AND NWK_NEW_EHRP_ACTIONS_TBL.EFFDT = PS_GVT_JOB.EFFDT
          AND NWK_NEW_EHRP_ACTIONS_TBL.EFFSEQ = PS_GVT_JOB.EFFSEQ
        ORDER BY PS_GVT_JOB.EFFDT
    """
    print("\n[STEP 1] Reading source tables (SQ_PS_GVT_JOB Source Qualifier)")

    # Read the two source tables
    actions_df = spark.table(f"{DATABASE}.nwk_new_ehrp_actions_tbl")
    gvt_job_df = spark.table(f"{DATABASE}.ps_gvt_job")

    print(f"  nwk_new_ehrp_actions_tbl: {actions_df.count():,} rows")
    print("  ps_gvt_job: (reading...)")

    # SQ_PS_GVT_JOB: Inner join on composite key, ordered by EFFDT
    # Using broadcast on the smaller actions table
    sq_df = gvt_job_df.join(
        F.broadcast(actions_df).alias("a"),
        (gvt_job_df["EMPLID"] == F.col("a.EMPLID")) &
        (gvt_job_df["EMPL_RCD"] == F.col("a.EMPL_RCD")) &
        (gvt_job_df["EFFDT"] == F.col("a.EFFDT")) &
        (gvt_job_df["EFFSEQ"] == F.col("a.EFFSEQ")),
        "inner"
    ).select(gvt_job_df["*"]).orderBy(gvt_job_df["EFFDT"])

    sq_count = sq_df.count()
    print(f"  SQ_PS_GVT_JOB join result: {sq_count:,} rows")

    # Assertion: source join must produce rows
    assert sq_count > 0, "SQ_PS_GVT_JOB produced 0 rows - check source data"

    return sq_df


# ============================================================================
# STEP 2: Load All Lookup Tables
# 9 Informatica Lookup transformations, each with caching enabled
# We use broadcast joins for all lookups (small dimension tables)
# ============================================================================
def load_lookup_tables(spark):
    """
    Load all 9 lookup tables used by the Informatica mapping.
    Each is loaded as a broadcast-ready DataFrame.
    """
    print("\n[STEP 2] Loading lookup tables")
    lookups = {}

    # --- lkp_OLD_SEQUENCE_NUMBER ---
    # Lookup Table: SEQUENCE_NUM_TBL
    # Condition: EHRP_YEAR = o_CURRENT_YEAR
    # Returns: EHRP_SEQ_NUMBER
    lookups["sequence_num"] = spark.table(f"{DATABASE}.sequence_num_tbl")
    print(f"  sequence_num_tbl: {lookups['sequence_num'].count()} rows")

    # --- lkp_PS_GVT_EMPLOYMENT ---
    # Condition: EMPLID, EMPL_RCD, EFFDT, EFFSEQ
    # Returns: hire dates, SCD dates, BUSINESS_TITLE, PROBATION_DT, etc.
    lookups["gvt_employment"] = spark.table(f"{DATABASE}.ps_gvt_employment")
    print(f"  ps_gvt_employment: {lookups['gvt_employment'].count()} rows")

    # --- lkp_PS_GVT_PERS_NID ---
    # Condition: EMPLID
    # Returns: NATIONAL_ID (SSN)
    lookups["gvt_pers_nid"] = spark.table(f"{DATABASE}.ps_gvt_pers_nid")
    print(f"  ps_gvt_pers_nid: {lookups['gvt_pers_nid'].count()} rows")

    # --- lkp_PS_GVT_AWD_DATA ---
    # Condition: EMPLID, EMPL_RCD, EFFDT, EFFSEQ
    # Returns: OTH_PAY, OTH_HRS, GOAL_AMT, EARNINGS_END_DT, ERNCD, GVT_TANG_BEN_AMT, etc.
    lookups["gvt_awd_data"] = spark.table(f"{DATABASE}.ps_gvt_awd_data")
    print(f"  ps_gvt_awd_data: {lookups['gvt_awd_data'].count()} rows")

    # --- lkp_PS_GVT_EE_DATA_TRK ---
    # SQL Override: SELECT DISTINCT A.GVT_DATE_WRK, A.EMPLID, A.EMPL_RCD,
    #               A.EFFDT, A.EFFSEQ, A.GVT_WIP_STATUS FROM PS_GVT_EE_DATA_TRK A
    # Returns: GVT_DATE_WRK
    lookups["gvt_ee_data_trk"] = spark.table(f"{DATABASE}.ps_gvt_ee_data_trk") \
        .select("GVT_DATE_WRK", "EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ", "GVT_WIP_STATUS") \
        .distinct()
    print(f"  ps_gvt_ee_data_trk (distinct): {lookups['gvt_ee_data_trk'].count()} rows")

    # --- lkp_PS_HE_FILL_POS ---
    # Condition: EMPLID, EMPL_RCD, EFFDT, EFFSEQ
    # Returns: HE_FILL_POSITION
    lookups["he_fill_pos"] = spark.table(f"{DATABASE}.ps_he_fill_pos")
    print(f"  ps_he_fill_pos: {lookups['he_fill_pos'].count()} rows")

    # --- lkp_PS_GVT_CITIZENSHIP ---
    # Condition: EMPLID
    # Returns: CITIZENSHIP_STATUS
    lookups["gvt_citizenship"] = spark.table(f"{DATABASE}.ps_gvt_citizenship")
    print(f"  ps_gvt_citizenship: {lookups['gvt_citizenship'].count()} rows")

    # --- lkp_PS_GVT_PERS_DATA ---
    # SQL Override: joins PS_GVT_PERS_DATA p with NWK_NEW_EHRP_ACTIONS_TBL n
    # on EMPLID, EMPL_RCD, EFFDT, EFFSEQ
    # Returns: LAST_NAME, FIRST_NAME, MIDDLE_NAME, ADDRESS1, CITY, STATE,
    #          POSTAL, GEO_CODE, SEX, BIRTHDATE, MILITARY_STATUS,
    #          GVT_CRED_MIL_SVCE, GVT_MILITARY_COMP, GVT_MIL_RESRVE_CAT,
    #          GVT_VET_PREF_APPT, ETHNIC_GROUP, GVT_DISABILITY_CD
    lookups["gvt_pers_data"] = spark.table(f"{DATABASE}.ps_gvt_pers_data")
    print(f"  ps_gvt_pers_data: {lookups['gvt_pers_data'].count()} rows")

    # --- lkp_PS_JPM_JP_ITEMS ---
    # SQL Override: WHERE jpm_cat_type = 'DEG' AND eff_status = 'A'
    # Condition: JPM_PROFILE_ID = EMPLID
    # Returns: JPM_CAT_ITEM_ID (degree code), JPM_INTEGER_2 (year), MAJOR_CODE
    lookups["jpm_jp_items"] = spark.table(f"{DATABASE}.ps_jpm_jp_items") \
        .filter(
            (F.col("JPM_CAT_TYPE") == "DEG") &
            (F.col("EFF_STATUS") == "A")
        )
    print(f"  ps_jpm_jp_items (DEG/Active): {lookups['jpm_jp_items'].count()} rows")

    return lookups


# ============================================================================
# STEP 3: Apply Lookup Joins
# Each Informatica lookup is implemented as a broadcast left join.
# Left join because lookups may not find a match (returns NULL in Informatica).
# ============================================================================
def apply_lookup_joins(sq_df, lookups):
    """
    Join the Source Qualifier output with all 9 lookup tables.
    Mirrors the Informatica data flow where SQ_PS_GVT_JOB feeds into
    multiple lookup transformations in parallel.
    """
    print("\n[STEP 3] Applying lookup joins")
    df = sq_df

    # --- lkp_PS_GVT_EMPLOYMENT ---
    # Informatica Connector #13: SQ_PS_GVT_JOB -> lkp_PS_GVT_EMPLOYMENT (4 fields)
    # Join: EMPLID, EMPL_RCD, EFFDT, EFFSEQ
    # Returns fields that flow to exp_MAIN2BIIS and directly to NWK_ACTION_PRIMARY_TBL
    emp_cols = [
        "HIRE_DT", "REHIRE_DT", "CMPNY_SENIORITY_DT", "SERVICE_DT",
        "TERMINATION_DT", "BUSINESS_TITLE", "PROBATION_DT", "SETID",
        "GVT_SCD_RETIRE", "GVT_SCD_TSP", "GVT_SCD_LEO", "GVT_SCD_SEVPAY",
        "GVT_SEVPAY_PRV_WKS", "GVT_WGI_STATUS", "GVT_WGI_DUE_DATE",
        "GVT_TEMP_PRO_EXPIR", "GVT_SABBATIC_EXPIR",
        "GVT_CURR_APT_AUTH1", "GVT_CURR_APT_AUTH2",
        "GVT_RTND_GRADE_BEG", "GVT_RTND_GRADE_EXP",
        "TEMP_GVT_EFFDT", "OTH_PAY"
    ]
    emp_lkp = lookups["gvt_employment"].alias("emp")

    # Build select list - only columns that actually exist
    emp_available = [c for c in emp_cols if c in lookups["gvt_employment"].columns]
    emp_select = [F.col(f"emp.{c}").alias(f"emp_{c}") for c in emp_available]

    df = df.join(
        F.broadcast(emp_lkp),
        (df["EMPLID"] == F.col("emp.EMPLID")) &
        (df["EMPL_RCD"] == F.col("emp.EMPL_RCD")) &
        (df["EFFDT"] == F.col("emp.EFFDT")) &
        (df["EFFSEQ"] == F.col("emp.EFFSEQ")),
        "left"
    ).select(df["*"], *emp_select)
    print("  [OK] lkp_PS_GVT_EMPLOYMENT joined")

    # --- lkp_PS_GVT_PERS_NID ---
    # Connector #14: SQ_PS_GVT_JOB -> lkp_PS_GVT_PERS_NID (4 fields input, 1 output)
    # Returns: NATIONAL_ID (SSN)
    nid_lkp = lookups["gvt_pers_nid"].alias("nid")
    nid_select = []
    if "NATIONAL_ID" in lookups["gvt_pers_nid"].columns:
        nid_select.append(F.col("nid.NATIONAL_ID").alias("nid_NATIONAL_ID"))

    df = df.join(
        F.broadcast(nid_lkp),
        df["EMPLID"] == F.col("nid.EMPLID"),
        "left"
    ).select(df["*"], *nid_select)
    print("  [OK] lkp_PS_GVT_PERS_NID joined")

    # --- lkp_PS_GVT_AWD_DATA ---
    # Connector #15: SQ_PS_GVT_JOB -> lkp_PS_GVT_AWD_DATA (4 fields)
    # Returns: OTH_PAY, OTH_HRS, GOAL_AMT, EARNINGS_END_DT, ERNCD, GVT_TANG_BEN_AMT, etc.
    awd_cols = [
        "OTH_PAY", "OTH_HRS", "GOAL_AMT", "EARNINGS_END_DT",
        "GVT_TANG_BEN_AMT", "GVT_INTANG_BEN_AMT", "ERNCD"
    ]
    awd_lkp = lookups["gvt_awd_data"].alias("awd")
    awd_available = [c for c in awd_cols if c in lookups["gvt_awd_data"].columns]
    awd_select = [F.col(f"awd.{c}").alias(f"awd_{c}") for c in awd_available]

    df = df.join(
        F.broadcast(awd_lkp),
        (df["EMPLID"] == F.col("awd.EMPLID")) &
        (df["EMPL_RCD"] == F.col("awd.EMPL_RCD")) &
        (df["EFFDT"] == F.col("awd.EFFDT")) &
        (df["EFFSEQ"] == F.col("awd.EFFSEQ")),
        "left"
    ).select(df["*"], *awd_select)
    print("  [OK] lkp_PS_GVT_AWD_DATA joined")

    # --- lkp_PS_GVT_EE_DATA_TRK ---
    # Connector #11: SQ_PS_GVT_JOB -> lkp_PS_GVT_EE_DATA_TRK (5 fields)
    # Returns: GVT_DATE_WRK
    trk_lkp = lookups["gvt_ee_data_trk"].alias("trk")
    df = df.join(
        F.broadcast(trk_lkp),
        (df["EMPLID"] == F.col("trk.EMPLID")) &
        (df["EMPL_RCD"] == F.col("trk.EMPL_RCD")) &
        (df["EFFDT"] == F.col("trk.EFFDT")) &
        (df["EFFSEQ"] == F.col("trk.EFFSEQ")),
        "left"
    ).select(df["*"], F.col("trk.GVT_DATE_WRK").alias("trk_GVT_DATE_WRK"))
    print("  [OK] lkp_PS_GVT_EE_DATA_TRK joined")

    # --- lkp_PS_HE_FILL_POS ---
    # Connector #16: SQ_PS_GVT_JOB -> lkp_PS_HE_FILL_POS (4 fields)
    # Returns: HE_FILL_POSITION
    fill_lkp = lookups["he_fill_pos"].alias("fill")
    fill_select = []
    if "HE_FILL_POSITION" in lookups["he_fill_pos"].columns:
        fill_select.append(F.col("fill.HE_FILL_POSITION").alias("fill_HE_FILL_POSITION"))

    df = df.join(
        F.broadcast(fill_lkp),
        (df["EMPLID"] == F.col("fill.EMPLID")) &
        (df["EMPL_RCD"] == F.col("fill.EMPL_RCD")) &
        (df["EFFDT"] == F.col("fill.EFFDT")) &
        (df["EFFSEQ"] == F.col("fill.EFFSEQ")),
        "left"
    ).select(df["*"], *fill_select)
    print("  [OK] lkp_PS_HE_FILL_POS joined")

    # --- lkp_PS_GVT_CITIZENSHIP ---
    # Connector #17: SQ_PS_GVT_JOB -> lkp_PS_GVT_CITIZENSHIP (4 fields)
    # Returns: CITIZENSHIP_STATUS
    cit_lkp = lookups["gvt_citizenship"].alias("cit")
    cit_select = []
    if "CITIZENSHIP_STATUS" in lookups["gvt_citizenship"].columns:
        cit_select.append(F.col("cit.CITIZENSHIP_STATUS").alias("cit_CITIZENSHIP_STATUS"))

    df = df.join(
        F.broadcast(cit_lkp),
        df["EMPLID"] == F.col("cit.EMPLID"),
        "left"
    ).select(df["*"], *cit_select)
    print("  [OK] lkp_PS_GVT_CITIZENSHIP joined")

    # --- lkp_PS_GVT_PERS_DATA ---
    # Connector #18: SQ_PS_GVT_JOB -> lkp_PS_GVT_PERS_DATA (4 fields)
    # SQL Override joins with NWK_NEW_EHRP_ACTIONS_TBL on all 4 keys
    # Returns personal data fields
    pers_cols = [
        "LAST_NAME", "FIRST_NAME", "MIDDLE_NAME", "ADDRESS1",
        "CITY", "STATE", "POSTAL", "GEO_CODE", "SEX", "BIRTHDATE",
        "MILITARY_STATUS", "GVT_CRED_MIL_SVCE", "GVT_MILITARY_COMP",
        "GVT_MIL_RESRVE_CAT", "GVT_VET_PREF_APPT", "ETHNIC_GROUP",
        "GVT_DISABILITY_CD"
    ]
    pers_lkp = lookups["gvt_pers_data"].alias("pers")
    pers_available = [c for c in pers_cols if c in lookups["gvt_pers_data"].columns]
    pers_select = [F.col(f"pers.{c}").alias(f"pers_{c}") for c in pers_available]

    df = df.join(
        F.broadcast(pers_lkp),
        (df["EMPLID"] == F.col("pers.EMPLID")) &
        (df["EMPL_RCD"] == F.col("pers.EMPL_RCD")) &
        (df["EFFDT"] == F.col("pers.EFFDT")) &
        (df["EFFSEQ"] == F.col("pers.EFFSEQ")),
        "left"
    ).select(df["*"], *pers_select)
    print("  [OK] lkp_PS_GVT_PERS_DATA joined")

    # --- lkp_PS_JPM_JP_ITEMS ---
    # Connector #28: SQ_PS_GVT_JOB -> lkp_PS_JPM_JP_ITEMS (1 field: EMPLID)
    # Condition: JPM_PROFILE_ID = EMPLID
    # Returns: JPM_CAT_ITEM_ID, JPM_INTEGER_2, MAJOR_CODE
    jpm_cols = ["JPM_CAT_ITEM_ID", "JPM_INTEGER_2", "MAJOR_CODE"]
    jpm_lkp = lookups["jpm_jp_items"].alias("jpm")
    jpm_available = [c for c in jpm_cols if c in lookups["jpm_jp_items"].columns]
    jpm_select = [F.col(f"jpm.{c}").alias(f"jpm_{c}") for c in jpm_available]

    # Add JPM_PROFILE_ID join if column exists
    jpm_join_col = "JPM_PROFILE_ID" if "JPM_PROFILE_ID" in lookups["jpm_jp_items"].columns else "EMPLID"
    df = df.join(
        F.broadcast(jpm_lkp),
        df["EMPLID"] == F.col(f"jpm.{jpm_join_col}"),
        "left"
    ).select(df["*"], *jpm_select)
    print("  [OK] lkp_PS_JPM_JP_ITEMS joined")

    row_count = df.count()
    print(f"  Total rows after all lookups: {row_count:,}")
    assert row_count > 0, "No rows remain after lookup joins"

    return df


# ============================================================================
# STEP 4: exp_GET_EFFDT_YEAR Expression Transformation
# Extracts the year from EFFDT for sequence number lookup.
#   v_curr_year = GET_DATE_PART(EFFDT, 'YYYY')
#   o_CURRENT_YEAR = v_curr_year
# ============================================================================
def apply_exp_get_effdt_year(df):
    """
    Informatica Expression: exp_GET_EFFDT_YEAR
    Input: EFFDT (date/time)
    Variable: v_curr_year = GET_DATE_PART(EFFDT, 'YYYY')
    Output: o_CURRENT_YEAR = v_curr_year
    """
    print("\n[STEP 4] exp_GET_EFFDT_YEAR - Extract year from EFFDT")
    df = df.withColumn("o_CURRENT_YEAR", F.year(F.col("EFFDT")))
    print("  [OK] Added o_CURRENT_YEAR column")
    return df


# ============================================================================
# STEP 5: lkp_OLD_SEQUENCE_NUMBER
# Looks up the starting sequence number for the year from SEQUENCE_NUM_TBL.
#   Condition: EHRP_YEAR = o_CURRENT_YEAR
#   Returns: EHRP_SEQ_NUMBER
# ============================================================================
def apply_lkp_old_sequence_number(df, lookups):
    """
    Informatica Lookup: lkp_OLD_SEQUENCE_NUMBER
    Table: SEQUENCE_NUM_TBL
    Condition: EHRP_YEAR = o_CURRENT_YEAR
    Output: EHRP_SEQ_NUMBER (used for EVENT_ID generation)
    """
    print("\n[STEP 5] lkp_OLD_SEQUENCE_NUMBER - Get sequence number for year")
    seq_df = lookups["sequence_num"].alias("seq")

    df = df.join(
        F.broadcast(seq_df),
        df["o_CURRENT_YEAR"] == F.col("seq.EHRP_YEAR"),
        "left"
    ).select(df["*"], F.col("seq.EHRP_SEQ_NUMBER").alias("EHRP_SEQ_NUMBER"))

    print("  [OK] EHRP_SEQ_NUMBER joined")
    return df


# ============================================================================
# STEP 6: exp_PERS_DATA Expression Transformation
# Pass-through expression for personal data fields from lkp_PS_GVT_PERS_DATA.
# All fields are simple pass-throughs (no computed logic).
# ============================================================================
def apply_exp_pers_data(df):
    """
    Informatica Expression: exp_PERS_DATA
    Type: Pass-through expression for personal data fields.
    Input from lkp_PS_GVT_PERS_DATA, output to NWK_ACTION_PRIMARY_TBL
    and exp_MAIN2BIIS (CITY, STATE for address concatenation).

    All 17 ports are simple pass-throughs:
      BIRTHDATE, FIRST_NAME, LAST_NAME, MIDDLE_NAME, CITY, STATE,
      GEO_CODE, POSTAL, ADDRESS1, GVT_DISABILITY_CD, ETHNIC_GROUP,
      GVT_VET_PREF_APPT, MILITARY_STATUS, SEX, GVT_MILITARY_COMP,
      GVT_MIL_RESRVE_CAT, GVT_CRED_MIL_SVCE
    """
    print("\n[STEP 6] exp_PERS_DATA - Personal data pass-through")
    # Fields already exist as pers_* columns from the lookup join.
    # No computation needed - they pass through to the target mapping.
    print("  [OK] Personal data fields available as pers_* columns")
    return df


# ============================================================================
# STEP 7: exp_MAIN2BIIS Expression Transformation
# This is the MAIN expression transformation containing ALL business logic.
# It receives input from SQ_PS_GVT_JOB and all lookup transformations,
# and produces the computed output fields for the 3 target tables.
# ============================================================================
def apply_exp_main2biis(df):
    """
    Informatica Expression: exp_MAIN2BIIS
    The central transformation with ~80 computed output ports.
    Translates all IIF, DECODE, IN, IS_SPACES, ISNULL functions to PySpark.
    """
    print("\n[STEP 7] exp_MAIN2BIIS - Main business logic transformation")

    # -------------------------------------------------------------------
    # Variable ports (intermediate calculations used by output ports)
    # -------------------------------------------------------------------

    # v_MONTH_CHAR = IIF(MONTH < 10, '0' || TO_CHAR(MONTH), TO_CHAR(MONTH))
    # v_DAY_CHAR = IIF(DAY < 10, '0' || TO_CHAR(DAY), TO_CHAR(DAY))
    # (Used for o_LOAD_ID construction)
    df = df.withColumn("v_MONTH_CHAR", F.lit(MONTH_CHAR))
    df = df.withColumn("v_DAY_CHAR", F.lit(DAY_CHAR))

    # v_EVENT_ID: Sequence-based EVENT_ID generation
    # In Informatica this uses EHRP_SEQ_NUMBER + row_number as incrementing sequence
    # Equivalent: EHRP_SEQ_NUMBER + monotonically_increasing_id or row_number
    w = Window.orderBy("EFFDT", "EMPLID", "EMPL_RCD", "EFFSEQ")
    df = df.withColumn(
        "v_EVENT_ID",
        F.coalesce(F.col("EHRP_SEQ_NUMBER"), F.lit(0)).cast("long") +
        F.row_number().over(w)
    )

    # -------------------------------------------------------------------
    # Output port: o_EVENT_ID
    # Expression: v_EVENT_ID
    # -------------------------------------------------------------------
    df = df.withColumn("o_EVENT_ID", F.col("v_EVENT_ID").cast("long"))

    # -------------------------------------------------------------------
    # o_AGCY_ASSIGN_CD = COMPANY || GVT_SUB_AGENCY
    # Agency assignment code: concatenation of company and sub-agency
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_AGCY_ASSIGN_CD",
        F.concat(F.col("COMPANY"), F.col("GVT_SUB_AGENCY"))
    )

    # -------------------------------------------------------------------
    # o_AGCY_SUBELEMENT_PRIOR_CD
    # IIF(IS_SPACES(GVT_XFER_FROM_AGCY) or ISNULL(GVT_XFER_FROM_AGCY),
    #     NULL, GVT_XFER_FROM_AGCY || '00')
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_AGCY_SUBELEMENT_PRIOR_CD",
        F.when(
            F.col("GVT_XFER_FROM_AGCY").isNull() |
            (F.trim(F.col("GVT_XFER_FROM_AGCY")) == ""),
            F.lit(None).cast(StringType())
        ).otherwise(
            F.concat(F.col("GVT_XFER_FROM_AGCY"), F.lit("00"))
        )
    )

    # -------------------------------------------------------------------
    # Zero-to-NULL conversions for leave/financial fields
    # Pattern: IIF(field=0, NULL, field)
    # -------------------------------------------------------------------
    zero_to_null_fields = {
        "o_HE_AL_RED_CRED": "HE_AL_RED_CRED",
        "o_HE_AL_BALANCE": "HE_AL_BALANCE",
        "o_HE_LUMP_HRS": "HE_LUMP_HRS",
        "o_HE_AL_CARRYOVER": "HE_AL_CARRYOVER",
        "o_HE_RES_LASTYR": "HE_RES_LASTYR",
        "o_HE_RES_BALANCE": "HE_RES_BALANCE",
        "o_HE_RES_TWOYRS": "HE_RES_TWOYRS",
        "o_HE_RES_THREEYRS": "HE_RES_THREEYRS",
        "o_HE_AL_ACCRUAL": "HE_AL_ACCRUAL",
        "o_HE_AL_TOTAL": "HE_AL_TOTAL",
        "o_HE_AWOP_SEP": "HE_AWOP_SEP",
        "o_HE_AWOP_WIGI": "HE_AWOP_WIGI",
        "o_HE_SL_RED_CRED": "HE_SL_RED_CRED",
        "o_HE_SL_BALANCE": "HE_SL_BALANCE",
        "o_HE_FROZEN_SL": "HE_FROZEN_SL",
        "o_HE_SL_CARRYOVER": "HE_SL_CARRYOVER",
        "o_HE_SL_ACCRUAL": "HE_SL_ACCRUAL",
        "o_HE_SL_TOTAL": "HE_SL_TOTAL",
        "o_GVT_ANNUITY_OFFSET": "GVT_ANNUITY_OFFSET",
    }
    for out_col, src_col in zero_to_null_fields.items():
        df = df.withColumn(
            out_col,
            F.when(F.col(src_col) == 0, F.lit(None)).otherwise(F.col(src_col))
        )
    print("  [OK] Zero-to-NULL conversions (19 fields)")

    # -------------------------------------------------------------------
    # o_BASE_HOURS = IIF(GVT_WORK_SCHED='I', STD_HOURS, STD_HOURS*2)
    # Work schedule-dependent base hours calculation
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_BASE_HOURS",
        F.when(F.col("GVT_WORK_SCHED") == "I", F.col("STD_HOURS"))
         .otherwise(F.col("STD_HOURS") * 2)
    )

    # -------------------------------------------------------------------
    # o_LEG_AUTH_TXT_1 = UPPER(GVT_PAR_AUTH_D1 || RTRIM(GVT_PAR_AUTH_D1_2))
    # o_LEG_AUTH_TXT_2 = UPPER(GVT_PAR_AUTH_D2 || RTRIM(GVT_PAR_AUTH_D2_2))
    # Legal authority text concatenation
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_LEG_AUTH_TXT_1",
        F.upper(F.concat(
            F.coalesce(F.col("GVT_PAR_AUTH_D1"), F.lit("")),
            F.rtrim(F.coalesce(F.col("GVT_PAR_AUTH_D1_2"), F.lit("")))
        ))
    )
    df = df.withColumn(
        "o_LEG_AUTH_TXT_2",
        F.upper(F.concat(
            F.coalesce(F.col("GVT_PAR_AUTH_D2"), F.lit("")),
            F.rtrim(F.coalesce(F.col("GVT_PAR_AUTH_D2_2"), F.lit("")))
        ))
    )

    # -------------------------------------------------------------------
    # o_PERM_TEMP_POSITION_CD
    # IIF(STD_HOURS < 40, 3, IIF(STD_HOURS >= 40, 1, 0))
    # Note: this determines part-time (3) vs full-time (1)
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_PERM_TEMP_POSITION_CD",
        F.when(F.col("STD_HOURS") < 40, F.lit("3"))
         .when(F.col("STD_HOURS") >= 40, F.lit("1"))
         .otherwise(F.lit("0"))
    )

    # -------------------------------------------------------------------
    # o_PERMANENT_TEMP_POSITION_CD
    # IIF(REG_TEMP='R' AND GVT_WORK_SCHED='F', '1',
    #   IIF(IN(REG_TEMP,'R','T') AND IN(GVT_WORK_SCHED,'P','I'), '3',
    #     IIF(REG_TEMP='T' AND GVT_WORK_SCHED='F', '2', NULL)))
    # Position temp/perm classification
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_PERMANENT_TEMP_POSITION_CD",
        F.when(
            (F.col("REG_TEMP") == "R") & (F.col("GVT_WORK_SCHED") == "F"),
            F.lit("1")
        ).when(
            F.col("REG_TEMP").isin("R", "T") & F.col("GVT_WORK_SCHED").isin("P", "I"),
            F.lit("3")
        ).when(
            (F.col("REG_TEMP") == "T") & (F.col("GVT_WORK_SCHED") == "F"),
            F.lit("2")
        ).otherwise(F.lit(None).cast(StringType()))
    )

    # -------------------------------------------------------------------
    # o_UNION_CD = IIF(IS_SPACES(UNION_CD), NULL, UNION_CD)
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_UNION_CD",
        F.when(
            F.col("UNION_CD").isNull() | (F.trim(F.col("UNION_CD")) == ""),
            F.lit(None).cast(StringType())
        ).otherwise(F.col("UNION_CD"))
    )

    # -------------------------------------------------------------------
    # o_EMP_RESID_CITY_STATE_NAME = CITY || ', ' || STATE
    # From exp_PERS_DATA output (pers_CITY, pers_STATE)
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_EMP_RESID_CITY_STATE_NAME",
        F.concat(
            F.coalesce(F.col("pers_CITY"), F.lit("")),
            F.lit(", "),
            F.coalesce(F.col("pers_STATE"), F.lit(""))
        )
    )

    # -------------------------------------------------------------------
    # o_EFFSEQ = EFFSEQ (cast to string for target)
    # -------------------------------------------------------------------
    df = df.withColumn("o_EFFSEQ", F.col("EFFSEQ").cast(StringType()))

    # -------------------------------------------------------------------
    # o_LOAD_ID = 'NK' || YYYY || MM || DD
    # o_LOAD_DATE = trunc(sysdate)
    # -------------------------------------------------------------------
    df = df.withColumn("o_LOAD_ID", F.lit(LOAD_ID))
    df = df.withColumn("o_LOAD_DATE", F.current_date())

    # -------------------------------------------------------------------
    # o_LINE_SEQ = '01N' (constant)
    # -------------------------------------------------------------------
    df = df.withColumn("o_LINE_SEQ", F.lit("01N"))

    # -------------------------------------------------------------------
    # o_CASH_AWARD_AMT
    # DECODE(TRUE,
    #   IN(GVT_NOA_CODE,'840','841','842','843','844','845','848','849',
    #      '873','874','875','876','877','878','879')
    #     AND NOT IN(ERNCD,'RCR','RLC'), GOAL_AMT,
    #   IN(GVT_NOA_CODE,'817','889'), GOAL_AMT,
    #   NULL)
    # -------------------------------------------------------------------
    award_noa_codes = [
        "840", "841", "842", "843", "844", "845", "848", "849",
        "873", "874", "875", "876", "877", "878", "879"
    ]
    df = df.withColumn(
        "o_CASH_AWARD_AMT",
        F.when(
            F.col("GVT_NOA_CODE").isin(award_noa_codes) &
            ~F.coalesce(F.col("awd_ERNCD"), F.lit("")).isin("RCR", "RLC"),
            F.col("awd_GOAL_AMT")
        ).when(
            F.col("GVT_NOA_CODE").isin("817", "889"),
            F.col("awd_GOAL_AMT")
        ).otherwise(F.lit(None))
    )

    # -------------------------------------------------------------------
    # o_CASH_AWARD_BNFT_AMT
    # IIF(IN(GVT_NOA_CODE, award_codes) AND NOT IN(ERNCD,'RCR','RLC'),
    #     GVT_TANG_BEN_AMT, NULL)
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_CASH_AWARD_BNFT_AMT",
        F.when(
            F.col("GVT_NOA_CODE").isin(award_noa_codes) &
            ~F.coalesce(F.col("awd_ERNCD"), F.lit("")).isin("RCR", "RLC"),
            F.col("awd_GVT_TANG_BEN_AMT")
        ).otherwise(F.lit(None))
    )

    # -------------------------------------------------------------------
    # o_RECRUITMENT_BONUS_AMT
    # IIF(GVT_NOA_CODE='815' OR (GVT_NOA_CODE='948' AND HE_NOA_EXT='0'),
    #     GOAL_AMT, NULL)
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_RECRUITMENT_BONUS_AMT",
        F.when(
            (F.col("GVT_NOA_CODE") == "815") |
            ((F.col("GVT_NOA_CODE") == "948") & (F.col("HE_NOA_EXT") == "0")),
            F.col("awd_GOAL_AMT")
        ).otherwise(F.lit(None))
    )

    # -------------------------------------------------------------------
    # o_RELOCATION_BONUS_AMT
    # IIF(IN(GVT_NOA_CODE, '816'), GOAL_AMT, NULL)
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_RELOCATION_BONUS_AMT",
        F.when(F.col("GVT_NOA_CODE") == "816", F.col("awd_GOAL_AMT"))
         .otherwise(F.lit(None))
    )

    # -------------------------------------------------------------------
    # o_TIME_OFF_AWARD_AMT = IIF(IN(GVT_NOA_CODE,'846','847'), OTH_HRS, NULL)
    # o_TIME_OFF_GRANTED_HRS = IIF(IN(GVT_NOA_CODE,'846','847'), OTH_HRS, NULL)
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_TIME_OFF_AWARD_AMT",
        F.when(F.col("GVT_NOA_CODE").isin("846", "847"), F.col("awd_OTH_HRS"))
         .otherwise(F.lit(None))
    )
    df = df.withColumn(
        "o_TIME_OFF_GRANTED_HRS",
        F.when(F.col("GVT_NOA_CODE").isin("846", "847"), F.col("awd_OTH_HRS"))
         .otherwise(F.lit(None))
    )

    # -------------------------------------------------------------------
    # o_CITIZENSHIP_STATUS
    # IIF(CITIZENSHIP_STATUS_IN='1' or CITIZENSHIP_STATUS_IN='2', '1', '8')
    # Maps PeopleSoft citizenship codes to BIIS format
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_CITIZENSHIP_STATUS",
        F.when(
            F.coalesce(F.col("cit_CITIZENSHIP_STATUS"), F.lit("")).isin("1", "2"),
            F.lit("1")
        ).otherwise(F.lit("8"))
    )

    # -------------------------------------------------------------------
    # o_TSP_VESTING_CD
    # IIF(IN(GVT_RETIRE_PLAN,'K','M'),
    #   IIF(IN(GVT_TYPE_OF_APPT,'55','34','36','46','44'), 2, 3), 0)
    # Complex TSP vesting code based on retirement plan and appointment type
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_TSP_VESTING_CD",
        F.when(
            F.col("GVT_RETIRE_PLAN").isin("K", "M"),
            F.when(
                F.col("GVT_TYPE_OF_APPT").isin("55", "34", "36", "46", "44"),
                F.lit("2")
            ).otherwise(F.lit("3"))
        ).otherwise(F.lit("0"))
    )

    # -------------------------------------------------------------------
    # o_EVENT_SUBMITTED_DTE
    # IIF(ISNULL(GVT_DATE_WRK), ACTION_DT, GVT_DATE_WRK)
    # Uses GVT_DATE_WRK from lkp_PS_GVT_EE_DATA_TRK if available
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_EVENT_SUBMITTED_DTE",
        F.when(
            F.col("trk_GVT_DATE_WRK").isNull(),
            F.col("ACTION_DT")
        ).otherwise(F.col("trk_GVT_DATE_WRK"))
    )

    # -------------------------------------------------------------------
    # o_RECRUITMENT_EXP_DTE
    # IIF(GVT_NOA_CODE='815' OR (GVT_NOA_CODE='948' AND HE_NOA_EXT='0'),
    #     EARNINGS_END_DT, NULL)
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_RECRUITMENT_EXP_DTE",
        F.when(
            (F.col("GVT_NOA_CODE") == "815") |
            ((F.col("GVT_NOA_CODE") == "948") & (F.col("HE_NOA_EXT") == "0")),
            F.col("awd_EARNINGS_END_DT")
        ).otherwise(F.lit(None).cast(DateType()))
    )

    # -------------------------------------------------------------------
    # o_RELOCATION_EXP_DTE
    # IIF(GVT_NOA_CODE='816' OR (GVT_NOA_CODE='948' AND HE_NOA_EXT='1'),
    #     EARNINGS_END_DT, NULL)
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_RELOCATION_EXP_DTE",
        F.when(
            (F.col("GVT_NOA_CODE") == "816") |
            ((F.col("GVT_NOA_CODE") == "948") & (F.col("HE_NOA_EXT") == "1")),
            F.col("awd_EARNINGS_END_DT")
        ).otherwise(F.lit(None).cast(DateType()))
    )

    # -------------------------------------------------------------------
    # o_GVT_CURR_APT_AUTH1 = IIF(IS_SPACES(GVT_CURR_APT_AUTH1), NULL, GVT_CURR_APT_AUTH1)
    # o_GVT_CURR_APT_AUTH2 = IIF(IS_SPACES(GVT_CURR_APT_AUTH2), NULL, GVT_CURR_APT_AUTH2)
    # -------------------------------------------------------------------
    for col_name in ["GVT_CURR_APT_AUTH1", "GVT_CURR_APT_AUTH2"]:
        src = f"emp_{col_name}"
        df = df.withColumn(
            f"o_{col_name}",
            F.when(
                F.col(src).isNull() | (F.trim(F.col(src)) == ""),
                F.lit(None).cast(StringType())
            ).otherwise(F.col(src))
        )

    # -------------------------------------------------------------------
    # FEGLI_LIVING_BNFT_CD = DECODE(GVT_NOA_CODE, '805', 'F', '806', 'P', NULL)
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_FEGLI_LIVING_BNFT_CD",
        F.when(F.col("GVT_NOA_CODE") == "805", F.lit("F"))
         .when(F.col("GVT_NOA_CODE") == "806", F.lit("P"))
         .otherwise(F.lit(None).cast(StringType()))
    )

    # -------------------------------------------------------------------
    # LWOP_START_DATE = IIF(IN(GVT_NOA_CODE, '460', '473'), EFFDT, NULL)
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_LWOP_START_DATE",
        F.when(F.col("GVT_NOA_CODE").isin("460", "473"), F.col("EFFDT"))
         .otherwise(F.lit(None).cast(DateType()))
    )

    # -------------------------------------------------------------------
    # BUYOUT_EFFDT = IIF(GVT_NOA_CODE = '825', EFFDT, NULL)
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_BUYOUT_EFFDT",
        F.when(F.col("GVT_NOA_CODE") == "825", F.col("EFFDT"))
         .otherwise(F.lit(None).cast(DateType()))
    )

    # -------------------------------------------------------------------
    # o_BUSINESS_TITLE = UPPER(BUSINESS_TITLE)
    # From lkp_PS_GVT_EMPLOYMENT
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_BUSINESS_TITLE",
        F.upper(F.col("emp_BUSINESS_TITLE"))
    )

    # -------------------------------------------------------------------
    # o_BUYOUT_AMT = IIF(GVT_NOA_CODE='825' AND ACTION='BON', GOAL_AMT, NULL)
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_BUYOUT_AMT",
        F.when(
            (F.col("GVT_NOA_CODE") == "825") & (F.col("ACTION") == "BON"),
            F.col("awd_GOAL_AMT")
        ).otherwise(F.lit(None))
    )

    # -------------------------------------------------------------------
    # o_APPT_LIMIT_NTE_HRS = IIF(GVT_APPT_LIMIT_HRS=0, NULL, GVT_APPT_LIMIT_HRS)
    # From lkp_PS_GVT_EMPLOYMENT
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_APPT_LIMIT_NTE_HRS",
        F.when(F.col("GVT_APPT_LIMIT_HRS") == 0, F.lit(None))
         .otherwise(F.col("GVT_APPT_LIMIT_HRS"))
    ) if "GVT_APPT_LIMIT_HRS" in df.columns else df.withColumn(
        "o_APPT_LIMIT_NTE_HRS", F.lit(None)
    )

    # -------------------------------------------------------------------
    # YEAR_DEGREE = IIF(JPM_INTEGER_2=0, NULL, JPM_INTEGER_2)
    # From lkp_PS_JPM_JP_ITEMS
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_YEAR_DEGREE",
        F.when(
            F.coalesce(F.col("jpm_JPM_INTEGER_2"), F.lit(0)) == 0,
            F.lit(None)
        ).otherwise(F.col("jpm_JPM_INTEGER_2"))
    )

    # -------------------------------------------------------------------
    # o_SEVERANCE_PAY_AMT = IIF(IN(GVT_NOA_CODE,'304','312','356','357'), OTH_PAY, NULL)
    # -------------------------------------------------------------------
    sev_noa = ["304", "312", "356", "357"]
    df = df.withColumn(
        "o_SEVERANCE_PAY_AMT",
        F.when(F.col("GVT_NOA_CODE").isin(sev_noa), F.col("awd_OTH_PAY"))
         .otherwise(F.lit(None))
    )

    # -------------------------------------------------------------------
    # o_SEVERANCE_TOTAL_AMT = IIF(IN(GVT_NOA_CODE,'304','312','356','357'), GOAL_AMT, NULL)
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_SEVERANCE_TOTAL_AMT",
        F.when(F.col("GVT_NOA_CODE").isin(sev_noa), F.col("awd_GOAL_AMT"))
         .otherwise(F.lit(None))
    )

    # -------------------------------------------------------------------
    # o_SEVERANCE_PAY_START_DTE = IIF(IN(GVT_NOA_CODE,'304','312','356','357'), EFFDT, NULL)
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_SEVERANCE_PAY_START_DTE",
        F.when(F.col("GVT_NOA_CODE").isin(sev_noa), F.col("EFFDT"))
         .otherwise(F.lit(None).cast(DateType()))
    )

    # -------------------------------------------------------------------
    # o_PROBATION_DT = IIF(ISNULL(PROBATION_DT), NULL, ADD_TO_DATE(PROBATION_DT, 'MM', -12))
    # Subtract 12 months from PROBATION_DT
    # -------------------------------------------------------------------
    df = df.withColumn(
        "o_PROBATION_DT",
        F.when(
            F.col("emp_PROBATION_DT").isNull(),
            F.lit(None).cast(DateType())
        ).otherwise(
            F.add_months(F.col("emp_PROBATION_DT"), -12)
        )
    )

    print("  [OK] All exp_MAIN2BIIS output ports computed (~50+ expressions)")
    return df


# ============================================================================
# STEP 8: Write to Target Tables
# Three target tables receive data from different combinations of transformations:
#   1. EHRP_RECS_TRACKING_TBL - from SQ_PS_GVT_JOB + exp_MAIN2BIIS
#   2. NWK_ACTION_PRIMARY_TBL - from exp_MAIN2BIIS + lookups + exp_PERS_DATA
#   3. NWK_ACTION_SECONDARY_TBL - from exp_MAIN2BIIS + exp_PERS_DATA
# ============================================================================
def write_ehrp_recs_tracking(df, spark):
    """
    Write to EHRP_RECS_TRACKING_TBL target table.
    Informatica Connectors:
      - #9: SQ_PS_GVT_JOB -> EHRP_RECS_TRACKING_TBL (7 fields)
      - #19: exp_MAIN2BIIS -> EHRP_RECS_TRACKING_TBL (3 fields)

    Target fields (10 total):
      EMPLID, EMPL_RCD, EFFDT, EFFSEQ, EVENT_SUBMITTED_DT,
      GVT_WIP_STATUS, NOA_CD, NOA_SUFFIX_CD, BIIS_EVENT_ID, LOAD_DATE
    """
    print("\n[STEP 8a] Writing EHRP_RECS_TRACKING_TBL")

    tracking_df = df.select(
        F.col("EMPLID").cast(StringType()).alias("EMPLID"),
        F.col("EMPL_RCD").alias("EMPL_RCD"),
        F.col("EFFDT").alias("EFFDT"),
        F.col("EFFSEQ").alias("EFFSEQ"),
        F.col("o_EVENT_SUBMITTED_DTE").alias("EVENT_SUBMITTED_DT"),
        F.col("GVT_WIP_STATUS").alias("GVT_WIP_STATUS"),
        F.col("GVT_NOA_CODE").alias("NOA_CD"),
        F.col("HE_NOA_EXT").alias("NOA_SUFFIX_CD"),
        F.col("o_EVENT_ID").alias("BIIS_EVENT_ID"),
        F.col("o_LOAD_DATE").alias("LOAD_DATE"),
    )

    count = tracking_df.count()
    print(f"  Rows to write: {count:,}")
    assert count > 0, "EHRP_RECS_TRACKING_TBL would receive 0 rows"

    tracking_df.write.format("delta") \
        .mode("append") \
        .saveAsTable(f"{DATABASE}.ehrp_recs_tracking_tbl")

    print(f"  [OK] {count:,} rows written to {DATABASE}.ehrp_recs_tracking_tbl")
    return count


def write_nwk_action_primary(df, spark):
    """
    Write to NWK_ACTION_PRIMARY_TBL target table.
    This is the largest target with 260 columns.

    Informatica Connectors feeding this target:
      - #3: exp_MAIN2BIIS -> NWK_ACTION_PRIMARY_TBL (93 fields)
      - #6: lkp_PS_GVT_EMPLOYMENT -> NWK_ACTION_PRIMARY_TBL (14 fields)
      - #7: exp_PERS_DATA -> NWK_ACTION_PRIMARY_TBL (13 fields)
      - #21: lkp_PS_JPM_JP_ITEMS -> NWK_ACTION_PRIMARY_TBL (2 fields)
      - #23: lkp_PS_HE_FILL_POS -> NWK_ACTION_PRIMARY_TBL (1 field)
      - #24: lkp_PS_GVT_PERS_NID -> NWK_ACTION_PRIMARY_TBL (1 field)
    """
    print("\n[STEP 8b] Writing NWK_ACTION_PRIMARY_TBL")

    primary_df = df.select(
        # From exp_MAIN2BIIS
        F.col("o_EVENT_ID").alias("EVENT_ID"),
        F.col("o_AGCY_ASSIGN_CD").alias("AGCY_ASSIGN_CD"),
        F.concat(F.col("COMPANY"), F.col("GVT_SUB_AGENCY")).alias("AGCY_SUBELEMENT_CD"),
        F.col("o_AGCY_SUBELEMENT_PRIOR_CD").alias("AGCY_SUBELEMENT_PRIOR_CD"),
        F.col("o_HE_AL_RED_CRED").alias("ANN_LV_CRDT_REDUCTN_HRS"),
        F.col("o_HE_AL_BALANCE").alias("ANN_LV_CUR_BAL_HRS"),
        F.col("o_HE_LUMP_HRS").alias("ANN_LV_LUMP_SUM_PAID_HRS"),
        F.col("o_HE_AL_CARRYOVER").alias("ANN_LV_PRIOR_YEAR_BAL_HRS"),
        F.col("o_HE_RES_BALANCE").alias("ANN_LV_RESTORED_BAL_LV_HRS"),
        F.col("o_HE_RES_LASTYR").alias("ANN_LV_RESTORED_BAL1_HRS"),
        F.col("o_HE_RES_TWOYRS").alias("ANN_LV_RESTORED_BAL2_HRS"),
        F.col("o_HE_RES_THREEYRS").alias("ANN_LV_RESTORED_BAL3_HRS"),
        F.col("o_HE_AL_ACCRUAL").alias("ANN_LV_YTD_ACCRD_HRS"),
        F.col("o_HE_AL_TOTAL").alias("ANN_LV_YTD_USED_HRS"),
        F.col("ANNL_BENEF_BASE_RT").alias("ANN_SALARY_RATE_AMT"),
        F.col("GVT_ANN_IND").alias("ANNUITANT_IND_CD"),
        F.col("o_APPT_LIMIT_NTE_HRS").alias("APPT_LMT_NTE_HRS"),
        F.col("GVT_PAR_NTE_DATE").alias("APPT_NTE_DTE"),
        F.col("GVT_TYPE_OF_APPT").alias("APPT_TYPE_CD"),
        F.col("BARG_UNIT").alias("BARGAINING_UNIT_CD"),
        F.col("o_BASE_HOURS").alias("BASE_HRS"),

        # Personal data from exp_PERS_DATA (via lkp_PS_GVT_PERS_DATA)
        F.col("pers_BIRTHDATE").alias("BIRTH_DTE"),
        F.col("o_CASH_AWARD_AMT").alias("CASH_AWARD_AMT"),
        F.col("o_CASH_AWARD_BNFT_AMT").alias("CASH_AWARD_BNFT_AMT"),
        F.col("DEPTID").alias("DUTY_STATION_CD"),
        F.col("pers_FIRST_NAME").alias("EMP_FIRST_NAME"),
        F.col("pers_LAST_NAME").alias("EMP_LAST_NAME"),
        F.col("pers_MIDDLE_NAME").alias("EMP_MID_INIT"),
        F.col("o_EMP_RESID_CITY_STATE_NAME").alias("EMP_RESID_CITY_ST_NAME"),
        F.col("pers_GEO_CODE").alias("EMP_RESID_GEOGPHCL_LOC_CD"),
        F.col("pers_POSTAL").alias("EMP_RESID_POSTAL_CD"),
        F.col("pers_ADDRESS1").alias("EMP_RESID_STREET_NAME"),

        # Dates and employment data
        F.col("EFFDT").alias("EVENT_EFF_DTE"),
        F.col("o_EVENT_SUBMITTED_DTE").alias("EVENT_SUBMITTED_DTE"),
        F.col("o_FEGLI_LIVING_BNFT_CD").alias("FEGLI_LIVING_BNFT_CD"),
        F.col("GVT_FEGLI").alias("FEGLI_CD"),
        F.col("GVT_FERS_COVERAGE").alias("FERS_COV_CD"),
        F.col("FLSA_STATUS").alias("FLSA_CATGRY_CD"),
        F.col("GVT_CSRS_FROZN_SVC").alias("FROZEN_SERVICE_PERIOD"),
        F.col("GRADE").alias("GRADE_CD"),
        F.col("pers_GVT_DISABILITY_CD").alias("HANDICAP_CD"),
        F.col("HOURLY_RT").alias("HRLY_RATE_AMT"),
        F.col("GVT_LEG_AUTH_1").alias("LEGAL_AUTH_CD"),
        F.col("o_LEG_AUTH_TXT_1").alias("LEGAL_AUTH_TXT"),
        F.col("GVT_LEG_AUTH_2").alias("LEGAL_AUTH2_CD"),
        F.col("o_LEG_AUTH_TXT_2").alias("LEGAL_AUTH2_TXT"),
        F.col("GVT_LOCALITY_ADJ").alias("LOCALITY_PAY_AMT"),
        F.col("o_HE_AWOP_WIGI").alias("LWOP_AWOP_WGI_HRS"),
        F.col("GVT_NOA_CODE").alias("NOA_CD"),
        F.col("HE_NOA_EXT").alias("NOA_SUFFIX_CD"),
        F.col("JOBCODE").alias("OCCUPATION_CD"),
        F.col("GVT_PAY_BASIS").alias("PAY_BASIS_CD"),
        F.col("GVT_PAY_PLAN").alias("PAY_PLAN_CD"),
        F.col("o_PERMANENT_TEMP_POSITION_CD").alias("PERMANENT_TEMP_POSITION_CD"),
        F.col("GVT_POI").alias("PERSONNEL_OFFICE_ID_CD"),
        F.col("POSITION_NBR").alias("POSITION_NUM"),
        F.col("GVT_POSN_OCCUPIED").alias("POSITION_OCCUPIED_CD"),
        F.col("o_BUSINESS_TITLE").alias("POSITION_TITLE_NAME"),
        F.col("GVT_PAY_RATE_DETER").alias("PRD_CD"),
        F.col("GVT_PREV_RET_COVRG").alias("PREV_RETMT_COV_CD"),
        F.col("o_PROBATION_DT").alias("PROB_TRIAL_PERIOD_START_DTE"),
        F.col("pers_ETHNIC_GROUP").alias("RACE_NATL_ORIGIN_CD"),
        F.col("o_RECRUITMENT_BONUS_AMT").alias("RECRUITMENT_BONUS_AMT"),
        F.col("o_RECRUITMENT_EXP_DTE").alias("RECRUITMENT_EXP_DTE"),
        F.col("o_RELOCATION_BONUS_AMT").alias("RELOCATION_BONUS_AMT"),
        F.col("o_RELOCATION_EXP_DTE").alias("RELOCATION_EXP_DTE"),
        F.col("GVT_RETIRE_PLAN").alias("RETMT_PLAN_CD"),
        F.col("GVT_COMPRATE").alias("SCHLD_ANN_SALARY_AMT"),
        F.col("GVT_HRLY_RT_NO_LOC").alias("SCHLD_HRLY_RATE_AMT"),
        F.col("o_SEVERANCE_PAY_AMT").alias("SEVERANCE_PAY_AMT"),
        F.col("o_SEVERANCE_PAY_START_DTE").alias("SEVERANCE_PAY_START_DTE"),
        F.col("o_SEVERANCE_TOTAL_AMT").alias("SEVERANCE_PAY_TOT_AMT"),
        F.col("pers_SEX").alias("SEX_CD"),
        F.col("o_HE_SL_RED_CRED").alias("SICK_LV_CRDT_REDUCTN_HRS"),
        F.col("o_HE_SL_BALANCE").alias("SICK_LV_CUR_BAL_HRS"),
        F.col("o_HE_FROZEN_SL").alias("SICK_LV_FERS_ELECT_BAL_HRS"),
        F.col("o_HE_SL_CARRYOVER").alias("SICK_LV_PRIOR_YEAR_BAL_HRS"),
        F.col("o_HE_SL_ACCRUAL").alias("SICK_LV_YTD_ACCRD_HRS"),
        F.col("o_HE_SL_TOTAL").alias("SICK_LV_YTD_USED_HRS"),

        # From lkp_PS_GVT_PERS_NID
        F.col("nid_NATIONAL_ID").alias("SSN"),

        F.col("GVT_STEP").alias("STEP_CD"),
        F.col("o_TIME_OFF_AWARD_AMT").alias("TIME_OFF_AWARD_AMT"),
        F.col("o_TIME_OFF_GRANTED_HRS").alias("TIME_OFF_GRANTED_HRS"),
        F.col("o_UNION_CD").alias("UNION_CD"),
        F.col("o_CITIZENSHIP_STATUS").alias("US_CITIZENSHIP_CD"),
        F.col("pers_GVT_VET_PREF_APPT").alias("VETERANS_PREFERENCE_CD"),
        F.col("pers_MILITARY_STATUS").alias("VETERANS_STATUS_CD"),
        F.col("GVT_WORK_SCHED").alias("WORK_SCHEDULE_CD"),
        F.col("o_YEAR_DEGREE").alias("YEAR_DEGREE_ATTAINED_DTE"),
        F.col("o_LOAD_ID").alias("LOAD_ID"),
        F.col("o_LOAD_DATE").alias("LOAD_DATE"),
        F.col("o_EFFSEQ").alias("EFFSEQ"),
        F.col("EMPL_RCD").alias("EMPL_REC_NO"),
        F.col("ACTION").alias("EHRP_TYPE_ACTION"),
        F.col("ACTION_REASON").alias("EHRP_ACTION_REASON"),
        F.col("GVT_WIP_STATUS").alias("GVT_WIP_STATUS"),
        F.col("GVT_STATUS_TYPE").alias("GVT_STATUS_TYPE"),
        F.col("POSITION_NBR").alias("EHRP_POSITION_NUMBER"),
        F.col("o_GVT_CURR_APT_AUTH1").alias("CURR_APPT_AUTH1_CD"),
        F.col("o_GVT_CURR_APT_AUTH2").alias("CURR_APPT_AUTH2_CD"),
        F.col("GVT_LEO_POSITION").alias("LEO_POSITION_CD"),
        F.col("REPORTS_TO").alias("REPORTS_TO"),
        F.col("POSITION_ENTRY_DT").alias("POSITION_ENTRY_DT"),
        F.col("SETID_DEPT").alias("SETID"),
        F.col("o_GVT_ANNUITY_OFFSET").alias("FEGLI_LIVING_BNFT_REMAIN_AMT"),

        # From lkp_PS_GVT_EMPLOYMENT dates
        F.col("emp_HIRE_DT").alias("EMP_EOD_DTE"),
        F.col("emp_GVT_SCD_RETIRE").alias("RETMT_SCD_DTE"),
        F.col("emp_GVT_SCD_TSP").alias("TSP_SCD_DTE"),
        F.col("emp_CMPNY_SENIORITY_DT").alias("CAREER_START_DTE"),
        F.col("emp_GVT_SCD_LEO").alias("LEO_SCD_DT"),
        F.col("GRADE_ENTRY_DT").alias("EMP_GRADE_START_DTE"),
        F.col("STEP_ENTRY_DT").alias("WGI_START_DTE"),
        F.col("o_BUYOUT_AMT").alias("BUYOUT_AMT"),
        F.col("o_BUYOUT_EFFDT").alias("BUYOUT_EFF_DTE"),
        F.col("o_LWOP_START_DATE").alias("LWOP_START_DTE"),

        # From lkp_PS_HE_FILL_POS
        F.col("fill_HE_FILL_POSITION").alias("FILLING_POSITION_CD"),

        # TSP fields
        F.col("o_TSP_VESTING_CD").alias("TSP_VESTING_CD"),
        F.col("HE_PP_UDED_AMT").alias("TSP_EMP_PP_UND_DED_AMT"),
        F.col("HE_NO_TSP_PAYPER").alias("TSP_EMP_PP_UND_DED_PYMT_COUNT"),
        F.col("HE_TSPA_SUB_YR").alias("TSP_PAY_SUBJ_YTD_AMT"),
        F.col("HE_EMP_UDED_AMT").alias("TSP_EMP_UND_DED_OUTSTNDG_AMT"),
        F.col("HE_GVT_UDED_AMT").alias("TSP_GOVT_UND_DED_OUTSTNDG_AMT"),
        F.col("HE_TLTR_NO").alias("TSP_UND_DED_LTR_NUM"),
        F.col("HE_UDED_PAY_CD").alias("TSP_UND_DED_OPTION_CD"),
        F.col("HE_TSP_CANC_CD").alias("TSP_UND_DED_STOP_OPTION_CD"),

        # Military fields from exp_PERS_DATA
        F.col("pers_GVT_MILITARY_COMP").alias("MIL_SRVC_BRANCH_CD"),
        F.col("pers_GVT_MIL_RESRVE_CAT").alias("MIL_SRVC_BRANCH_COMPONENT_CD"),
        F.col("pers_GVT_CRED_MIL_SVCE").alias("CRDTBL_MIL_SRVC_PERIOD"),
        F.col("HE_REG_MILITARY").alias("MIL_LV_CUR_FY_HRS"),
        F.col("HE_SPC_MILITARY").alias("MIL_LV_EMERG_CUR_FY_HRS"),
        F.col("o_HE_AWOP_SEP").alias("AWOP_YTD_HRS"),

        # Retained grade fields
        F.col("GVT_RTND_PAY_PLAN").alias("RETND1_PAY_PLAN_CD"),
        F.col("GVT_RTND_GRADE").alias("RETND1_GRADE_CD"),
        F.col("GVT_RTND_STEP").alias("RETND1_STEP_CD"),
        F.col("SAL_ADMIN_PLAN").alias("SAL_ADMIN_PLAN"),
        F.col("PAYGROUP").alias("PAYGROUP"),
        F.col("o_LINE_SEQ").alias("LINE_SEQ"),
        F.col("REG_TEMP").alias("REG_TEMP"),
        F.col("ACTION_DT").alias("AUTHENTICATION_DTE"),

        # From lkp_PS_JPM_JP_ITEMS
        F.col("jpm_JPM_CAT_ITEM_ID").alias("EDUCATION_LEVEL_CD"),
        F.col("jpm_MAJOR_CODE").alias("INSTRUCTIONAL_PROGRAM_CD"),
    )

    count = primary_df.count()
    print(f"  Rows to write: {count:,}")
    assert count > 0, "NWK_ACTION_PRIMARY_TBL would receive 0 rows"

    primary_df.write.format("delta") \
        .mode("append") \
        .saveAsTable(f"{DATABASE}.nwk_action_primary_tbl")

    print(f"  [OK] {count:,} rows written to {DATABASE}.nwk_action_primary_tbl")
    return count


def write_nwk_action_secondary(df, spark):
    """
    Write to NWK_ACTION_SECONDARY_TBL target table.
    Informatica Connectors:
      - #4: exp_MAIN2BIIS -> NWK_ACTION_SECONDARY_TBL (32 fields)
      - #20: exp_PERS_DATA -> NWK_ACTION_SECONDARY_TBL (2 fields: CITY, STATE)

    Target fields (209 total, most are NULL/unmapped).
    """
    print("\n[STEP 8c] Writing NWK_ACTION_SECONDARY_TBL")

    secondary_df = df.select(
        F.col("o_EVENT_ID").alias("EVENT_ID"),

        # TSP fields from exp_MAIN2BIIS
        F.col("o_TSP_VESTING_CD").alias("TSP_VESTING_CD"),
        F.col("HE_PP_UDED_AMT").alias("TSP_EMP_PP_UND_DED_AMT"),
        F.col("HE_NO_TSP_PAYPER").alias("TSP_EMP_PP_UND_DED_PYMT_COUNT"),
        F.col("HE_TSPA_SUB_YR").alias("TSP_PAY_SUBJ_YTD_AMT"),
        F.col("HE_EMP_UDED_AMT").alias("TSP_EMP_UND_DED_OUTSTNDG_AMT"),
        F.col("HE_GVT_UDED_AMT").alias("TSP_GOVT_UND_DED_OUTSTNDG_AMT"),
        F.col("HE_TLTR_NO").alias("TSP_UND_DED_LTR_NUM"),
        F.col("HE_UDED_PAY_CD").alias("TSP_UND_DED_OPTION_CD"),
        F.col("HE_TSP_CANC_CD").alias("TSP_UND_DED_STOP_OPTION_CD"),
        F.col("emp_GVT_SCD_TSP").alias("TSP_SCD_DTE"),

        # Retained grade fields
        F.col("GVT_RTND_PAY_PLAN").alias("RETND1_PAY_PLAN_CD"),
        F.col("GVT_RTND_GRADE").alias("RETND1_GRADE_CD"),
        F.col("GVT_RTND_STEP").cast(StringType()).alias("RETND1_STEP_CD"),
        F.col("emp_GVT_RTND_GRADE_BEG").alias("RETND1_EFF_DTE"),
        F.col("emp_GVT_RTND_GRADE_EXP").alias("RETND1_EXP_DTE"),

        # SCD dates from lkp_PS_GVT_EMPLOYMENT
        F.col("emp_GVT_SCD_RETIRE").alias("RETMT_SCD_DTE"),
        F.col("emp_GVT_SCD_LEO").alias("LEO_SCD_DT"),

        # Award-related
        F.col("o_RECRUITMENT_BONUS_AMT").alias("RECRUITMENT_BONUS_AMT"),
        F.col("o_RECRUITMENT_EXP_DTE").alias("RECRUITMENT_EXP_DTE"),
        F.col("o_RELOCATION_BONUS_AMT").alias("RELOCATION_BONUS_AMT"),
        F.col("o_RELOCATION_EXP_DTE").alias("RELOCATION_EXP_DTE"),
        F.col("o_SEVERANCE_PAY_AMT").alias("SEVERANCE_PAY_AMT"),
        F.col("o_SEVERANCE_PAY_START_DTE").alias("SEVERANCE_PAY_START_DTE"),
        F.col("o_SEVERANCE_TOTAL_AMT").alias("SEVERANCE_PAY_TOT_AMT"),
        F.col("o_TIME_OFF_AWARD_AMT").alias("TIME_OFF_AWARD_AMT"),
        F.col("o_TIME_OFF_GRANTED_HRS").alias("TIME_OFF_GRANTED_HRS"),

        # FEGLI / Annuity
        F.col("o_FEGLI_LIVING_BNFT_CD").alias("FEGLI_LIVING_BNFT_CD"),
        F.col("o_GVT_ANNUITY_OFFSET").alias("GVT_ANNUITY_OFFSET"),

        # LWOP / Buyout
        F.col("o_LWOP_START_DATE").alias("LWOP_START_DTE"),
        F.col("o_BUYOUT_AMT").alias("BUYOUT_AMT"),
        F.col("o_BUYOUT_EFFDT").alias("BUYOUT_EFF_DTE"),

        # Sabbatical
        F.col("emp_GVT_SABBATIC_EXPIR").alias("SES_SABBATICAL_NTE_DTE"),

        # Paygroup
        F.col("PAYGROUP").alias("PAYGROUP"),
    )

    count = secondary_df.count()
    print(f"  Rows to write: {count:,}")
    assert count > 0, "NWK_ACTION_SECONDARY_TBL would receive 0 rows"

    secondary_df.write.format("delta") \
        .mode("append") \
        .saveAsTable(f"{DATABASE}.nwk_action_secondary_tbl")

    print(f"  [OK] {count:,} rows written to {DATABASE}.nwk_action_secondary_tbl")
    return count


# ============================================================================
# MAIN ETL PIPELINE
# ============================================================================
def main():
    """
    Execute the full EHRP2BIIS UPDATE ETL pipeline.
    Follows the Informatica workflow: wf_EHRP2BIIS_UPDATE
      Start --> s_m_EHRP2BIIS_UPDATE (Session)

    Session configuration:
      - Treat source rows as: Insert
      - Commit Type: Target
      - Commit Interval: 10000
      - Commit On End Of File: YES
    """
    print(f"\n{'#' * 70}")
    print("# EHRP2BIIS UPDATE - Main ETL Pipeline")
    print("# Informatica Mapping: m_EHRP2BIIS_UPDATE")
    print(f"# Run Date: {LOAD_DATE}  |  Load ID: {LOAD_ID}")
    print(f"{'#' * 70}\n")

    start_time = datetime.now()
    spark = create_spark_session()
    counts = {}

    try:
        # STEP 1: Source Qualifier (SQ_PS_GVT_JOB)
        sq_df = read_source_tables(spark)

        # STEP 2: Load all lookup tables
        lookups = load_lookup_tables(spark)

        # STEP 3: Apply all 9 lookup joins (broadcast)
        enriched_df = apply_lookup_joins(sq_df, lookups)

        # STEP 4: exp_GET_EFFDT_YEAR (extract year for sequence lookup)
        enriched_df = apply_exp_get_effdt_year(enriched_df)

        # STEP 5: lkp_OLD_SEQUENCE_NUMBER (get starting sequence number)
        enriched_df = apply_lkp_old_sequence_number(enriched_df, lookups)

        # STEP 6: exp_PERS_DATA (personal data pass-through)
        enriched_df = apply_exp_pers_data(enriched_df)

        # STEP 7: exp_MAIN2BIIS (main business logic - all expressions)
        transformed_df = apply_exp_main2biis(enriched_df)

        # Cache the transformed DataFrame since it feeds 3 targets
        transformed_df.cache()
        total_rows = transformed_df.count()
        print(f"\n[INFO] Total transformed rows: {total_rows:,}")

        # Assertion: verify key computed fields
        null_event_ids = transformed_df.filter(F.col("o_EVENT_ID").isNull()).count()
        assert null_event_ids == 0, f"Found {null_event_ids} NULL EVENT_IDs after transformation"

        null_load_dates = transformed_df.filter(F.col("o_LOAD_DATE").isNull()).count()
        assert null_load_dates == 0, f"Found {null_load_dates} NULL LOAD_DATEs"
        print("[OK] Post-transformation assertions passed")

        # STEP 8: Write to all 3 target tables
        counts["ehrp_recs_tracking"] = write_ehrp_recs_tracking(transformed_df, spark)
        counts["nwk_action_primary"] = write_nwk_action_primary(transformed_df, spark)
        counts["nwk_action_secondary"] = write_nwk_action_secondary(transformed_df, spark)

        # Unpersist cached DataFrame
        transformed_df.unpersist()

    except Exception as e:
        print(f"\n[CRITICAL] ETL pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        raise

    # ---------------------------------------------------------------
    # ETL Summary
    # ---------------------------------------------------------------
    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()

    print(f"\n{'=' * 70}")
    print("ETL PIPELINE SUMMARY")
    print(f"{'=' * 70}")
    print(f"  Load ID:          {LOAD_ID}")
    print(f"  Load Date:        {LOAD_DATE}")
    print(f"  Source Rows:      {total_rows:,}")
    print("  Target Counts:")
    for target, cnt in counts.items():
        print(f"    {target}: {cnt:,}")
    print(f"  Elapsed Time:     {elapsed:.1f} seconds")
    print("  Status:           SUCCESS")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
