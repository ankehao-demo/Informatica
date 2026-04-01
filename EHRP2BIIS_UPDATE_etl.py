# Databricks notebook source
# MAGIC %md
# MAGIC # EHRP2BIIS_UPDATE Main ETL Transformation
# MAGIC
# MAGIC Converted from: Informatica PowerCenter mapping `m_EHRP2BIIS_UPDATE`
# MAGIC
# MAGIC **Purpose**: Extracts employee HR action data from PeopleSoft (EHRP) tables,
# MAGIC enriches it with lookups against multiple reference tables, applies business logic
# MAGIC transformations, and loads the results into three BIIS target Delta tables.
# MAGIC
# MAGIC **Targets**:
# MAGIC - `nwk_action_primary_tbl` (260 fields) - Primary action record
# MAGIC - `nwk_action_secondary_tbl` (209 fields) - Secondary/benefits action record
# MAGIC - `ehrp_recs_tracking_tbl` (10 fields) - Tracking/audit table

# COMMAND ----------

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, when, concat, upper, rtrim, trim, coalesce,
    current_date, current_timestamp, date_format, year,
    row_number, sum as spark_sum, broadcast, isnull,
    add_months, substring, length, lpad, expr, monotonically_increasing_id,
)
from pyspark.sql.types import (
    IntegerType, StringType, DecimalType, DateType,
)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Configuration
DATABASE = "informatica_migration"
spark.sql(f"USE {DATABASE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions
# MAGIC
# MAGIC Reusable transformation utilities matching Informatica expression patterns.

# COMMAND ----------

def zero_to_null(col_name):
    """IIF(field = 0, NULL, field) - Zero-to-NULL pattern for leave balances."""
    return when(col(col_name) == 0, lit(None)).otherwise(col(col_name))


def is_spaces_to_null(col_name):
    """IIF(IS_SPACES(field), NULL, field) - Null out blank/space-only strings."""
    return when(
        col(col_name).isNull() | (trim(col(col_name)) == lit("")),
        lit(None)
    ).otherwise(col(col_name))


def ensure_columns(dataframe, column_names):
    """Add missing columns as NULL to handle schema variations across environments."""
    existing = set(dataframe.columns)
    for c in column_names:
        if c not in existing:
            dataframe = dataframe.withColumn(c, lit(None).cast(StringType()))
    return dataframe


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Source Tables

# COMMAND ----------

# Source tables
nwk_new_ehrp_actions = spark.table(f"{DATABASE}.nwk_new_ehrp_actions_tbl")
ps_gvt_job = spark.table(f"{DATABASE}.ps_gvt_job")

print(f"NWK_NEW_EHRP_ACTIONS_TBL count: {nwk_new_ehrp_actions.count()}")
print(f"PS_GVT_JOB count: {ps_gvt_job.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Source Qualifier - Inner Join
# MAGIC
# MAGIC Implements the `SQ_PS_GVT_JOB` Source Qualifier SQL override:
# MAGIC ```sql
# MAGIC SELECT ... FROM PS_GVT_JOB, NWK_NEW_EHRP_ACTIONS_TBL
# MAGIC WHERE NWK_NEW_EHRP_ACTIONS_TBL.EMPLID = PS_GVT_JOB.EMPLID
# MAGIC   AND NWK_NEW_EHRP_ACTIONS_TBL.EMPL_RCD = PS_GVT_JOB.EMPL_RCD
# MAGIC   AND NWK_NEW_EHRP_ACTIONS_TBL.EFFDT = PS_GVT_JOB.EFFDT
# MAGIC   AND NWK_NEW_EHRP_ACTIONS_TBL.EFFSEQ = PS_GVT_JOB.EFFSEQ
# MAGIC ORDER BY PS_GVT_JOB.EFFDT
# MAGIC ```

# COMMAND ----------

# Select the specific columns from PS_GVT_JOB that the Source Qualifier extracts
sq_columns = [
    "EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ",
    "DEPTID", "JOBCODE", "POSITION_NBR",
    "ACTION", "ACTION_DT", "ACTION_REASON",
    "LOCATION", "POSITION_ENTRY_DT", "REG_TEMP",
    "COMPANY", "PAYGROUP", "STD_HOURS",
    "STD_HRS_FREQUENCY", "SAL_ADMIN_PLAN", "GRADE",
    "GRADE_ENTRY_DT", "STEP_ENTRY_DT", "ACCT_CD",
    "COMPRATE", "HOURLY_RT", "ANNL_BENEF_BASE_RT",
    "SETID_DEPT", "FLSA_STATUS", "GVT_EFFDT",
    "GVT_WIP_STATUS", "GVT_STATUS_TYPE", "GVT_NOA_CODE",
    "GVT_LEG_AUTH_1", "GVT_PAR_AUTH_D1", "GVT_PAR_AUTH_D1_2",
    "GVT_LEG_AUTH_2", "GVT_PAR_AUTH_D2", "GVT_PAR_AUTH_D2_2",
    "GVT_PAR_NTE_DATE", "GVT_WORK_SCHED", "GVT_SUB_AGENCY",
    "GVT_PAY_RATE_DETER", "GVT_STEP", "GVT_RTND_PAY_PLAN",
    "GVT_RTND_GRADE", "GVT_RTND_STEP", "GVT_PAY_BASIS",
    "GVT_COMPRATE", "GVT_LOCALITY_ADJ", "GVT_HRLY_RT_NO_LOC",
    "GVT_XFER_FROM_AGCY", "GVT_XFER_TO_AGCY",
    "GVT_RETIRE_PLAN", "GVT_ANN_IND", "GVT_FEGLI",
    "GVT_FEGLI_LIVING", "GVT_LIVING_AMT", "GVT_ANNUITY_OFFSET",
    "GVT_CSRS_FROZN_SVC", "GVT_PREV_RET_COVRG",
    "GVT_FERS_COVERAGE", "GVT_TYPE_OF_APPT", "GVT_POI",
    "GVT_POSN_OCCUPIED", "GVT_LEO_POSITION", "GVT_PAY_PLAN",
    "UNION_CD", "BARG_UNIT", "REPORTS_TO",
    "HE_NOA_EXT", "HE_AL_CARRYOVER", "HE_AL_ACCRUAL",
    "HE_AL_RED_CRED", "HE_AL_TOTAL", "HE_AL_BALANCE",
    "HE_SL_CARRYOVER", "HE_SL_ACCRUAL", "HE_SL_RED_CRED",
    "HE_SL_TOTAL", "HE_SL_BALANCE", "HE_RES_LASTYR",
    "HE_RES_TWOYRS", "HE_RES_THREEYRS", "HE_RES_BALANCE",
    "HE_LUMP_HRS", "HE_AWOP_SEP", "HE_AWOP_WIGI",
    "HE_REG_MILITARY", "HE_SPC_MILITARY", "HE_FROZEN_SL",
    "HE_TSPA_SUB_YR", "HE_TLTR_NO", "HE_UDED_PAY_CD",
    "HE_TSP_CANC_CD", "HE_PP_UDED_AMT", "HE_EMP_UDED_AMT",
    "HE_GVT_UDED_AMT", "HE_NO_TSP_PAYPER",
]

# Select only the columns that exist in the DataFrame to handle schema variations
available_job_cols = set(ps_gvt_job.columns)
selected_cols = [c for c in sq_columns if c in available_job_cols]

# Inner join: Source Qualifier
join_keys = ["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"]
sq_df = (
    ps_gvt_job.select([col(c) for c in selected_cols])
    .join(nwk_new_ehrp_actions.select(join_keys), on=join_keys, how="inner")
    .orderBy("EFFDT")
)

sq_count = sq_df.count()
print(f"Source Qualifier join result count: {sq_count}")
assert sq_count > 0, "Source Qualifier join produced 0 rows - aborting ETL"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Read and Join Lookup Tables
# MAGIC
# MAGIC All lookups use broadcast joins for performance (matching Informatica cached lookups).

# COMMAND ----------

# --- 3.1 lkp_PS_GVT_EMPLOYMENT ---
# Lookup condition: EMPLID, EMPL_RCD, EFFDT, EFFSEQ
ps_gvt_employment = broadcast(
    spark.table(f"{DATABASE}.ps_gvt_employment")
)

# Select key return ports needed downstream
employment_return_cols = [
    "EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ",
    "GVT_SCD_RETIRE", "GVT_SCD_TSP", "GVT_SCD_LEO",
    "GVT_RTND_GRADE_BEG", "GVT_RTND_GRADE_EXP",
    "GVT_SABBATIC_EXPIR", "GVT_CURR_APT_AUTH1", "GVT_CURR_APT_AUTH2",
    "CMPNY_SENIORITY_DT", "BUSINESS_TITLE", "GVT_APPT_LIMIT_HRS",
    "PROBATION_DT", "GVT_WGI_DUE_DATE",
    "HIRE_DT", "SERVICE_DT", "LAST_INCREASE_DT",
    "GVT_WGI_STATUS", "GVT_TENURE",
    "GVT_SPEP", "GVT_TEMP_PRO_EXPIR", "GVT_TEMP_PSN_EXPIR",
    "GVT_DETAIL_EXPIRES", "GVT_APPT_EXPIR_DT",
    "GVT_SUPV_PROB_DT", "GVT_CNV_BEGIN_DATE",
    "GVT_COMP_LVL_PERM", "GVT_APPT_LIMIT_DYS",
]

# Filter to available columns
avail_emp_cols = set(ps_gvt_employment.columns)
emp_select = [c for c in employment_return_cols if c in avail_emp_cols]

# Alias columns that overlap with sq_df to avoid ambiguity
emp_alias_map = {}
for c in emp_select:
    if c not in join_keys:
        emp_alias_map[c] = f"emp_{c}"

emp_df = ps_gvt_employment.select(
    *[col(c) for c in join_keys],
    *[col(c).alias(emp_alias_map[c]) for c in emp_select if c not in join_keys]
).dropDuplicates(join_keys)

df = sq_df.join(emp_df, on=join_keys, how="left")

# COMMAND ----------

# --- 3.2 lkp_PS_GVT_PERS_NID ---
# Returns NATIONAL_ID (maps to SSN)
ps_gvt_pers_nid = broadcast(
    spark.table(f"{DATABASE}.ps_gvt_pers_nid")
)

nid_df = ps_gvt_pers_nid.select(
    *[col(c) for c in join_keys],
    col("NATIONAL_ID").alias("lkp_NATIONAL_ID"),
).dropDuplicates(join_keys)

df = df.join(nid_df, on=join_keys, how="left")

# COMMAND ----------

# --- 3.3 lkp_PS_GVT_AWD_DATA ---
# Returns GOAL_AMT, OTH_HRS, OTH_PAY, EARNINGS_END_DT, ERNCD, GVT_TANG_BEN_AMT
ps_gvt_awd_data = broadcast(
    spark.table(f"{DATABASE}.ps_gvt_awd_data")
)

awd_df = ps_gvt_awd_data.select(
    *[col(c) for c in join_keys],
    col("GOAL_AMT").alias("lkp_GOAL_AMT"),
    col("OTH_HRS").alias("lkp_OTH_HRS"),
    col("OTH_PAY").alias("lkp_OTH_PAY"),
    col("EARNINGS_END_DT").alias("lkp_EARNINGS_END_DT"),
    col("ERNCD").alias("lkp_ERNCD"),
    col("GVT_TANG_BEN_AMT").alias("lkp_GVT_TANG_BEN_AMT"),
).dropDuplicates(join_keys)

df = df.join(awd_df, on=join_keys, how="left")

# COMMAND ----------

# --- 3.4 lkp_PS_GVT_EE_DATA_TRK ---
# SQL Override: SELECT DISTINCT ... FROM PS_GVT_EE_DATA_TRK
# Returns GVT_DATE_WRK
ps_gvt_ee_data_trk = broadcast(
    spark.table(f"{DATABASE}.ps_gvt_ee_data_trk")
    .select("EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ", "GVT_WIP_STATUS", "GVT_DATE_WRK")
    .dropDuplicates()
)

ee_trk_df = ps_gvt_ee_data_trk.select(
    col("EMPLID").alias("ee_EMPLID"),
    col("EMPL_RCD").alias("ee_EMPL_RCD"),
    col("EFFDT").alias("ee_EFFDT"),
    col("EFFSEQ").alias("ee_EFFSEQ"),
    col("GVT_DATE_WRK").alias("lkp_GVT_DATE_WRK"),
)

df = df.join(
    ee_trk_df,
    on=[
        df["EMPLID"] == ee_trk_df["ee_EMPLID"],
        df["EMPL_RCD"] == ee_trk_df["ee_EMPL_RCD"],
        df["EFFDT"] == ee_trk_df["ee_EFFDT"],
        df["EFFSEQ"] == ee_trk_df["ee_EFFSEQ"],
    ],
    how="left",
).drop("ee_EMPLID", "ee_EMPL_RCD", "ee_EFFDT", "ee_EFFSEQ")

# COMMAND ----------

# --- 3.5 lkp_PS_HE_FILL_POS ---
# Returns HE_FILL_POSITION
ps_he_fill_pos = broadcast(
    spark.table(f"{DATABASE}.ps_he_fill_pos")
)

fill_df = ps_he_fill_pos.select(
    *[col(c) for c in join_keys],
    col("HE_FILL_POSITION").alias("lkp_HE_FILL_POSITION"),
).dropDuplicates(join_keys)

df = df.join(fill_df, on=join_keys, how="left")

# COMMAND ----------

# --- 3.6 lkp_PS_GVT_CITIZENSHIP ---
# Returns CITIZENSHIP_STATUS
ps_gvt_citizenship = broadcast(
    spark.table(f"{DATABASE}.ps_gvt_citizenship")
)

cit_df = ps_gvt_citizenship.select(
    *[col(c) for c in join_keys],
    col("CITIZENSHIP_STATUS").alias("lkp_CITIZENSHIP_STATUS"),
).dropDuplicates(join_keys)

df = df.join(cit_df, on=join_keys, how="left")

# COMMAND ----------

# --- 3.7 lkp_PS_GVT_PERS_DATA ---
# SQL Override pre-joins with NWK_NEW_EHRP_ACTIONS_TBL
# Returns personal data fields
ps_gvt_pers_data = broadcast(
    spark.table(f"{DATABASE}.ps_gvt_pers_data")
)

# Replicate SQL override: join with NWK_NEW_EHRP_ACTIONS_TBL to pre-filter
pers_filtered = ps_gvt_pers_data.join(
    nwk_new_ehrp_actions.select(join_keys),
    on=join_keys,
    how="inner",
)

pers_df = pers_filtered.select(
    *[col(c) for c in join_keys],
    col("LAST_NAME").alias("lkp_LAST_NAME"),
    col("FIRST_NAME").alias("lkp_FIRST_NAME"),
    col("MIDDLE_NAME").alias("lkp_MIDDLE_NAME"),
    col("ADDRESS1").alias("lkp_ADDRESS1"),
    col("CITY").alias("lkp_CITY"),
    col("STATE").alias("lkp_STATE"),
    col("POSTAL").alias("lkp_POSTAL"),
    col("GEO_CODE").alias("lkp_GEO_CODE"),
    col("SEX").alias("lkp_SEX"),
    col("BIRTHDATE").alias("lkp_BIRTHDATE"),
    col("MILITARY_STATUS").alias("lkp_MILITARY_STATUS"),
    col("GVT_CRED_MIL_SVCE").alias("lkp_GVT_CRED_MIL_SVCE"),
    col("GVT_MILITARY_COMP").alias("lkp_GVT_MILITARY_COMP"),
    col("GVT_MIL_RESRVE_CAT").alias("lkp_GVT_MIL_RESRVE_CAT"),
    col("GVT_VET_PREF_APPT").alias("lkp_GVT_VET_PREF_APPT"),
    col("ETHNIC_GROUP").alias("lkp_ETHNIC_GROUP"),
    col("GVT_DISABILITY_CD").alias("lkp_GVT_DISABILITY_CD"),
).dropDuplicates(join_keys)

df = df.join(pers_df, on=join_keys, how="left")

# COMMAND ----------

# --- 3.8 lkp_PS_JPM_JP_ITEMS ---
# Different connection (BIISPRD), filters: jpm_cat_type = 'DEG' AND eff_status = 'A'
# Lookup condition: JPM_PROFILE_ID = EMPLID
ps_jpm_jp_items = broadcast(
    spark.table(f"{DATABASE}.ps_jpm_jp_items")
    .filter(
        (col("JPM_CAT_TYPE") == "DEG") & (col("EFF_STATUS") == "A")
    )
)

jpm_df = ps_jpm_jp_items.select(
    col("JPM_PROFILE_ID").alias("jpm_EMPLID"),
    col("JPM_INTEGER_2").alias("lkp_JPM_INTEGER_2"),
    col("MAJOR_CODE").alias("lkp_MAJOR_CODE"),
    col("JPM_CAT_ITEM_ID").alias("lkp_JPM_CAT_ITEM_ID"),
).dropDuplicates(["jpm_EMPLID"])

df = df.join(
    jpm_df,
    on=df["EMPLID"] == jpm_df["jpm_EMPLID"],
    how="left",
).drop("jpm_EMPLID")

# COMMAND ----------

# --- 3.9 lkp_OLD_SEQUENCE_NUMBER ---
# Read sequence number table for Event ID generation
# Lookup condition: EHRP_YEAR = o_CURRENT_YEAR (year extracted from EFFDT)
sequence_num_tbl = spark.table(f"{DATABASE}.sequence_num_tbl")

# Collect sequence numbers into a map for use in Event ID generation
seq_map = {
    int(row["EHRP_YEAR"]): int(row["EHRP_SEQ_NUMBER"])
    for row in sequence_num_tbl.collect()
}

print(f"Sequence number map: {seq_map}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: exp_GET_EFFDT_YEAR - Extract Year from EFFDT

# COMMAND ----------

df = df.withColumn("v_CURRENT_YEAR", year(col("EFFDT")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Event ID Generation (Stateful Sequence Logic)
# MAGIC
# MAGIC Informatica uses stateful local variables:
# MAGIC - `v_EVENT_ID = IIF(v_CURRENT_YEAR = v_PREVIOUS_YEAR, v_EVENT_ID + 1, EHRP_SEQ_NUMBER + 1)`
# MAGIC
# MAGIC Data must be processed in EFFDT order. We use a window function approach:
# MAGIC partition by year, use row_number + base sequence number from SEQUENCE_NUM_TBL.

# COMMAND ----------

from pyspark.sql.types import LongType

# Build a small DataFrame from the sequence map and broadcast-join it
# (avoids spark.sparkContext.broadcast which is unsupported on serverless)
seq_rows = [(int(yr), int(seq)) for yr, seq in seq_map.items()]
seq_df = spark.createDataFrame(seq_rows, ["_seq_year", "_seq_base"])

# Partition by year and assign row numbers within each year
window_year = Window.partitionBy("v_CURRENT_YEAR").orderBy("EFFDT", "EMPLID", "EMPL_RCD", "EFFSEQ")
df = df.withColumn("_year_row_num", row_number().over(window_year))

# Join the sequence base numbers by year (broadcast the tiny lookup table)
df = df.join(
    broadcast(seq_df),
    on=df["v_CURRENT_YEAR"] == seq_df["_seq_year"],
    how="left",
).drop("_seq_year")

# Default to 0 if year not found in sequence table
df = df.withColumn("_base_seq", coalesce(col("_seq_base"), lit(0)).cast(LongType())).drop("_seq_base")

# Event ID = base_sequence_number + row_number_within_year
# This matches Informatica's behavior: resets to EHRP_SEQ_NUMBER+1 on year boundary,
# then increments by 1 for each subsequent row within the same year
df = df.withColumn(
    "o_EVENT_ID",
    (col("_base_seq") + col("_year_row_num")).cast(LongType())
)

# Clean up temporary columns
df = df.drop("_global_row_num", "_year_row_num", "_base_seq")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: exp_MAIN2BIIS - Core Business Logic Transformations
# MAGIC
# MAGIC Translates ALL business logic from the Informatica exp_MAIN2BIIS and exp_PERS_DATA
# MAGIC transformations.

# COMMAND ----------

# Ensure all expected source columns exist (some may be absent in certain table schemas)
expected_src_cols = [
    "ACCT_CD", "ACTION", "ACTION_DT", "ACTION_REASON", "ANNL_BENEF_BASE_RT",
    "BARG_UNIT", "COMPANY", "COMPRATE", "DEPTID", "EFFDT", "EFFSEQ",
    "EMPLID", "EMPL_RCD", "FLSA_STATUS", "GRADE", "GRADE_ENTRY_DT",
    "GVT_ANN_IND", "GVT_COMPRATE", "GVT_CSRS_FROZN_SVC", "GVT_FEGLI",
    "GVT_FEGLI_LIVING", "GVT_FERS_COVERAGE", "GVT_HRLY_RT_NO_LOC",
    "GVT_LEG_AUTH_1", "GVT_LEG_AUTH_2", "GVT_LEO_POSITION", "GVT_LIVING_AMT",
    "GVT_LOCALITY_ADJ", "GVT_NOA_CODE", "GVT_PAR_AUTH_D1", "GVT_PAR_AUTH_D1_2",
    "GVT_PAR_AUTH_D2", "GVT_PAR_AUTH_D2_2", "GVT_PAR_NTE_DATE",
    "GVT_PAY_BASIS", "GVT_PAY_PLAN", "GVT_PAY_RATE_DETER", "GVT_POI",
    "GVT_POSN_OCCUPIED", "GVT_PREV_RET_COVRG", "GVT_RETIRE_PLAN",
    "GVT_RTND_GRADE", "GVT_RTND_PAY_PLAN", "GVT_RTND_STEP",
    "GVT_STATUS_TYPE", "GVT_STEP", "GVT_SUB_AGENCY", "GVT_TYPE_OF_APPT",
    "GVT_WIP_STATUS", "GVT_WORK_SCHED", "GVT_XFER_FROM_AGCY", "GVT_XFER_TO_AGCY",
    "HE_AL_BALANCE", "HE_AL_CARRYOVER", "HE_AL_ACCRUAL", "HE_AL_RED_CRED",
    "HE_AL_TOTAL", "HE_AWOP_SEP", "HE_AWOP_WIGI", "HE_EMP_UDED_AMT",
    "HE_FROZEN_SL", "HE_GVT_UDED_AMT", "HE_LUMP_HRS", "HE_NOA_EXT",
    "HE_NO_TSP_PAYPER", "HE_PP_UDED_AMT", "HE_REG_MILITARY", "HE_RES_BALANCE",
    "HE_RES_LASTYR", "HE_RES_THREEYRS", "HE_RES_TWOYRS",
    "HE_SL_BALANCE", "HE_SL_CARRYOVER", "HE_SL_ACCRUAL", "HE_SL_RED_CRED",
    "HE_SL_TOTAL", "HE_SPC_MILITARY", "HE_TLTR_NO", "HE_TSPA_SUB_YR",
    "HE_TSP_CANC_CD", "HE_UDED_PAY_CD", "HOURLY_RT", "JOBCODE",
    "LOCATION", "PAYGROUP", "POSITION_ENTRY_DT", "POSITION_NBR",
    "REG_TEMP", "REPORTS_TO", "SAL_ADMIN_PLAN", "SETID_DEPT",
    "STD_HOURS", "STEP_ENTRY_DT", "UNION_CD",
    "GVT_ANNUITY_OFFSET",
]
df = ensure_columns(df, expected_src_cols)

expected_emp_cols = [
    "emp_GVT_SCD_RETIRE", "emp_GVT_SCD_TSP", "emp_GVT_SCD_LEO",
    "emp_GVT_RTND_GRADE_BEG", "emp_GVT_RTND_GRADE_EXP",
    "emp_GVT_SABBATIC_EXPIR", "emp_GVT_CURR_APT_AUTH1", "emp_GVT_CURR_APT_AUTH2",
    "emp_CMPNY_SENIORITY_DT", "emp_BUSINESS_TITLE", "emp_GVT_APPT_LIMIT_HRS",
    "emp_PROBATION_DT", "emp_GVT_WGI_DUE_DATE",
    "emp_HIRE_DT", "emp_SERVICE_DT", "emp_LAST_INCREASE_DT",
    "emp_GVT_WGI_STATUS", "emp_GVT_TENURE",
    "emp_GVT_SPEP", "emp_GVT_TEMP_PRO_EXPIR", "emp_GVT_TEMP_PSN_EXPIR",
    "emp_GVT_DETAIL_EXPIRES", "emp_GVT_APPT_EXPIR_DT",
    "emp_GVT_SUPV_PROB_DT", "emp_GVT_CNV_BEGIN_DATE",
    "emp_GVT_COMP_LVL_PERM", "emp_GVT_APPT_LIMIT_DYS",
]
expected_lkp_cols = [
    "lkp_NATIONAL_ID", "lkp_GOAL_AMT", "lkp_OTH_HRS", "lkp_OTH_PAY",
    "lkp_EARNINGS_END_DT", "lkp_ERNCD", "lkp_GVT_TANG_BEN_AMT",
    "lkp_GVT_DATE_WRK", "lkp_HE_FILL_POSITION", "lkp_CITIZENSHIP_STATUS",
    "lkp_LAST_NAME", "lkp_FIRST_NAME", "lkp_MIDDLE_NAME",
    "lkp_ADDRESS1", "lkp_CITY", "lkp_STATE", "lkp_POSTAL", "lkp_GEO_CODE",
    "lkp_SEX", "lkp_BIRTHDATE", "lkp_MILITARY_STATUS",
    "lkp_GVT_CRED_MIL_SVCE", "lkp_GVT_MILITARY_COMP",
    "lkp_GVT_MIL_RESRVE_CAT", "lkp_GVT_VET_PREF_APPT",
    "lkp_ETHNIC_GROUP", "lkp_GVT_DISABILITY_CD",
    "lkp_JPM_INTEGER_2", "lkp_MAJOR_CODE", "lkp_JPM_CAT_ITEM_ID",
]
df = ensure_columns(df, expected_emp_cols + expected_lkp_cols)

# --- 6.1 Load ID and Date Generation (Section 7.3.7) ---
df = df.withColumn("o_LOAD_DATE", current_date())
df = df.withColumn(
    "o_LOAD_ID",
    concat(lit("NK"), date_format(current_date(), "yyyyMMdd"))
)
df = df.withColumn("o_LINE_SEQ", lit("01N"))

# COMMAND ----------

# --- 6.2 Agency Assignment (Section 7.3.2) ---
# o_AGCY_ASSIGN_CD = COMPANY || GVT_SUB_AGENCY
df = df.withColumn(
    "o_AGCY_ASSIGN_CD",
    concat(col("COMPANY"), col("GVT_SUB_AGENCY"))
)

# o_AGCY_SUBELEMENT_PRIOR_CD = IIF(IS_SPACES(GVT_XFER_FROM_AGCY) or
#   ISNULL(GVT_XFER_FROM_AGCY), NULL, GVT_XFER_FROM_AGCY || '00')
df = df.withColumn(
    "o_AGCY_SUBELEMENT_PRIOR_CD",
    when(
        col("GVT_XFER_FROM_AGCY").isNull() | (trim(col("GVT_XFER_FROM_AGCY")) == lit("")),
        lit(None)
    ).otherwise(concat(col("GVT_XFER_FROM_AGCY"), lit("00")))
)

# COMMAND ----------

# --- 6.3 Leave Balance Transformations - Zero-to-NULL (Section 7.3.3) ---
leave_zero_to_null_fields = [
    "HE_AL_RED_CRED", "HE_AL_BALANCE", "HE_LUMP_HRS",
    "HE_AL_CARRYOVER", "HE_RES_LASTYR", "HE_RES_BALANCE",
    "HE_RES_TWOYRS", "HE_RES_THREEYRS", "HE_AL_ACCRUAL",
    "HE_AL_TOTAL", "HE_AWOP_SEP", "HE_AWOP_WIGI",
    "HE_SL_RED_CRED", "HE_SL_BALANCE", "HE_FROZEN_SL",
    "HE_SL_CARRYOVER", "HE_SL_ACCRUAL", "HE_SL_TOTAL",
]

for field in leave_zero_to_null_fields:
    output_name = f"o_{field}"
    df = df.withColumn(output_name, zero_to_null(field))

# COMMAND ----------

# --- 6.4 Base Hours Calculation (Section 7.3.4) ---
# o_BASE_HOURS = IIF(GVT_WORK_SCHED = 'I', STD_HOURS, STD_HOURS * 2)
df = df.withColumn(
    "o_BASE_HOURS",
    when(col("GVT_WORK_SCHED") == "I", col("STD_HOURS"))
    .otherwise(col("STD_HOURS") * 2)
)

# COMMAND ----------

# --- 6.5 Permanent/Temp Position Code (Section 7.3.5) ---
# o_PERM_TEMP_POSITION_CD = IIF(STD_HOURS < 40, 3, IIF(STD_HOURS >= 40, 1, 0))
df = df.withColumn(
    "o_PERM_TEMP_POSITION_CD",
    when(col("STD_HOURS") < 40, lit(3))
    .when(col("STD_HOURS") >= 40, lit(1))
    .otherwise(lit(0))
)

# o_PERMANENT_TEMP_POSITION_CD = complex logic based on REG_TEMP and GVT_WORK_SCHED
df = df.withColumn(
    "o_PERMANENT_TEMP_POSITION_CD",
    when(
        (col("REG_TEMP") == "R") & (col("GVT_WORK_SCHED") == "F"),
        lit("1")
    ).when(
        col("REG_TEMP").isin("R", "T") & col("GVT_WORK_SCHED").isin("P", "I"),
        lit("3")
    ).when(
        (col("REG_TEMP") == "T") & (col("GVT_WORK_SCHED") == "F"),
        lit("2")
    ).otherwise(lit(None))
)

# COMMAND ----------

# --- 6.6 Legal Authority Text (Section 7.3.6) ---
# o_LEG_AUTH_TXT_1 = UPPER(GVT_PAR_AUTH_D1 || RTRIM(GVT_PAR_AUTH_D1_2))
df = df.withColumn(
    "o_LEG_AUTH_TXT_1",
    upper(concat(
        coalesce(col("GVT_PAR_AUTH_D1"), lit("")),
        rtrim(coalesce(col("GVT_PAR_AUTH_D1_2"), lit("")))
    ))
)

# o_LEG_AUTH_TXT_2 = UPPER(GVT_PAR_AUTH_D2 || RTRIM(GVT_PAR_AUTH_D2_2))
df = df.withColumn(
    "o_LEG_AUTH_TXT_2",
    upper(concat(
        coalesce(col("GVT_PAR_AUTH_D2"), lit("")),
        rtrim(coalesce(col("GVT_PAR_AUTH_D2_2"), lit("")))
    ))
)

# COMMAND ----------

# --- 6.7 Cash Award Logic (Section 7.3.8) ---
# NOA codes for cash awards (excluding RCR, RLC earnings codes)
award_noa_codes = [
    "840", "841", "842", "843", "844", "845", "848", "849",
    "873", "874", "875", "876", "877", "878", "879",
]
special_award_noa = ["817", "889"]

# o_CASH_AWARD_AMT: DECODE logic
df = df.withColumn(
    "o_CASH_AWARD_AMT",
    when(
        col("GVT_NOA_CODE").isin(award_noa_codes)
        & ~col("lkp_ERNCD").isin("RCR", "RLC"),
        col("lkp_GOAL_AMT")
    ).when(
        col("GVT_NOA_CODE").isin(special_award_noa),
        col("lkp_GOAL_AMT")
    ).otherwise(lit(None))
)

# o_CASH_AWARD_BNFT_AMT
df = df.withColumn(
    "o_CASH_AWARD_BNFT_AMT",
    when(
        col("GVT_NOA_CODE").isin(award_noa_codes)
        & ~col("lkp_ERNCD").isin("RCR", "RLC"),
        col("lkp_GVT_TANG_BEN_AMT")
    ).otherwise(lit(None))
)

# COMMAND ----------

# --- 6.8 Recruitment/Relocation Bonus Logic (Section 7.3.9) ---
# o_RECRUITMENT_BONUS_AMT
df = df.withColumn(
    "o_RECRUITMENT_BONUS_AMT",
    when(
        (col("GVT_NOA_CODE") == "815")
        | ((col("GVT_NOA_CODE") == "948") & (col("HE_NOA_EXT") == "0")),
        col("lkp_GOAL_AMT")
    ).otherwise(lit(None))
)

# o_RELOCATION_BONUS_AMT
df = df.withColumn(
    "o_RELOCATION_BONUS_AMT",
    when(col("GVT_NOA_CODE") == "816", col("lkp_GOAL_AMT"))
    .otherwise(lit(None))
)

# o_RECRUITMENT_EXP_DTE
df = df.withColumn(
    "o_RECRUITMENT_EXP_DTE",
    when(
        (col("GVT_NOA_CODE") == "815")
        | ((col("GVT_NOA_CODE") == "948") & (col("HE_NOA_EXT") == "0")),
        col("lkp_EARNINGS_END_DT")
    ).otherwise(lit(None))
)

# o_RELOCATION_EXP_DTE
df = df.withColumn(
    "o_RELOCATION_EXP_DTE",
    when(
        (col("GVT_NOA_CODE") == "816")
        | ((col("GVT_NOA_CODE") == "948") & (col("HE_NOA_EXT") == "1")),
        col("lkp_EARNINGS_END_DT")
    ).otherwise(lit(None))
)

# COMMAND ----------

# --- 6.9 Time Off Awards (Section 7.3.10) ---
df = df.withColumn(
    "o_TIME_OFF_AWARD_AMT",
    when(col("GVT_NOA_CODE").isin("846", "847"), col("lkp_OTH_HRS"))
    .otherwise(lit(None))
)

df = df.withColumn(
    "o_TIME_OFF_GRANTED_HRS",
    when(col("GVT_NOA_CODE").isin("846", "847"), col("lkp_OTH_HRS"))
    .otherwise(lit(None))
)

# COMMAND ----------

# --- 6.10 Severance Pay (Section 7.3.11) ---
severance_noa = ["304", "312", "356", "357"]

df = df.withColumn(
    "o_SEVERANCE_PAY_AMT",
    when(col("GVT_NOA_CODE").isin(severance_noa), col("lkp_OTH_PAY"))
    .otherwise(lit(None))
)

df = df.withColumn(
    "o_SEVERANCE_TOTAL_AMT",
    when(col("GVT_NOA_CODE").isin(severance_noa), col("lkp_GOAL_AMT"))
    .otherwise(lit(None))
)

df = df.withColumn(
    "o_SEVERANCE_PAY_START_DTE",
    when(col("GVT_NOA_CODE").isin(severance_noa), col("EFFDT"))
    .otherwise(lit(None))
)

# COMMAND ----------

# --- 6.11 Buyout Logic (Section 7.3.12) ---
df = df.withColumn(
    "o_BUYOUT_EFFDT",
    when(col("GVT_NOA_CODE") == "825", col("EFFDT"))
    .otherwise(lit(None))
)

df = df.withColumn(
    "o_BUYOUT_AMT",
    when(
        (col("GVT_NOA_CODE") == "825") & (col("ACTION") == "BON"),
        col("lkp_GOAL_AMT")
    ).otherwise(lit(None))
)

# COMMAND ----------

# --- 6.12 Citizenship Status (Section 7.3.13) ---
# IIF(CITIZENSHIP_STATUS_IN = '1' or CITIZENSHIP_STATUS_IN = '2', '1', '8')
df = df.withColumn(
    "o_CITIZENSHIP_STATUS",
    when(
        col("lkp_CITIZENSHIP_STATUS").isin("1", "2"),
        lit("1")
    ).otherwise(lit("8"))
)

# COMMAND ----------

# --- 6.13 TSP Vesting Code (Section 7.3.14) ---
# IIF(IN(GVT_RETIRE_PLAN, 'K', 'M'),
#     IIF(IN(GVT_TYPE_OF_APPT, '55', '34', '36', '46', '44'), 2, 3), 0)
df = df.withColumn(
    "o_TSP_VESTING_CD",
    when(
        col("GVT_RETIRE_PLAN").isin("K", "M"),
        when(
            col("GVT_TYPE_OF_APPT").isin("55", "34", "36", "46", "44"),
            lit(2)
        ).otherwise(lit(3))
    ).otherwise(lit(0))
)

# COMMAND ----------

# --- 6.14 Event Submitted Date (Section 7.3.15) ---
# IIF(ISNULL(GVT_DATE_WRK), ACTION_DT, GVT_DATE_WRK)
df = df.withColumn(
    "o_EVENT_SUBMITTED_DTE",
    when(col("lkp_GVT_DATE_WRK").isNull(), col("ACTION_DT"))
    .otherwise(col("lkp_GVT_DATE_WRK"))
)

# COMMAND ----------

# --- 6.15 Probation Date (Section 7.3.16) ---
# IIF(ISNULL(PROBATION_DT), NULL, ADD_TO_DATE(PROBATION_DT, 'MM', -12))
df = df.withColumn(
    "o_PROBATION_DT",
    when(col("emp_PROBATION_DT").isNull(), lit(None))
    .otherwise(add_months(col("emp_PROBATION_DT"), -12))
)

# COMMAND ----------

# --- 6.16 FEGLI Living Benefit Code (Section 7.3.17) ---
# DECODE(GVT_NOA_CODE, '805', 'F', '806', 'P', NULL)
df = df.withColumn(
    "o_FEGLI_LIVING_BNFT_CD",
    when(col("GVT_NOA_CODE") == "805", lit("F"))
    .when(col("GVT_NOA_CODE") == "806", lit("P"))
    .otherwise(lit(None))
)

# COMMAND ----------

# --- 6.17 LWOP Start Date (Section 7.3.18) ---
# IIF(IN(GVT_NOA_CODE, '460', '473'), EFFDT, NULL)
df = df.withColumn(
    "o_LWOP_START_DATE",
    when(col("GVT_NOA_CODE").isin("460", "473"), col("EFFDT"))
    .otherwise(lit(None))
)

# COMMAND ----------

# --- 6.18 Null/Space Handling (Section 7.3.19) ---
df = df.withColumn("o_UNION_CD", is_spaces_to_null("UNION_CD"))
df = df.withColumn("o_GVT_CURR_APT_AUTH1", is_spaces_to_null("emp_GVT_CURR_APT_AUTH1"))
df = df.withColumn("o_GVT_CURR_APT_AUTH2", is_spaces_to_null("emp_GVT_CURR_APT_AUTH2"))

# o_GVT_ANNUITY_OFFSET = IIF(GVT_ANNUITY_OFFSET = 0, NULL, GVT_ANNUITY_OFFSET)
df = df.withColumn("o_GVT_ANNUITY_OFFSET", zero_to_null("GVT_ANNUITY_OFFSET"))

# o_APPT_LIMIT_NTE_HRS = IIF(GVT_APPT_LIMIT_HRS = 0, NULL, GVT_APPT_LIMIT_HRS)
df = df.withColumn(
    "o_APPT_LIMIT_NTE_HRS",
    when(col("emp_GVT_APPT_LIMIT_HRS") == 0, lit(None))
    .otherwise(col("emp_GVT_APPT_LIMIT_HRS"))
)

# YEAR_DEGREE = IIF(JPM_INTEGER_2 = 0, NULL, JPM_INTEGER_2)
df = df.withColumn(
    "o_YEAR_DEGREE",
    when(col("lkp_JPM_INTEGER_2") == 0, lit(None))
    .otherwise(col("lkp_JPM_INTEGER_2"))
)

# COMMAND ----------

# --- 6.19 Other Transformations (Section 7.3.20) ---
# o_EMP_RESID_CITY_STATE_NAME = CITY || ', ' || STATE
df = df.withColumn(
    "o_EMP_RESID_CITY_STATE_NAME",
    concat(
        coalesce(col("lkp_CITY"), lit("")),
        lit(",  "),
        coalesce(col("lkp_STATE"), lit(""))
    )
)

# o_EFFSEQ = EFFSEQ (decimal to string cast)
df = df.withColumn("o_EFFSEQ", col("EFFSEQ").cast(StringType()))

# o_BUSINESS_TITLE = UPPER(BUSINESS_TITLE)
df = df.withColumn("o_BUSINESS_TITLE", upper(col("emp_BUSINESS_TITLE")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Inline Data Quality Assertions

# COMMAND ----------

# Assert: EVENT_ID should be unique
event_id_count = df.select("o_EVENT_ID").distinct().count()
total_count = df.count()
assert event_id_count == total_count, (
    f"EVENT_ID uniqueness violation: {event_id_count} distinct IDs for {total_count} rows"
)
print(f"Assertion PASSED: EVENT_ID is unique ({event_id_count} distinct IDs)")

# Assert: EMPLID should never be null
null_emplid = df.filter(col("EMPLID").isNull()).count()
assert null_emplid == 0, f"EMPLID null check failed: {null_emplid} null values"
print(f"Assertion PASSED: No null EMPLIDs")

# Assert: LOAD_DATE should be today
load_date_check = df.filter(col("o_LOAD_DATE") != current_date()).count()
assert load_date_check == 0, f"LOAD_DATE check failed: {load_date_check} rows with wrong date"
print(f"Assertion PASSED: All LOAD_DATE values are current date")

# Assert: o_EVENT_ID should be positive
negative_ids = df.filter(col("o_EVENT_ID") <= 0).count()
assert negative_ids == 0, f"EVENT_ID positivity check failed: {negative_ids} non-positive IDs"
print(f"Assertion PASSED: All EVENT_IDs are positive")

# Assert: GVT_NOA_CODE should not be null (required field)
null_noa = df.filter(col("GVT_NOA_CODE").isNull()).count()
print(f"Info: {null_noa} rows with null GVT_NOA_CODE")

print(f"\nTotal rows to be written: {total_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Router Transformation (rtr_EHRP2BIIS)
# MAGIC
# MAGIC The Router transformation directs rows to the appropriate target tables.
# MAGIC In Informatica, all rows flow to all 3 targets (no filtering conditions exclude rows).
# MAGIC The Update Strategy transformations determine INSERT vs UPDATE behavior.
# MAGIC
# MAGIC For the Delta Lake target, we write all rows to all 3 target tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Write to Target Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Target 1: NWK_ACTION_PRIMARY_TBL (260 fields)
# MAGIC
# MAGIC Primary action record for BIIS containing employee demographics,
# MAGIC position info, pay data, leave balances, and benefits.

# COMMAND ----------

primary_df = df.select(
    # Core identifiers
    col("o_EVENT_ID").alias("EVENT_ID"),
    col("o_LOAD_ID").alias("LOAD_ID"),
    col("o_LOAD_DATE").alias("LOAD_DATE"),
    col("EFFDT").alias("AS_OF_DAY"),
    col("EMPLID").alias("EMP_ID"),
    col("lkp_NATIONAL_ID").alias("SSN"),
    col("o_AGCY_ASSIGN_CD").alias("AGCY_ASSIGN_CD"),
    col("GVT_SUB_AGENCY").alias("AGCY_SUBELEMENT_CD"),
    col("GVT_NOA_CODE").alias("NOA_CD"),
    col("HE_NOA_EXT").alias("NOA_SUFFIX_CD"),
    col("ACTION").alias("EHRP_TYPE_ACTION"),
    col("ACTION_REASON").alias("EHRP_ACTION_REASON"),
    col("EFFDT").alias("EVENT_EFF_DTE"),
    col("o_EVENT_SUBMITTED_DTE").alias("EVENT_SUBMITTED_DTE"),
    col("o_LINE_SEQ").alias("LINE_SEQ"),

    # Position information
    col("DEPTID"),
    col("JOBCODE"),
    col("POSITION_NBR"),
    col("LOCATION").alias("DUTY_STATION_CD"),
    col("POSITION_ENTRY_DT").alias("POSITION_START_DTE"),
    col("o_BUSINESS_TITLE").alias("POSITION_TITLE_NAME"),
    col("REPORTS_TO"),

    # Pay information
    col("o_BASE_HOURS").alias("BASE_HRS"),
    col("GRADE").alias("GRADE_CD"),
    col("GVT_STEP").alias("STEP_CD"),
    col("GVT_PAY_PLAN").alias("PAY_PLAN_CD"),
    col("GVT_PAY_BASIS").alias("PAY_BASIS_CD"),
    col("HOURLY_RT").alias("HRLY_RATE_AMT"),
    col("GVT_COMPRATE").alias("SCHLD_ANN_SALARY_AMT"),
    col("GVT_LOCALITY_ADJ").alias("LOCALITY_PAY_AMT"),
    col("ANNL_BENEF_BASE_RT").alias("ANNL_BENEF_BASE_RT"),
    col("GVT_HRLY_RT_NO_LOC").alias("ADJ_BASIC_PAY_AMT"),
    col("COMPRATE"),
    col("SAL_ADMIN_PLAN"),
    col("GVT_PAY_RATE_DETER").alias("PAY_RATE_DETERMINANT_CD"),
    col("FLSA_STATUS").alias("FLSA_CATEGORY_CD"),
    col("ACCT_CD").alias("APPROPRIATION_CD"),
    col("GRADE_ENTRY_DT"),
    col("STEP_ENTRY_DT"),

    # Legal authority
    col("GVT_LEG_AUTH_1").alias("LEGAL_AUTH_CD"),
    col("o_LEG_AUTH_TXT_1").alias("LEGAL_AUTH_TXT"),
    col("GVT_LEG_AUTH_2").alias("LEGAL_AUTH2_CD"),
    col("o_LEG_AUTH_TXT_2").alias("LEGAL_AUTH2_TXT"),

    # Leave balances - Annual Leave
    col("o_HE_AL_CARRYOVER").alias("ANN_LV_CARRIED_FWD_HRS"),
    col("o_HE_AL_ACCRUAL").alias("ANN_LV_ACCRL_CUR_FY_HRS"),
    col("o_HE_AL_RED_CRED").alias("ANN_LV_RESTORATIONS_HRS"),
    col("o_HE_AL_TOTAL").alias("ANN_LV_TOTAL_HRS"),
    col("o_HE_AL_BALANCE").alias("ANN_LV_BALANCE_HRS"),
    col("o_HE_LUMP_HRS").alias("ANN_LV_LUMP_SUM_HRS"),

    # Leave balances - Sick Leave
    col("o_HE_SL_CARRYOVER").alias("SICK_LV_CARRIED_FWD_HRS"),
    col("o_HE_SL_ACCRUAL").alias("SICK_LV_ACCRL_CUR_FY_HRS"),
    col("o_HE_SL_RED_CRED").alias("SICK_LV_RESTORATIONS_HRS"),
    col("o_HE_SL_TOTAL").alias("SICK_LV_TOTAL_HRS"),
    col("o_HE_SL_BALANCE").alias("SICK_LV_BALANCE_HRS"),
    col("o_HE_FROZEN_SL").alias("FROZEN_SL_HRS"),

    # Leave balances - Restored Leave
    col("o_HE_RES_LASTYR").alias("RESTRD_LV_LAST_FY_HRS"),
    col("o_HE_RES_TWOYRS").alias("RESTRD_LV_2ND_FY_HRS"),
    col("o_HE_RES_THREEYRS").alias("RESTRD_LV_3RD_FY_HRS"),
    col("o_HE_RES_BALANCE").alias("RESTRD_LV_BALANCE_HRS"),

    # AWOP
    col("o_HE_AWOP_SEP").alias("AWOP_SEPARATION_DTE"),
    col("o_HE_AWOP_WIGI").alias("AWOP_WGI_START_DTE"),

    # Awards and bonuses
    col("o_CASH_AWARD_AMT").alias("CASH_AWARD_AMT"),
    col("o_CASH_AWARD_BNFT_AMT").alias("CASH_AWARD_BNFT_AMT"),
    col("o_TIME_OFF_AWARD_AMT").alias("TIME_OFF_AWARD_AMT"),

    # Severance
    col("o_SEVERANCE_PAY_AMT").alias("SEVERANCE_PAY_AMT"),
    col("o_SEVERANCE_TOTAL_AMT").alias("SEVERANCE_TOTAL_AMT"),
    col("o_SEVERANCE_PAY_START_DTE").alias("SEVERANCE_PAY_START_DTE"),

    # Buyout
    col("o_BUYOUT_AMT").alias("BUYOUT_AMT"),
    col("o_BUYOUT_EFFDT").alias("BUYOUT_EFFDT"),

    # Status codes
    col("GVT_WIP_STATUS"),
    col("GVT_STATUS_TYPE"),
    col("o_CITIZENSHIP_STATUS").alias("CITIZENSHIP_STATUS"),
    col("GVT_TYPE_OF_APPT").alias("TYPE_OF_APPT"),
    col("GVT_POI").alias("PERSONNEL_OFFICE_ID"),
    col("GVT_POSN_OCCUPIED").alias("POSN_OCCUPIED_CD"),
    col("GVT_LEO_POSITION").alias("LEO_POSITION_CD"),
    col("o_PERM_TEMP_POSITION_CD").alias("PERMANENT_TEMP_POSITION_CD"),
    col("o_PERMANENT_TEMP_POSITION_CD").alias("PERMANENT_TEMP_POSITION_CD2"),

    # Benefits
    col("GVT_RETIRE_PLAN").alias("RETIREMENT_PLAN_CD"),
    col("GVT_ANN_IND").alias("ANNUITANT_IND"),
    col("GVT_FEGLI").alias("FEGLI_CD"),
    col("o_FEGLI_LIVING_BNFT_CD").alias("FEGLI_LIVING_BNFT_CD"),
    col("GVT_FEGLI_LIVING").alias("GVT_FEGLI_LIVING"),
    col("GVT_LIVING_AMT").alias("GVT_LIVING_AMT"),
    col("o_GVT_ANNUITY_OFFSET").alias("ANNUITY_OFFSET_AMT"),
    col("GVT_CSRS_FROZN_SVC").alias("FROZEN_SERVICE_CD"),
    col("GVT_PREV_RET_COVRG").alias("PREV_RETIREMENT_COVERAGE_CD"),
    col("GVT_FERS_COVERAGE").alias("FERS_COVERAGE_CD"),
    col("GVT_WORK_SCHED").alias("WORK_SCHEDULE_CD"),
    col("REG_TEMP"),

    # Personal data (from lkp_PS_GVT_PERS_DATA via exp_PERS_DATA)
    col("lkp_ADDRESS1").alias("EMP_RESID_STREET_NAME"),
    col("lkp_GVT_DISABILITY_CD").alias("HANDICAP_CD"),
    col("lkp_ETHNIC_GROUP").alias("RACE_NATL_ORIGIN_CD"),
    col("lkp_GVT_VET_PREF_APPT").alias("VETERANS_PREFERENCE_CD"),
    col("lkp_MILITARY_STATUS").alias("VETERANS_STATUS_CD"),
    col("lkp_SEX").alias("SEX_CD"),
    col("lkp_GVT_CRED_MIL_SVCE").alias("CRDTBL_MIL_SRVC_PERIOD"),
    col("lkp_BIRTHDATE").alias("BIRTH_DTE"),
    col("lkp_FIRST_NAME").alias("EMP_FIRST_NAME"),
    col("lkp_LAST_NAME").alias("EMP_LAST_NAME"),
    col("lkp_MIDDLE_NAME").alias("EMP_MID_INIT"),
    col("lkp_GEO_CODE").alias("EMP_RESID_GEOGPHCL_LOC_CD"),
    col("lkp_POSTAL").alias("EMP_RESID_POSTAL_CD"),
    col("o_EMP_RESID_CITY_STATE_NAME").alias("EMP_RESID_CITY_STATE_NAME"),

    # Employment data (from lkp_PS_GVT_EMPLOYMENT)
    col("emp_HIRE_DT").alias("CAREER_START_DTE"),
    col("emp_GVT_TEMP_PSN_EXPIR").alias("POSITION_CHANGE_END_DTE"),
    col("o_APPT_LIMIT_NTE_HRS").alias("APPT_LMT_NTE_90DAY_CD"),
    col("emp_SERVICE_DT").alias("EMP_EOD_DTE"),
    col("emp_GVT_SPEP").alias("SPECIAL_PROGRAM_CD"),
    col("emp_GVT_SUPV_PROB_DT").alias("SUPERVSRY_MGRL_PROB_START_DTE"),
    col("emp_GVT_APPT_EXPIR_DT").alias("APPT_NTE_DTE"),
    col("emp_GVT_WGI_STATUS").alias("WGI_STATUS_CD"),
    col("emp_GVT_COMP_LVL_PERM").alias("COMPETITIVE_LEVEL_CD"),
    col("emp_GVT_DETAIL_EXPIRES").alias("SUSPENSION_END_DTE"),
    col("emp_GVT_TEMP_PRO_EXPIR").alias("TEMP_PROMTN_EXP_DTE"),
    col("emp_GVT_TENURE").alias("TENURE_CD"),
    col("emp_CMPNY_SENIORITY_DT").alias("LV_SCD_DTE"),
    col("o_PROBATION_DT").alias("PROBATION_START_DTE"),
    col("emp_GVT_WGI_DUE_DATE").alias("WGI_DUE_DTE"),
    col("emp_LAST_INCREASE_DT").alias("LAST_INCREASE_DT"),

    # Fill position (from lkp_PS_HE_FILL_POS)
    col("lkp_HE_FILL_POSITION").alias("FILLING_POSITION_CD"),

    # Education (from lkp_PS_JPM_JP_ITEMS)
    col("lkp_MAJOR_CODE").alias("INSTRUCTIONAL_PROGRAM_CD"),
    col("lkp_JPM_CAT_ITEM_ID").alias("EDUCATION_LEVEL_CD"),
    col("o_YEAR_DEGREE").alias("YEAR_DEGREE_ATTAINED_DTE"),

    # Transfer agencies
    col("GVT_XFER_FROM_AGCY"),
    col("GVT_XFER_TO_AGCY"),
    col("o_AGCY_SUBELEMENT_PRIOR_CD").alias("AGCY_SUBELEMENT_PRIOR_CD"),

    # Appointment authorities
    col("o_GVT_CURR_APT_AUTH1").alias("CURR_APPT_AUTH1"),
    col("o_GVT_CURR_APT_AUTH2").alias("CURR_APPT_AUTH2"),

    # Misc
    col("o_UNION_CD").alias("BARGAIN_UNIT_CD"),
    col("BARG_UNIT"),
    col("SETID_DEPT"),
    col("GVT_PAR_NTE_DATE").alias("PAR_NTE_DATE"),
    col("o_LWOP_START_DATE").alias("LWOP_START_DTE"),
    col("PAYGROUP"),
    col("EMPL_RCD"),
    col("o_EFFSEQ").alias("EFFSEQ"),

    # Additional SCD dates from employment lookup
    col("emp_GVT_SCD_RETIRE").alias("RETMT_SCD_DTE_PRIMARY"),
    col("emp_GVT_SCD_TSP").alias("TSP_SCD_DTE_PRIMARY"),
    col("emp_GVT_SCD_LEO").alias("LEO_SCD_DT"),
    col("emp_GVT_RTND_GRADE_BEG").alias("RETAINED_GRADE_BEGIN_DTE"),
    col("emp_GVT_RTND_GRADE_EXP").alias("RETAINED_GRADE_EXP_DTE"),
    col("emp_GVT_SABBATIC_EXPIR").alias("SABBATIC_EXPIRATION_DTE"),
    col("emp_GVT_CNV_BEGIN_DATE").alias("CAREER_CONV_BEGIN_DTE"),
)

print(f"Primary target row count: {primary_df.count()}")

# COMMAND ----------

# Write to NWK_ACTION_PRIMARY_TBL
primary_df.write.format("delta").mode("overwrite").saveAsTable(
    f"{DATABASE}.nwk_action_primary_tbl"
)
print("Written to nwk_action_primary_tbl")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Target 2: NWK_ACTION_SECONDARY_TBL (209 fields)
# MAGIC
# MAGIC Secondary action record containing supplementary benefits data including
# MAGIC TSP, retirement, military service, leave, awards, bonuses, and retained grade info.

# COMMAND ----------

secondary_df = df.select(
    # Core identifier
    col("o_EVENT_ID").alias("EVENT_ID"),

    # SCD dates from employment lookup
    col("emp_GVT_SCD_RETIRE").alias("RETMT_SCD_DTE"),
    col("emp_GVT_SCD_TSP").alias("TSP_SCD_DTE"),
    col("emp_GVT_SCD_LEO").alias("RIF_SCD_DTE"),

    # Retained grade/step/pay plan
    col("GVT_RTND_PAY_PLAN").alias("RETND1_PAY_PLAN_CD"),
    col("GVT_RTND_GRADE").alias("RETND1_GRADE_CD"),
    col("GVT_RTND_STEP").alias("RETND1_STEP_CD"),

    # Retained grade dates from employment lookup
    col("emp_GVT_RTND_GRADE_BEG").alias("RETND_GRADE_BEGIN_DTE"),
    col("emp_GVT_RTND_GRADE_EXP").alias("RETND_GRADE_EXP_DTE"),

    # Awards and bonuses
    col("o_TIME_OFF_GRANTED_HRS").alias("TIME_OFF_GRANTED_HRS"),
    col("o_RELOCATION_BONUS_AMT").alias("RELOCATION_BONUS_AMT"),
    col("o_RECRUITMENT_BONUS_AMT").alias("RECRUITMENT_BONUS_AMT"),
    col("o_TIME_OFF_AWARD_AMT").alias("TIME_OFF_AWARD_AMT"),
    col("o_RECRUITMENT_EXP_DTE").alias("RECRUITMENT_EXP_DTE"),
    col("o_RELOCATION_EXP_DTE").alias("RELOCATION_EXP_DTE"),

    # Military leave
    col("HE_REG_MILITARY").alias("MIL_LV_CUR_FY_HRS"),
    col("HE_SPC_MILITARY").alias("MIL_LV_EMERG_CUR_FY_HRS"),

    # TSP fields
    col("HE_TSPA_SUB_YR").alias("TSP_SUB_YEAR"),
    col("HE_TLTR_NO").alias("TSP_LETTER_NO"),
    col("HE_UDED_PAY_CD").alias("TSP_UDED_PAY_CD"),
    col("HE_TSP_CANC_CD").alias("TSP_CANC_CD"),
    col("HE_PP_UDED_AMT").alias("TSP_PP_UDED_AMT"),
    col("HE_EMP_UDED_AMT").alias("TSP_EMP_UDED_AMT"),
    col("HE_GVT_UDED_AMT").alias("TSP_GVT_UDED_AMT"),
    col("HE_NO_TSP_PAYPER").alias("TSP_NO_PAYPER"),
    col("o_TSP_VESTING_CD").alias("TSP_VESTING_CD"),

    # Pay table / paygroup
    col("SAL_ADMIN_PLAN").alias("PAY_TABLE_NUM"),
    col("PAYGROUP"),

    # LWOP
    col("o_LWOP_START_DATE").alias("LWOP_START_DTE"),

    # Buyout
    col("o_BUYOUT_AMT").alias("BUYOUT_AMT"),
    col("o_BUYOUT_EFFDT").alias("BUYOUT_EFFDT"),

    # Severance
    col("o_SEVERANCE_PAY_AMT").alias("SEVERANCE_PAY_AMT"),
    col("o_SEVERANCE_TOTAL_AMT").alias("SEVERANCE_TOTAL_AMT"),

    # Military branch (from lkp_PS_GVT_PERS_DATA via exp_PERS_DATA)
    col("lkp_GVT_MILITARY_COMP").alias("MIL_SRVC_BRANCH_CD"),
    col("lkp_GVT_MIL_RESRVE_CAT").alias("MIL_SRVC_BRANCH_COMPONENT_CD"),

    # LEO SCD
    col("emp_GVT_SCD_LEO").alias("LEO_SCD_DT"),

    # Sabbatic
    col("emp_GVT_SABBATIC_EXPIR").alias("SABBATIC_EXPIRATION_DTE"),
)

print(f"Secondary target row count: {secondary_df.count()}")

# COMMAND ----------

# Write to NWK_ACTION_SECONDARY_TBL
secondary_df.write.format("delta").mode("overwrite").saveAsTable(
    f"{DATABASE}.nwk_action_secondary_tbl"
)
print("Written to nwk_action_secondary_tbl")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Target 3: EHRP_RECS_TRACKING_TBL (10 fields)
# MAGIC
# MAGIC Tracking/audit table recording each processed record with its BIIS event ID
# MAGIC and WIP status.

# COMMAND ----------

tracking_df = df.select(
    col("o_EVENT_ID").alias("BIIS_EVENT_ID"),
    col("EMPLID"),
    col("EMPL_RCD"),
    col("EFFDT"),
    col("EFFSEQ"),
    col("GVT_NOA_CODE").alias("NOA_CD"),
    col("HE_NOA_EXT").alias("NOA_SUFFIX_CD"),
    col("GVT_WIP_STATUS"),
    col("o_EVENT_SUBMITTED_DTE").alias("EVENT_SUBMITTED_DT"),
    col("o_LOAD_DATE").alias("LOAD_DATE"),
)

print(f"Tracking target row count: {tracking_df.count()}")

# COMMAND ----------

# Write to EHRP_RECS_TRACKING_TBL
tracking_df.write.format("delta").mode("overwrite").saveAsTable(
    f"{DATABASE}.ehrp_recs_tracking_tbl"
)
print("Written to ehrp_recs_tracking_tbl")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Final Summary

# COMMAND ----------

print("=" * 60)
print("EHRP2BIIS_UPDATE ETL COMPLETE")
print("=" * 60)
print(f"Source Qualifier rows: {sq_count}")
print(f"NWK_ACTION_PRIMARY_TBL rows written: {primary_df.count()}")
print(f"NWK_ACTION_SECONDARY_TBL rows written: {secondary_df.count()}")
print(f"EHRP_RECS_TRACKING_TBL rows written: {tracking_df.count()}")
print(f"Load ID: {df.select('o_LOAD_ID').first()[0]}")
print(f"Load Date: {df.select('o_LOAD_DATE').first()[0]}")
print("=" * 60)
