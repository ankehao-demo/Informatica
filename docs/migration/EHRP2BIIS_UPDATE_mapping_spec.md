# m_EHRP2BIIS_UPDATE -- mapping specification

Generated from `XML/EHRP2BIIS_UPDATE` by `tools/infa_xml_spec.py` + `tools/render_mapping_spec.py`. Do not edit by hand.

* Folder `EHRP2BIIS`, workflow `wf_EHRP2BIIS_UPDATE`, session `s_m_EHRP2BIIS_UPDATE`, mapping `m_EHRP2BIIS_UPDATE`
* 13 transformations, 874 TRANSFORMFIELDs, 589 connectors
* Session: Treat source rows as = `Insert`, Commit Type = `Target`, Commit Interval = `10000`, Commit On End Of File = `YES`, Recovery Strategy = `Fail task and continue workflow`
* Schedule: start 9/28/2018 12:00, recurring `{'DAYS': '1', 'HOURS': '0', 'MINUTES': '0'}` (daily)
* No Router, Filter or Update Strategy transformation exists: every row is an INSERT. The port uses Delta `MERGE` on deterministic keys so reruns replace the load slice instead of duplicating it (see `pipelines/ehrp2biis/writers.py`).
* Reconciliation keys: `LOAD_ID = 'NK' || YYYYMMDD` and `LOAD_DATE = trunc(sysdate)` are preserved exactly (sysdate = the run's logical date).

## 1. Stage order (PowerCenter -> Spark)

| # | Instance | Type | Spark implementation |
|---|---|---|---|
| 1 | `SQ_PS_GVT_JOB` | Source Qualifier | `source_qualifier`: PS_GVT_JOB INNER JOIN NWK_NEW_EHRP_ACTIONS_TBL on 4 keys |
| 2 | `exp_GET_EFFDT_YEAR` | Expression | `expression`: input ports -> variable ports in order -> output ports (catalog) |
| 3 | `lkp_PS_GVT_AWD_DATA` | Lookup Procedure | `lookup`: LEFT JOIN on the lookup condition, one row per key (Use Any Value) |
| 4 | `lkp_PS_GVT_CITIZENSHIP` | Lookup Procedure | `lookup`: LEFT JOIN on the lookup condition, one row per key (Use Any Value) |
| 5 | `lkp_PS_GVT_EE_DATA_TRK` | Lookup Procedure | `lookup`: LEFT JOIN on the lookup condition, one row per key (Use Any Value) |
| 6 | `lkp_PS_GVT_EMPLOYMENT` | Lookup Procedure | `lookup`: LEFT JOIN on the lookup condition, one row per key (Use Any Value) |
| 7 | `lkp_PS_GVT_PERS_DATA` | Lookup Procedure | `lookup`: LEFT JOIN on the lookup condition, one row per key (Use Any Value) |
| 8 | `lkp_PS_GVT_PERS_NID` | Lookup Procedure | `lookup`: LEFT JOIN on the lookup condition, one row per key (Use Any Value) |
| 9 | `lkp_PS_HE_FILL_POS` | Lookup Procedure | `lookup`: LEFT JOIN on the lookup condition, one row per key (Use Any Value) |
| 10 | `lkp_PS_JPM_JP_ITEMS` | Lookup Procedure | `lookup`: LEFT JOIN on the lookup condition, one row per key (Use Any Value) |
| 11 | `lkp_OLD_SEQUENCE_NUMBER` | Lookup Procedure | `lookup`: LEFT JOIN on the lookup condition, one row per key (Use Any Value) |
| 12 | `exp_PERS_DATA` | Expression | `expression`: input ports -> variable ports in order -> output ports (catalog) |
| 13 | `exp_MAIN2BIIS` | Expression | `expression`: input ports -> variable ports in order -> output ports (catalog) |
| - | targets | Relational Writer | `conform_targets` (cast/truncate to target type, reject NOT NULL / overflow) + `writers.merge_targets` |

## 2. Function translation

| Informatica | Spark SQL | PySpark | Semantics preserved |
|---|---|---|---|
| `IIF(c, a, b)` | `CASE WHEN c THEN a ELSE b END` | `F.when(c, a).otherwise(b)` | NULL condition = FALSE -> b |
| `DECODE(TRUE, c1, v1, ..., d)` | `CASE WHEN c1 THEN v1 ... ELSE d END` | `infa.decode_true` | first TRUE wins; NULL conditions never match |
| `DECODE(x, s1, r1, ..., d)` | `CASE x WHEN s1 THEN r1 ... ELSE d END` | `infa.decode_value` | NULL x never matches |
| `IN(x, a, b)` | `x IN (a, b)` | `col.isin(a, b)` | NULL x -> NULL (three-valued) |
| `NOT(...)` | `NOT (...)` | `~col` | NOT NULL -> NULL |
| `IS_SPACES(x)` | `x RLIKE '^[ \t\n\r\f\v]*$'` | `infa.is_spaces` | NULL -> NULL; '' and ' ' -> TRUE |
| `ISNULL(x)` | `x IS NULL` | `col.isNull()` |  |
| `a \|\| b` | `CASE WHEN a IS NULL AND b IS NULL THEN NULL ELSE concat(coalesce(a,''), coalesce(b,'')) END` | `infa.concat` | NULL operand = '' (Spark concat would return NULL) |
| `RTRIM(x) / UPPER(x)` | `rtrim(x) / upper(x)` | `F.rtrim / F.upper` | RTRIM trims blanks only |
| `GET_DATE_PART(d, 'YYYY'\|'MM'\|'DD')` | `year(d) / month(d) / day(d)` | `F.year / F.month / F.dayofmonth` |  |
| `GET_DATE_PART(SYSDATE, 'MM')` | `month(:run_date)` | `F.month(run_ts)` | SYSDATE pinned to the run's logical date |
| `trunc(sysdate)` | `CAST(trunc(:run_date,'DD') AS TIMESTAMP)` | `F.date_trunc('DD', run_ts)` | LOAD_DATE / AS_OF_DAY |
| `ADD_TO_DATE(d, 'MM', n)` | `d + make_interval(0, n)` | `infa.add_to_date_months` | day clamped to month end, time kept |
| `TO_CHAR(int)` | `CAST(x AS STRING)` | `infa.to_char_int` |  |
| `implicit string -> number` | `leading numeric part, else 0` | `infa.string_to_number` | ' ' -> 0 (TO_DECIMAL rules) |

## 3. Sources and Source Qualifier

* `NKNIGHT.NWK_NEW_EHRP_ACTIONS_TBL` (4 columns)
* `EHRP.PS_GVT_JOB` (246 columns)

SQ override (verbatim):

```sql
SELECT PS_GVT_JOB.EMPLID, PS_GVT_JOB.EMPL_RCD, PS_GVT_JOB.EFFDT, PS_GVT_JOB.EFFSEQ, PS_GVT_JOB.DEPTID, PS_GVT_JOB.JOBCODE, PS_GVT_JOB.POSITION_NBR, PS_GVT_JOB.ACTION, PS_GVT_JOB.ACTION_DT, PS_GVT_JOB.ACTION_REASON, PS_GVT_JOB.LOCATION, PS_GVT_JOB.POSITION_ENTRY_DT, PS_GVT_JOB.REG_TEMP, PS_GVT_JOB.COMPANY, PS_GVT_JOB.PAYGROUP, PS_GVT_JOB.STD_HOURS, PS_GVT_JOB.STD_HRS_FREQUENCY, PS_GVT_JOB.SAL_ADMIN_PLAN, PS_GVT_JOB.GRADE, PS_GVT_JOB.GRADE_ENTRY_DT, PS_GVT_JOB.STEP_ENTRY_DT, PS_GVT_JOB.ACCT_CD, PS_GVT_JOB.COMPRATE, PS_GVT_JOB.HOURLY_RT, PS_GVT_JOB.ANNL_BENEF_BASE_RT, PS_GVT_JOB.SETID_DEPT, PS_GVT_JOB.FLSA_STATUS, PS_GVT_JOB.GVT_EFFDT, PS_GVT_JOB.GVT_WIP_STATUS, PS_GVT_JOB.GVT_STATUS_TYPE, PS_GVT_JOB.GVT_NOA_CODE, PS_GVT_JOB.GVT_LEG_AUTH_1, PS_GVT_JOB.GVT_PAR_AUTH_D1, PS_GVT_JOB.GVT_PAR_AUTH_D1_2, PS_GVT_JOB.GVT_LEG_AUTH_2, PS_GVT_JOB.GVT_PAR_AUTH_D2, PS_GVT_JOB.GVT_PAR_AUTH_D2_2, PS_GVT_JOB.GVT_PAR_NTE_DATE, PS_GVT_JOB.GVT_WORK_SCHED, PS_GVT_JOB.GVT_SUB_AGENCY, PS_GVT_JOB.GVT_PAY_RATE_DETER, PS_GVT_JOB.GVT_STEP, PS_GVT_JOB.GVT_RTND_PAY_PLAN, PS_GVT_JOB.GVT_RTND_GRADE, PS_GVT_JOB.GVT_RTND_STEP, PS_GVT_JOB.GVT_PAY_BASIS, PS_GVT_JOB.GVT_COMPRATE, PS_GVT_JOB.GVT_LOCALITY_ADJ, PS_GVT_JOB.GVT_HRLY_RT_NO_LOC, PS_GVT_JOB.GVT_XFER_FROM_AGCY, PS_GVT_JOB.GVT_XFER_TO_AGCY, PS_GVT_JOB.GVT_RETIRE_PLAN, PS_GVT_JOB.GVT_ANN_IND, PS_GVT_JOB.GVT_FEGLI, PS_GVT_JOB.GVT_FEGLI_LIVING, PS_GVT_JOB.GVT_LIVING_AMT, PS_GVT_JOB.GVT_ANNUITY_OFFSET, PS_GVT_JOB.GVT_CSRS_FROZN_SVC, PS_GVT_JOB.GVT_PREV_RET_COVRG, PS_GVT_JOB.GVT_FERS_COVERAGE, PS_GVT_JOB.GVT_TYPE_OF_APPT, PS_GVT_JOB.GVT_POI, PS_GVT_JOB.GVT_POSN_OCCUPIED, PS_GVT_JOB.GVT_LEO_POSITION, PS_GVT_JOB.GVT_PAY_PLAN, PS_GVT_JOB.UNION_CD, PS_GVT_JOB.BARG_UNIT, PS_GVT_JOB.REPORTS_TO, PS_GVT_JOB.HE_NOA_EXT, PS_GVT_JOB.HE_AL_CARRYOVER, PS_GVT_JOB.HE_AL_ACCRUAL, PS_GVT_JOB.HE_AL_RED_CRED, PS_GVT_JOB.HE_AL_TOTAL, PS_GVT_JOB.HE_AL_BALANCE, PS_GVT_JOB.HE_SL_CARRYOVER, PS_GVT_JOB.HE_SL_ACCRUAL, PS_GVT_JOB.HE_SL_RED_CRED, PS_GVT_JOB.HE_SL_TOTAL, PS_GVT_JOB.HE_SL_BALANCE, PS_GVT_JOB.HE_RES_LASTYR, PS_GVT_JOB.HE_RES_TWOYRS, PS_GVT_JOB.HE_RES_THREEYRS, PS_GVT_JOB.HE_RES_BALANCE, PS_GVT_JOB.HE_LUMP_HRS, PS_GVT_JOB.HE_AWOP_SEP, PS_GVT_JOB.HE_AWOP_WIGI, PS_GVT_JOB.HE_REG_MILITARY, PS_GVT_JOB.HE_SPC_MILITARY, PS_GVT_JOB.HE_FROZEN_SL, PS_GVT_JOB.HE_TSPA_SUB_YR, PS_GVT_JOB.HE_TLTR_NO, PS_GVT_JOB.HE_UDED_PAY_CD, PS_GVT_JOB.HE_TSP_CANC_CD, PS_GVT_JOB.HE_PP_UDED_AMT, PS_GVT_JOB.HE_EMP_UDED_AMT, PS_GVT_JOB.HE_GVT_UDED_AMT, PS_GVT_JOB.HE_NO_TSP_PAYPER 
FROM
 PS_GVT_JOB, NWK_NEW_EHRP_ACTIONS_TBL 
WHERE
 NWK_NEW_EHRP_ACTIONS_TBL.EMPLID = PS_GVT_JOB.EMPLID AND
NWK_NEW_EHRP_ACTIONS_TBL.EMPL_RCD = PS_GVT_JOB.EMPL_RCD AND
NWK_NEW_EHRP_ACTIONS_TBL.EFFDT = PS_GVT_JOB.EFFDT AND
NWK_NEW_EHRP_ACTIONS_TBL.EFFSEQ  = PS_GVT_JOB.EFFSEQ
ORDER BY PS_GVT_JOB.EFFDT
```

Note: `ORDER BY PS_GVT_JOB.EFFDT` is what makes the stateful `v_EVENT_ID` sequence reproducible; the port orders by `EFFDT, EMPLID, EMPL_RCD, EFFSEQ` (tie-break documented in ASSUMPTIONS.md).

## 4. Lookup procedures

| Lookup | Table | Condition | Policy | Conn | Case-sensitive | SQL override | Output ports |
|---|---|---|---|---|---|---|---|
| `lkp_OLD_SEQUENCE_NUMBER` | SEQUENCE_NUM_TBL | `EHRP_YEAR = o_CURRENT_YEAR` | Use Any Value | $Source | NO | - | 1: EHRP_SEQ_NUMBER |
| `lkp_PS_GVT_EMPLOYMENT` | PS_GVT_EMPLOYMENT | `EMPLID = EMPLID_IN AND EMPL_RCD = EMPL_RCD_IN AND EFFDT = EFFDT_IN AND EFFSEQ = EFFSEQ_IN` | Use Any Value | $Source | NO | - | 81: EMPLID, EMPL_RCD, EFFDT, EFFSEQ, BENEFIT_RCD_NBR, HOME_HOST_CLASS, HIRE_DT, REHIRE_DT, CMPNY_SENIORITY_DT, SERVICE_DT, PROF_EXPERIENCE_DT, LAST_VERIFICATN_DT ... |
| `lkp_PS_GVT_PERS_NID` | PS_GVT_PERS_NID | `EMPLID = EMPLID_IN AND EMPL_RCD = EMPL_RCD_IN AND EFFDT = EFFDT_IN AND EFFSEQ = EFFSEQ_IN` | Use Any Value | $Source | NO | - | 9: EMPLID, EMPL_RCD, EFFDT, EFFSEQ, COUNTRY, NATIONAL_ID_TYPE, NATIONAL_ID, SSN_KEY_FRA, PRIMARY_NID |
| `lkp_PS_GVT_AWD_DATA` | PS_GVT_AWD_DATA | `EMPLID = EMPLID_IN AND EMPL_RCD = EMPL_RCD_IN AND EFFDT = EFFDT_IN AND EFFSEQ = EFFSEQ_IN` | Use Any Value | $Source | NO | - | 26: EMPLID, EMPL_RCD, EFFDT, EFFSEQ, GVT_AWD_CLASS, OTH_PAY, OTH_HRS, GOAL_AMT, EARNINGS_END_DT, GVT_USE_BY_DATE, GVT_AWARD_GROUP, GVT_TANG_BEN_AMT ... |
| `lkp_PS_GVT_EE_DATA_TRK` | PS_GVT_EE_DATA_TRK | `EMPLID = EMPLID_IN AND EMPL_RCD = EMPL_RCD_IN AND EFFDT = EFFDT_IN AND EFFSEQ = EFFSEQ_IN` | Use Any Value | $Source | NO | `SELECT DISTINCT A.GVT_DATE_WRK as GVT_DATE_WRK, A.EMPLID as EMPLID, A.EMPL_RCD as EMPL_RCD, A.EFFDT as EFFDT, A.EFFSEQ as EFFSEQ, A.GVT_WIP_STATUS as GVT_WIP_STATUS FROM PS_GVT_EE_DATA_TRK A` | 1: GVT_DATE_WRK |
| `lkp_PS_HE_FILL_POS` | PS_HE_FILL_POS | `EMPLID = EMPLID_IN AND EMPL_RCD = EMPL_RCD_IN AND EFFDT = EFFDT_IN AND EFFSEQ = EFFSEQ_IN` | Use Any Value | $Source | NO | - | 1: HE_FILL_POSITION |
| `lkp_PS_GVT_CITIZENSHIP` | PS_GVT_CITIZENSHIP | `EMPLID = EMPLID_IN AND EMPL_RCD = EMPL_RCD_IN AND EFFDT = EFFDT_IN AND EFFSEQ = EFFSEQ_IN` | Use Any Value | $Source | NO | - | 1: CITIZENSHIP_STATUS |
| `lkp_PS_GVT_PERS_DATA` | PS_GVT_PERS_DATA | `EMPLID = EMPLID_IN AND EMPL_RCD = EMPL_RCD_IN AND EFFDT = EFFDT_IN AND EFFSEQ = EFFSEQ_IN` | Use Any Value | $Source | NO | `SELECT p.LAST_NAME as LAST_NAME, p.FIRST_NAME as FIRST_NAME, p.MIDDLE_NAME as MIDDLE_NAME, p.ADDRESS1 as ADDRESS1, p.CITY as CITY, p.STATE as STATE, p.POSTAL as POSTAL, p.GEO_CODE as GEO_CODE, p.SEX as SEX, p.BIRTHDATE as BIRTHDATE, p.MILITARY_STATUS as MILITARY_STATUS, p.GVT_CRED_MIL_SVCE as GVT_CRED_MIL_SVCE, p.GVT_MILITARY_COMP as GVT_MILITARY_COMP, p.GVT_MIL_RESRVE_CAT as GVT_MIL_RESRVE_CAT, p.GVT_VET_PREF_APPT as GVT_VET_PREF_APPT, p.ETHNIC_GROUP as ETHNIC_GROUP, p.GVT_DISABILITY_CD as GVT_DISABILITY_CD, p.EMPLID as EMPLID, p.EMPL_RCD as EMPL_RCD, p.EFFDT as EFFDT, p.EFFSEQ as EFFSEQ FROM PS_GVT_PERS_DATA p, NWK_NEW_EHRP_ACTIONS_TBL n WHERE p.EMPLID = n.EMPLID and p.EMPL_RCD = n.EMPL_RCD and p.EFFDT = n.EFFDT and p.EFFSEQ = n.EFFSEQ` | 17: LAST_NAME, FIRST_NAME, MIDDLE_NAME, ADDRESS1, CITY, STATE, POSTAL, GEO_CODE, SEX, BIRTHDATE, MILITARY_STATUS, GVT_CRED_MIL_SVCE ... |
| `lkp_PS_JPM_JP_ITEMS` | PS_JPM_JP_ITEMS | `JPM_PROFILE_ID = EMPLID_IN` | Use Any Value | BIISPRD | NO | `SELECT PS_JPM_JP_ITEMS.JPM_CAT_TYPE as JPM_CAT_TYPE, PS_JPM_JP_ITEMS.JPM_CAT_ITEM_ID as JPM_CAT_ITEM_ID, PS_JPM_JP_ITEMS.JPM_CAT_ITEM_QUAL2 as JPM_CAT_ITEM_QUAL2, PS_JPM_JP_ITEMS.EFFDT as EFFDT, PS_JPM_JP_ITEMS.EFF_STATUS as EFF_STATUS, PS_JPM_JP_ITEMS.JPM_INTEGER_2 as JPM_INTEGER_2, PS_JPM_JP_ITEMS.MAJOR_CODE as MAJOR_CODE, PS_JPM_JP_ITEMS.MAJOR_DESCR as MAJOR_DESCR, PS_JPM_JP_ITEMS.LASTUPDOPRID as LASTUPDOPRID, PS_JPM_JP_ITEMS.JPM_PROFILE_ID as JPM_PROFILE_ID FROM EHRP.PS_JPM_JP_ITEMS WHERE jpm_cat_type = 'DEG' AND eff_status = 'A'` | 10: JPM_PROFILE_ID, JPM_CAT_TYPE, JPM_CAT_ITEM_ID, JPM_CAT_ITEM_QUAL2, EFFDT, EFF_STATUS, JPM_INTEGER_2, MAJOR_CODE, MAJOR_DESCR, LASTUPDOPRID |

PySpark: each lookup is `wide.join(lkp, keys, 'left')` where `lkp` is the table (or the override run verbatim through Spark SQL with schema owners stripped), de-duplicated with `row_number()` per condition key ordered by the output ports (deterministic stand-in for *Use Any Value*). Keys with more than one candidate row are written to `EHRP2BIIS_LOOKUP_MULTIMATCH` for review. String keys are compared with `upper()` because *Case Sensitive String Comparison = NO*.

## 5. Expression ports (business logic)

### exp_GET_EFFDT_YEAR

| Port | Port type | Datatype | Informatica expression | Spark SQL equivalent | Notes |
|---|---|---|---|---|---|
| `EFFDT` | INPUT/OUTPUT | date/time(29,9) | `EFFDT` | pass-through of `SQ_PS_GVT_JOB.EFFDT` | |
| `v_curr_year` | LOCAL VARIABLE | decimal(10,0) | `GET_DATE_PART(EFFDT, 'YYYY')` | `year(EFFDT)` |  |
| `o_CURRENT_YEAR` | OUTPUT | decimal(10,0) | `v_curr_year` | `v_curr_year` |  |

### exp_PERS_DATA

| Port | Port type | Datatype | Informatica expression | Spark SQL equivalent | Notes |
|---|---|---|---|---|---|
| `BIRTHDATE` | INPUT/OUTPUT | date/time(29,9) | `BIRTHDATE` | pass-through of `lkp_PS_GVT_PERS_DATA.BIRTHDATE` | |
| `FIRST_NAME` | INPUT/OUTPUT | string(30,0) | `FIRST_NAME` | pass-through of `lkp_PS_GVT_PERS_DATA.FIRST_NAME` | |
| `LAST_NAME` | INPUT/OUTPUT | string(30,0) | `LAST_NAME` | pass-through of `lkp_PS_GVT_PERS_DATA.LAST_NAME` | |
| `MIDDLE_NAME` | INPUT/OUTPUT | string(30,0) | `MIDDLE_NAME` | pass-through of `lkp_PS_GVT_PERS_DATA.MIDDLE_NAME` | |
| `CITY` | INPUT/OUTPUT | string(30,0) | `CITY` | pass-through of `lkp_PS_GVT_PERS_DATA.CITY` | |
| `STATE` | INPUT/OUTPUT | string(6,0) | `STATE` | pass-through of `lkp_PS_GVT_PERS_DATA.STATE` | |
| `GEO_CODE` | INPUT/OUTPUT | string(11,0) | `GEO_CODE` | pass-through of `lkp_PS_GVT_PERS_DATA.GEO_CODE` | |
| `POSTAL` | INPUT/OUTPUT | string(12,0) | `POSTAL` | pass-through of `lkp_PS_GVT_PERS_DATA.POSTAL` | |
| `ADDRESS1` | INPUT/OUTPUT | string(55,0) | `ADDRESS1` | pass-through of `lkp_PS_GVT_PERS_DATA.ADDRESS1` | |
| `GVT_DISABILITY_CD` | INPUT/OUTPUT | string(2,0) | `GVT_DISABILITY_CD` | pass-through of `lkp_PS_GVT_PERS_DATA.GVT_DISABILITY_CD` | |
| `ETHNIC_GROUP` | INPUT/OUTPUT | string(1,0) | `ETHNIC_GROUP` | pass-through of `lkp_PS_GVT_PERS_DATA.ETHNIC_GROUP` | |
| `GVT_VET_PREF_APPT` | INPUT/OUTPUT | string(1,0) | `GVT_VET_PREF_APPT` | pass-through of `lkp_PS_GVT_PERS_DATA.GVT_VET_PREF_APPT` | |
| `MILITARY_STATUS` | INPUT/OUTPUT | string(1,0) | `MILITARY_STATUS` | pass-through of `lkp_PS_GVT_PERS_DATA.MILITARY_STATUS` | |
| `SEX` | INPUT/OUTPUT | string(1,0) | `SEX` | pass-through of `lkp_PS_GVT_PERS_DATA.SEX` | |
| `GVT_MILITARY_COMP` | INPUT/OUTPUT | string(1,0) | `GVT_MILITARY_COMP` | pass-through of `lkp_PS_GVT_PERS_DATA.GVT_MILITARY_COMP` | |
| `GVT_MIL_RESRVE_CAT` | INPUT/OUTPUT | string(1,0) | `GVT_MIL_RESRVE_CAT` | pass-through of `lkp_PS_GVT_PERS_DATA.GVT_MIL_RESRVE_CAT` | |
| `GVT_CRED_MIL_SVCE` | INPUT/OUTPUT | string(6,0) | `GVT_CRED_MIL_SVCE` | pass-through of `lkp_PS_GVT_PERS_DATA.GVT_CRED_MIL_SVCE` | |

### exp_MAIN2BIIS

| Port | Port type | Datatype | Informatica expression | Spark SQL equivalent | Notes |
|---|---|---|---|---|---|
| `EHRP_SEQ_NUMBER` | INPUT | decimal(4,0) | `EHRP_SEQ_NUMBER` | pass-through of `lkp_OLD_SEQUENCE_NUMBER.EHRP_SEQ_NUMBER` | |
| `v_CURRENT_YEAR` | LOCAL VARIABLE | integer(10,0) | `GET_DATE_PART(EFFDT, 'YYYY')` | `year(EFFDT)` |  |
| `v_EVENT_ID` | LOCAL VARIABLE | integer(10,0) | `IIF(v_CURRENT_YEAR =v_PREVIOUS_YEAR ,v_EVENT_ID + 1, EHRP_SEQ_NUMBER+1)` | `EHRP_SEQ_NUMBER + row_number() OVER (PARTITION BY year(EFFDT) ORDER BY EFFDT, EMPLID, EMPL_RCD, EFFSEQ)` | Stateful row-by-row variable. SQ orders by EFFDT, so each year is one contiguous run: first row = EHRP_SEQ_NUMBER + 1, then +1 per row. Ties on EFFDT are broken by EMPLID, EMPL_RCD, EFFSEQ (PowerCenter order within a tie is database-dependent). |
| `v_PREVIOUS_YEAR` | LOCAL VARIABLE | integer(10,0) | `v_CURRENT_YEAR` | `lag(year(EFFDT)) OVER (ORDER BY EFFDT, EMPLID, EMPL_RCD, EFFSEQ)` |  |
| `o_EVENT_ID` | OUTPUT | integer(10,0) | `v_EVENT_ID` | `v_EVENT_ID` |  |
| `COMPANY` | INPUT/OUTPUT | string(3,0) | `COMPANY` | pass-through of `SQ_PS_GVT_JOB.COMPANY` | |
| `GVT_SUB_AGENCY` | INPUT | string(2,0) | `GVT_SUB_AGENCY` | pass-through of `SQ_PS_GVT_JOB.GVT_SUB_AGENCY` | |
| `o_AGCY_ASSIGN_CD` | OUTPUT | string(10,0) | `COMPANY \|\| GVT_SUB_AGENCY` | `CASE WHEN COMPANY IS NULL AND GVT_SUB_AGENCY IS NULL THEN NULL ELSE concat(coalesce(CAST(COMPANY AS STRING), ''), coalesce(CAST(GVT_SUB_AGENCY AS STRING), '')) END` |  |
| `GVT_XFER_TO_AGCY` | INPUT/OUTPUT | string(2,0) | `GVT_XFER_TO_AGCY` | pass-through of `SQ_PS_GVT_JOB.GVT_XFER_TO_AGCY` | |
| `GVT_XFER_FROM_AGCY` | INPUT | string(2,0) | `GVT_XFER_FROM_AGCY` | pass-through of `SQ_PS_GVT_JOB.GVT_XFER_FROM_AGCY` | |
| `o_AGCY_SUBELEMENT_PRIOR_CD` | OUTPUT | string(4,0) | `IIF(IS_SPACES( GVT_XFER_FROM_AGCY) or ISNULL(GVT_XFER_FROM_AGCY), NULL, GVT_XFER_FROM_AGCY \|\| '00' )` | `CASE WHEN (GVT_XFER_FROM_AGCY RLIKE '^[ \\t\\n\\r\\f\\x0B]*$') OR GVT_XFER_FROM_AGCY IS NULL THEN NULL ELSE concat(GVT_XFER_FROM_AGCY, '00') END` |  |
| `HE_AL_RED_CRED` | INPUT | decimal(5,2) | `HE_AL_RED_CRED` | pass-through of `SQ_PS_GVT_JOB.HE_AL_RED_CRED` | |
| `o_HE_AL_RED_CRED` | OUTPUT | decimal(5,2) | `IIF(HE_AL_RED_CRED =0, NULL, HE_AL_RED_CRED)` | `CASE WHEN HE_AL_RED_CRED = 0 THEN NULL ELSE HE_AL_RED_CRED END` |  |
| `HE_AL_BALANCE` | INPUT | decimal(6,2) | `HE_AL_BALANCE` | pass-through of `SQ_PS_GVT_JOB.HE_AL_BALANCE` | |
| `o_HE_AL_BALANCE` | OUTPUT | decimal(6,2) | `IIF(HE_AL_BALANCE=0, NULL, HE_AL_BALANCE)` | `CASE WHEN HE_AL_BALANCE = 0 THEN NULL ELSE HE_AL_BALANCE END` |  |
| `HE_LUMP_HRS` | INPUT | decimal(38,0) | `HE_LUMP_HRS` | pass-through of `SQ_PS_GVT_JOB.HE_LUMP_HRS` | |
| `o_HE_LUMP_HRS` | OUTPUT | decimal(38,0) | `IIF(HE_LUMP_HRS=0, NULL, HE_LUMP_HRS)` | `CASE WHEN HE_LUMP_HRS = 0 THEN NULL ELSE HE_LUMP_HRS END` |  |
| `HE_AL_CARRYOVER` | INPUT | decimal(6,2) | `HE_AL_CARRYOVER` | pass-through of `SQ_PS_GVT_JOB.HE_AL_CARRYOVER` | |
| `o_HE_AL_CARRYOVER` | OUTPUT | decimal(6,2) | `IIF(HE_AL_CARRYOVER=0, NULL, HE_AL_CARRYOVER)` | `CASE WHEN HE_AL_CARRYOVER = 0 THEN NULL ELSE HE_AL_CARRYOVER END` |  |
| `HE_RES_LASTYR` | INPUT | decimal(6,2) | `HE_RES_LASTYR` | pass-through of `SQ_PS_GVT_JOB.HE_RES_LASTYR` | |
| `o_HE_RES_LASTYR` | OUTPUT | decimal(6,2) | `IIF(HE_RES_LASTYR=0, NULL, HE_RES_LASTYR)` | `CASE WHEN HE_RES_LASTYR = 0 THEN NULL ELSE HE_RES_LASTYR END` |  |
| `HE_RES_BALANCE` | INPUT | decimal(6,2) | `HE_RES_BALANCE` | pass-through of `SQ_PS_GVT_JOB.HE_RES_BALANCE` | |
| `o_HE_RES_BALANCE` | OUTPUT | decimal(6,0) | `IIF(HE_RES_BALANCE=0, NULL, HE_RES_BALANCE)` | `CASE WHEN HE_RES_BALANCE = 0 THEN NULL ELSE HE_RES_BALANCE END` |  |
| `HE_RES_TWOYRS` | INPUT | decimal(6,2) | `HE_RES_TWOYRS` | pass-through of `SQ_PS_GVT_JOB.HE_RES_TWOYRS` | |
| `o_HE_RES_TWOYRS` | OUTPUT | decimal(6,2) | `IIF(HE_RES_TWOYRS=0, NULL, HE_RES_TWOYRS)` | `CASE WHEN HE_RES_TWOYRS = 0 THEN NULL ELSE HE_RES_TWOYRS END` |  |
| `HE_RES_THREEYRS` | INPUT | decimal(6,2) | `HE_RES_THREEYRS` | pass-through of `SQ_PS_GVT_JOB.HE_RES_THREEYRS` | |
| `o_HE_RES_THREEYRS` | OUTPUT | decimal(6,2) | `IIF(HE_RES_THREEYRS=0, NULL, HE_RES_THREEYRS)` | `CASE WHEN HE_RES_THREEYRS = 0 THEN NULL ELSE HE_RES_THREEYRS END` |  |
| `HE_AL_ACCRUAL` | INPUT | decimal(5,2) | `HE_AL_ACCRUAL` | pass-through of `SQ_PS_GVT_JOB.HE_AL_ACCRUAL` | |
| `o_HE_AL_ACCRUAL` | OUTPUT | decimal(5,2) | `IIF(HE_AL_ACCRUAL=0, NULL, HE_AL_ACCRUAL)` | `CASE WHEN HE_AL_ACCRUAL = 0 THEN NULL ELSE HE_AL_ACCRUAL END` |  |
| `HE_AL_TOTAL` | INPUT | decimal(5,2) | `HE_AL_TOTAL` | pass-through of `SQ_PS_GVT_JOB.HE_AL_TOTAL` | |
| `o_HE_AL_TOTAL` | OUTPUT | decimal(5,2) | `IIF(HE_AL_TOTAL=0, NULL, HE_AL_TOTAL)` | `CASE WHEN HE_AL_TOTAL = 0 THEN NULL ELSE HE_AL_TOTAL END` |  |
| `ANNL_BENEF_BASE_RT` | INPUT/OUTPUT | decimal(18,3) | `ANNL_BENEF_BASE_RT` | pass-through of `SQ_PS_GVT_JOB.ANNL_BENEF_BASE_RT` | |
| `GVT_ANN_IND` | INPUT/OUTPUT | string(1,0) | `GVT_ANN_IND` | pass-through of `SQ_PS_GVT_JOB.GVT_ANN_IND` | |
| `GVT_TYPE_OF_APPT` | INPUT/OUTPUT | string(2,0) | `GVT_TYPE_OF_APPT` | pass-through of `SQ_PS_GVT_JOB.GVT_TYPE_OF_APPT` | |
| `HE_AWOP_SEP` | INPUT/OUTPUT | decimal(6,2) | `HE_AWOP_SEP` | pass-through of `SQ_PS_GVT_JOB.HE_AWOP_SEP` | |
| `o_HE_AWOP_SEP` | OUTPUT | decimal(6,2) | `IIF(HE_AWOP_SEP=0, NULL, HE_AWOP_SEP)` | `CASE WHEN HE_AWOP_SEP = 0 THEN NULL ELSE HE_AWOP_SEP END` |  |
| `BARG_UNIT` | INPUT/OUTPUT | string(4,0) | `BARG_UNIT` | pass-through of `SQ_PS_GVT_JOB.BARG_UNIT` | |
| `STD_HOURS` | INPUT | decimal(6,2) | `STD_HOURS` | pass-through of `SQ_PS_GVT_JOB.STD_HOURS` | |
| `STD_HRS_FREQUENCY` | INPUT | string(5,0) | `STD_HRS_FREQUENCY` | pass-through of `SQ_PS_GVT_JOB.STD_HRS_FREQUENCY` | |
| `o_BASE_HOURS` | OUTPUT | decimal(7,2) | `IIF(GVT_WORK_SCHED='I', STD_HOURS,STD_HOURS*2)` | `CASE WHEN GVT_WORK_SCHED = 'I' THEN STD_HOURS ELSE STD_HOURS * 2 END` | NULL GVT_WORK_SCHED -> IIF condition NULL -> FALSE -> STD_HOURS * 2. |
| `ACCT_CD` | INPUT/OUTPUT | string(25,0) | `ACCT_CD` | pass-through of `SQ_PS_GVT_JOB.ACCT_CD` | |
| `LOCATION` | INPUT/OUTPUT | string(10,0) | `LOCATION` | pass-through of `SQ_PS_GVT_JOB.LOCATION` | |
| `GRADE_ENTRY_DT` | INPUT/OUTPUT | date/time(29,9) | `GRADE_ENTRY_DT` | pass-through of `SQ_PS_GVT_JOB.GRADE_ENTRY_DT` | |
| `EFFDT` | INPUT/OUTPUT | date/time(29,9) | `EFFDT` | pass-through of `SQ_PS_GVT_JOB.EFFDT` | |
| `GVT_FEGLI` | INPUT/OUTPUT | string(2,0) | `GVT_FEGLI` | pass-through of `SQ_PS_GVT_JOB.GVT_FEGLI` | |
| `GVT_FEGLI_LIVING` | INPUT/OUTPUT | string(1,0) | `GVT_FEGLI_LIVING` | pass-through of `SQ_PS_GVT_JOB.GVT_FEGLI_LIVING` | |
| `GVT_LIVING_AMT` | INPUT/OUTPUT | decimal(38,0) | `GVT_LIVING_AMT` | pass-through of `SQ_PS_GVT_JOB.GVT_LIVING_AMT` | |
| `GVT_FERS_COVERAGE` | INPUT/OUTPUT | string(1,0) | `GVT_FERS_COVERAGE` | pass-through of `SQ_PS_GVT_JOB.GVT_FERS_COVERAGE` | |
| `FLSA_STATUS` | INPUT/OUTPUT | string(1,0) | `FLSA_STATUS` | pass-through of `SQ_PS_GVT_JOB.FLSA_STATUS` | |
| `GVT_CSRS_FROZN_SVC` | INPUT/OUTPUT | string(4,0) | `GVT_CSRS_FROZN_SVC` | pass-through of `SQ_PS_GVT_JOB.GVT_CSRS_FROZN_SVC` | |
| `GRADE` | INPUT/OUTPUT | string(3,0) | `GRADE` | pass-through of `SQ_PS_GVT_JOB.GRADE` | |
| `HOURLY_RT` | INPUT/OUTPUT | decimal(18,6) | `HOURLY_RT` | pass-through of `SQ_PS_GVT_JOB.HOURLY_RT` | |
| `GVT_LEG_AUTH_1` | INPUT/OUTPUT | string(3,0) | `GVT_LEG_AUTH_1` | pass-through of `SQ_PS_GVT_JOB.GVT_LEG_AUTH_1` | |
| `GVT_PAR_AUTH_D1` | INPUT | string(25,0) | `GVT_PAR_AUTH_D1` | pass-through of `SQ_PS_GVT_JOB.GVT_PAR_AUTH_D1` | |
| `GVT_PAR_AUTH_D1_2` | INPUT | string(25,0) | `GVT_PAR_AUTH_D1_2` | pass-through of `SQ_PS_GVT_JOB.GVT_PAR_AUTH_D1_2` | |
| `o_LEG_AUTH_TXT_1` | OUTPUT | string(60,0) | `UPPER(GVT_PAR_AUTH_D1 \|\| RTRIM(GVT_PAR_AUTH_D1_2))` | `upper(CASE WHEN GVT_PAR_AUTH_D1 IS NULL AND rtrim(GVT_PAR_AUTH_D1_2) IS NULL THEN NULL ELSE concat(coalesce(CAST(GVT_PAR_AUTH_D1 AS STRING), ''), coalesce(CAST(rtrim(GVT_PAR_AUTH_D1_2) AS STRING), '')) END)` | Only the second operand is RTRIM'd; trailing blanks of D1 are kept byte-for-byte. |
| `GVT_LEG_AUTH_2` | INPUT/OUTPUT | string(3,0) | `GVT_LEG_AUTH_2` | pass-through of `SQ_PS_GVT_JOB.GVT_LEG_AUTH_2` | |
| `GVT_PAR_AUTH_D2` | INPUT | string(25,0) | `GVT_PAR_AUTH_D2` | pass-through of `SQ_PS_GVT_JOB.GVT_PAR_AUTH_D2` | |
| `GVT_PAR_AUTH_D2_2` | INPUT | string(25,0) | `GVT_PAR_AUTH_D2_2` | pass-through of `SQ_PS_GVT_JOB.GVT_PAR_AUTH_D2_2` | |
| `o_LEG_AUTH_TXT_2` | OUTPUT | string(60,0) | `UPPER(GVT_PAR_AUTH_D2 \|\| RTRIM(GVT_PAR_AUTH_D2_2))` | `upper(CASE WHEN GVT_PAR_AUTH_D2 IS NULL AND rtrim(GVT_PAR_AUTH_D2_2) IS NULL THEN NULL ELSE concat(coalesce(CAST(GVT_PAR_AUTH_D2 AS STRING), ''), coalesce(CAST(rtrim(GVT_PAR_AUTH_D2_2) AS STRING), '')) END)` |  |
| `GVT_LOCALITY_ADJ` | INPUT/OUTPUT | decimal(8,2) | `GVT_LOCALITY_ADJ` | pass-through of `SQ_PS_GVT_JOB.GVT_LOCALITY_ADJ` | |
| `HE_AWOP_WIGI` | INPUT/OUTPUT | decimal(6,2) | `HE_AWOP_WIGI` | pass-through of `SQ_PS_GVT_JOB.HE_AWOP_WIGI` | |
| `o_HE_AWOP_WIGI` | OUTPUT | decimal(6,2) | `IIF(HE_AWOP_WIGI=0, NULL, HE_AWOP_WIGI)` | `CASE WHEN HE_AWOP_WIGI = 0 THEN NULL ELSE HE_AWOP_WIGI END` |  |
| `GVT_NOA_CODE` | INPUT/OUTPUT | string(3,0) | `GVT_NOA_CODE` | pass-through of `SQ_PS_GVT_JOB.GVT_NOA_CODE` | |
| `HE_NOA_EXT` | INPUT/OUTPUT | string(1,0) | `HE_NOA_EXT` | pass-through of `SQ_PS_GVT_JOB.HE_NOA_EXT` | |
| `DEPTID` | INPUT/OUTPUT | string(10,0) | `DEPTID` | pass-through of `SQ_PS_GVT_JOB.DEPTID` | |
| `GVT_PAY_BASIS` | INPUT/OUTPUT | string(2,0) | `GVT_PAY_BASIS` | pass-through of `SQ_PS_GVT_JOB.GVT_PAY_BASIS` | |
| `GVT_PAY_PLAN` | INPUT/OUTPUT | string(2,0) | `GVT_PAY_PLAN` | pass-through of `SQ_PS_GVT_JOB.GVT_PAY_PLAN` | |
| `o_PERM_TEMP_POSITION_CD` | OUTPUT | string(10,0) | `IIF(STD_HOURS < 40,3,IIF(STD_HOURS>=40,1,0))` | `CAST(CASE WHEN STD_HOURS < 40 THEN 3 WHEN STD_HOURS >= 40 THEN 1 ELSE 0 END AS STRING)` | NULL STD_HOURS fails both tests -> '0'. |
| `GVT_POI` | INPUT/OUTPUT | string(4,0) | `GVT_POI` | pass-through of `SQ_PS_GVT_JOB.GVT_POI` | |
| `JOBCODE` | INPUT/OUTPUT | string(6,0) | `JOBCODE` | pass-through of `SQ_PS_GVT_JOB.JOBCODE` | |
| `POSITION_NBR` | INPUT/OUTPUT | string(8,0) | `POSITION_NBR` | pass-through of `SQ_PS_GVT_JOB.POSITION_NBR` | |
| `GVT_POSN_OCCUPIED` | INPUT/OUTPUT | string(1,0) | `GVT_POSN_OCCUPIED` | pass-through of `SQ_PS_GVT_JOB.GVT_POSN_OCCUPIED` | |
| `GVT_PAY_RATE_DETER` | INPUT/OUTPUT | string(1,0) | `GVT_PAY_RATE_DETER` | pass-through of `SQ_PS_GVT_JOB.GVT_PAY_RATE_DETER` | |
| `GVT_PREV_RET_COVRG` | INPUT/OUTPUT | string(1,0) | `GVT_PREV_RET_COVRG` | pass-through of `SQ_PS_GVT_JOB.GVT_PREV_RET_COVRG` | |
| `GVT_ANNUITY_OFFSET` | INPUT/OUTPUT | decimal(10,2) | `GVT_ANNUITY_OFFSET` | pass-through of `SQ_PS_GVT_JOB.GVT_ANNUITY_OFFSET` | |
| `o_GVT_ANNUITY_OFFSET` | OUTPUT | decimal(10,2) | `IIF(GVT_ANNUITY_OFFSET=0,NULL,GVT_ANNUITY_OFFSET)` | `CASE WHEN GVT_ANNUITY_OFFSET = 0 THEN NULL ELSE GVT_ANNUITY_OFFSET END` |  |
| `GVT_RETIRE_PLAN` | INPUT/OUTPUT | string(2,0) | `GVT_RETIRE_PLAN` | pass-through of `SQ_PS_GVT_JOB.GVT_RETIRE_PLAN` | |
| `COMPRATE` | INPUT | decimal(18,6) | `COMPRATE` | pass-through of `SQ_PS_GVT_JOB.COMPRATE` | |
| `GVT_COMPRATE` | INPUT/OUTPUT | decimal(18,6) | `GVT_COMPRATE` | pass-through of `SQ_PS_GVT_JOB.GVT_COMPRATE` | |
| `GVT_HRLY_RT_NO_LOC` | INPUT/OUTPUT | decimal(18,6) | `GVT_HRLY_RT_NO_LOC` | pass-through of `SQ_PS_GVT_JOB.GVT_HRLY_RT_NO_LOC` | |
| `HE_SL_RED_CRED` | INPUT | decimal(5,2) | `HE_SL_RED_CRED` | pass-through of `SQ_PS_GVT_JOB.HE_SL_RED_CRED` | |
| `o_HE_SL_RED_CRED` | OUTPUT | decimal(5,2) | `IIF(HE_SL_RED_CRED=0,NULL, HE_SL_RED_CRED)` | `CASE WHEN HE_SL_RED_CRED = 0 THEN NULL ELSE HE_SL_RED_CRED END` |  |
| `HE_SL_BALANCE` | INPUT | decimal(6,2) | `HE_SL_BALANCE` | pass-through of `SQ_PS_GVT_JOB.HE_SL_BALANCE` | |
| `o_HE_SL_BALANCE` | OUTPUT | decimal(6,2) | `IIF(HE_SL_BALANCE=0, NULL, HE_SL_BALANCE)` | `CASE WHEN HE_SL_BALANCE = 0 THEN NULL ELSE HE_SL_BALANCE END` |  |
| `HE_FROZEN_SL` | INPUT | decimal(6,2) | `HE_FROZEN_SL` | pass-through of `SQ_PS_GVT_JOB.HE_FROZEN_SL` | |
| `o_HE_FROZEN_SL` | OUTPUT | decimal(6,2) | `IIF(HE_FROZEN_SL=0, NULL, HE_FROZEN_SL)` | `CASE WHEN HE_FROZEN_SL = 0 THEN NULL ELSE HE_FROZEN_SL END` |  |
| `HE_SL_CARRYOVER` | INPUT | decimal(6,2) | `HE_SL_CARRYOVER` | pass-through of `SQ_PS_GVT_JOB.HE_SL_CARRYOVER` | |
| `o_HE_SL_CARRYOVER` | OUTPUT | decimal(6,2) | `IIF(HE_SL_CARRYOVER=0, NULL, HE_SL_CARRYOVER)` | `CASE WHEN HE_SL_CARRYOVER = 0 THEN NULL ELSE HE_SL_CARRYOVER END` |  |
| `HE_SL_ACCRUAL` | INPUT | decimal(5,2) | `HE_SL_ACCRUAL` | pass-through of `SQ_PS_GVT_JOB.HE_SL_ACCRUAL` | |
| `o_HE_SL_ACCRUAL` | OUTPUT | decimal(5,2) | `IIF(HE_SL_ACCRUAL=0, NULL, HE_SL_ACCRUAL)` | `CASE WHEN HE_SL_ACCRUAL = 0 THEN NULL ELSE HE_SL_ACCRUAL END` |  |
| `HE_SL_TOTAL` | INPUT | decimal(6,2) | `HE_SL_TOTAL` | pass-through of `SQ_PS_GVT_JOB.HE_SL_TOTAL` | |
| `o_HE_SL_TOTAL` | OUTPUT | decimal(6,2) | `IIF(HE_SL_TOTAL=0, NULL, HE_SL_TOTAL)` | `CASE WHEN HE_SL_TOTAL = 0 THEN NULL ELSE HE_SL_TOTAL END` |  |
| `GVT_STEP` | INPUT/OUTPUT | string(2,0) | `GVT_STEP` | pass-through of `SQ_PS_GVT_JOB.GVT_STEP` | |
| `UNION_CD` | INPUT/OUTPUT | string(3,0) | `UNION_CD` | pass-through of `SQ_PS_GVT_JOB.UNION_CD` | |
| `o_UNION_CD` | OUTPUT | string(3,0) | `IIF(IS_SPACES(UNION_CD), NULL, UNION_CD)` | `CASE WHEN UNION_CD RLIKE '^[ \\t\\n\\r\\f\\x0B]*$' THEN NULL ELSE UNION_CD END` | PeopleSoft stores blank CHAR fields as a single space; IS_SPACES(' ') is TRUE -> NULL. |
| `STEP_ENTRY_DT` | INPUT/OUTPUT | date/time(29,9) | `STEP_ENTRY_DT` | pass-through of `SQ_PS_GVT_JOB.STEP_ENTRY_DT` | |
| `GVT_WORK_SCHED` | INPUT/OUTPUT | string(1,0) | `GVT_WORK_SCHED` | pass-through of `SQ_PS_GVT_JOB.GVT_WORK_SCHED` | |
| `CITY` | INPUT | string(30,0) | `CITY` | pass-through of `exp_PERS_DATA.CITY` | |
| `STATE` | INPUT | string(6,0) | `STATE` | pass-through of `exp_PERS_DATA.STATE` | |
| `o_EMP_RESID_CITY_STATE_NAME` | OUTPUT | string(60,0) | `CITY \|\| ', ' \|\| STATE` | `concat(coalesce(CAST(CITY AS STRING), ''), ', ', coalesce(CAST(STATE AS STRING), ''))` | Never NULL: a NULL CITY/STATE is concatenated as ''. Result for a lookup miss is ', '. |
| `EMPLID` | INPUT/OUTPUT | string(11,0) | `EMPLID` | pass-through of `SQ_PS_GVT_JOB.EMPLID` | |
| `EFFSEQ` | INPUT/OUTPUT | decimal(38,0) | `EFFSEQ` | pass-through of `SQ_PS_GVT_JOB.EFFSEQ` | |
| `o_EFFSEQ` | OUTPUT | string(10,0) | `EFFSEQ` | `CAST(CAST(EFFSEQ AS BIGINT) AS STRING)` |  |
| `EMPL_RCD` | INPUT/OUTPUT | decimal(38,0) | `EMPL_RCD` | pass-through of `SQ_PS_GVT_JOB.EMPL_RCD` | |
| `ACTION` | INPUT/OUTPUT | string(3,0) | `ACTION` | pass-through of `SQ_PS_GVT_JOB.ACTION` | |
| `ACTION_REASON` | INPUT/OUTPUT | string(3,0) | `ACTION_REASON` | pass-through of `SQ_PS_GVT_JOB.ACTION_REASON` | |
| `v_MONTH_NUMBER` | LOCAL VARIABLE | integer(10,0) | `GET_DATE_PART(SYSDATE, 'MM')` | `month(:run_date)` |  |
| `v_DAY_NUMBER` | LOCAL VARIABLE | integer(10,0) | `GET_DATE_PART(SYSDATE, 'DD')` | `day(:run_date)` |  |
| `v_MONTH_CHAR` | LOCAL VARIABLE | string(2,0) | `IIF(v_MONTH_NUMBER < 10, '0' \|\| TO_CHAR(v_MONTH_NUMBER), TO_CHAR(v_MONTH_NUMBER))` | `lpad(month(:run_date), 2, '0')` |  |
| `v_DAY_CHAR` | LOCAL VARIABLE | string(2,0) | `IIF(v_DAY_NUMBER < 10, '0' \|\| TO_CHAR(v_DAY_NUMBER), TO_CHAR(v_DAY_NUMBER))` | `lpad(day(:run_date), 2, '0')` |  |
| `o_LOAD_ID` | OUTPUT | string(10,0) | `'NK' \|\| GET_DATE_PART(SYSDATE, 'YYYY') \|\| v_MONTH_CHAR \|\| v_DAY_CHAR` | `concat('NK', date_format(:run_date, 'yyyyMMdd'))` | Reconciliation key. SYSDATE is pinned to the run's logical date (Airflow ds) so reruns are stable. |
| `o_LOAD_DATE` | OUTPUT | date/time(29,9) | `trunc(sysdate)` | `date_trunc('DD', :run_date)` | trunc(sysdate) -> midnight of the run date. Reconciliation value. |
| `o_LINE_SEQ` | OUTPUT | string(3,0) | `'01N'` | `'01N'` | Unconnected port (dead logic). |
| `GVT_WIP_STATUS` | INPUT/OUTPUT | string(3,0) | `GVT_WIP_STATUS` | pass-through of `SQ_PS_GVT_JOB.GVT_WIP_STATUS` | |
| `GVT_STATUS_TYPE` | INPUT/OUTPUT | string(3,0) | `GVT_STATUS_TYPE` | pass-through of `SQ_PS_GVT_JOB.GVT_STATUS_TYPE` | |
| `EARNINGS_END_DT` | INPUT | date/time(29,9) | `EARNINGS_END_DT` | pass-through of `lkp_PS_GVT_AWD_DATA.EARNINGS_END_DT` | |
| `ERNCD` | INPUT | string(3,0) | `ERNCD` | pass-through of `lkp_PS_GVT_AWD_DATA.ERNCD` | |
| `GOAL_AMT` | INPUT | decimal(10,2) | `GOAL_AMT` | pass-through of `lkp_PS_GVT_AWD_DATA.GOAL_AMT` | |
| `o_CASH_AWARD_AMT` | OUTPUT | decimal(19,4) | `DECODE(TRUE, IN (GVT_NOA_CODE,'840', '841', '842' , '843', '844','845', '848', '849', '873', '874', '875', '876','877','878','879') AND NOT (IN(ERNCD, 'RCR', 'RLC')), GOAL_AMT, IN(GVT_NOA_CODE, '817', '889'), GOAL_AMT, NULL)` | `CASE WHEN GVT_NOA_CODE IN ('840', '841', '842', '843', '844', '845', '848', '849', '873', '874', '875', '876', '877', '878', '879') AND NOT (ERNCD IN ('RCR', 'RLC')) THEN GOAL_AMT WHEN GVT_NOA_CODE IN ('817', '889') THEN GOAL_AMT ELSE NULL END` | DECODE(TRUE,...) truthiness: NULL ERNCD makes NOT IN(...) NULL, so an award NOA with no ERNCD falls through to the next branch (and finally NULL). |
| `GVT_TANG_BEN_AMT` | INPUT | decimal(38,0) | `GVT_TANG_BEN_AMT` | pass-through of `lkp_PS_GVT_AWD_DATA.GVT_TANG_BEN_AMT` | |
| `o_CASH_AWARD_BNFT_AMT` | OUTPUT | decimal(19,4) | `IIF(IN (GVT_NOA_CODE,'840', '841', '842' , '843', '844','845', '848', '849', '873', '874', '875', '876','877','878','879') AND NOT (IN(ERNCD, 'RCR', 'RLC')) ,GVT_TANG_BEN_AMT,NULL)` | `CASE WHEN GVT_NOA_CODE IN ('840', '841', '842', '843', '844', '845', '848', '849', '873', '874', '875', '876', '877', '878', '879') AND NOT (ERNCD IN ('RCR', 'RLC')) THEN GVT_TANG_BEN_AMT ELSE NULL END` |  |
| `o_RECRUITMENT_BONUS_AMT` | OUTPUT | decimal(19,4) | `IIF(GVT_NOA_CODE = '815' OR (GVT_NOA_CODE = '948' AND HE_NOA_EXT = '0'), GOAL_AMT,NULL)` | `CASE WHEN GVT_NOA_CODE = '815' OR (GVT_NOA_CODE = '948' AND HE_NOA_EXT = '0') THEN GOAL_AMT ELSE NULL END` |  |
| `o_RELOCATION_BONUS_AMT` | OUTPUT | decimal(19,4) | `IIF(IN(GVT_NOA_CODE, '816'), GOAL_AMT,NULL)` | `CASE WHEN GVT_NOA_CODE IN ('816') THEN GOAL_AMT ELSE NULL END` |  |
| `OTH_HRS` | INPUT | decimal(6,2) | `OTH_HRS` | pass-through of `lkp_PS_GVT_AWD_DATA.OTH_HRS` | |
| `o_TIME_OFF_AWARD_AMT` | OUTPUT | decimal(9,2) | `IIF(IN(GVT_NOA_CODE, '846', '847'), OTH_HRS,NULL)` | `CASE WHEN GVT_NOA_CODE IN ('846', '847') THEN OTH_HRS ELSE NULL END` |  |
| `o_TIME_OFF_GRANTED_HRS` | OUTPUT | decimal(9,2) | `IIF(IN(GVT_NOA_CODE, '846', '847'), OTH_HRS,NULL)` | `CASE WHEN GVT_NOA_CODE IN ('846', '847') THEN OTH_HRS ELSE NULL END` |  |
| `HE_REG_MILITARY` | INPUT/OUTPUT | decimal(38,0) | `HE_REG_MILITARY` | pass-through of `SQ_PS_GVT_JOB.HE_REG_MILITARY` | |
| `HE_SPC_MILITARY` | INPUT/OUTPUT | decimal(38,0) | `HE_SPC_MILITARY` | pass-through of `SQ_PS_GVT_JOB.HE_SPC_MILITARY` | |
| `SAL_ADMIN_PLAN` | INPUT/OUTPUT | string(4,0) | `SAL_ADMIN_PLAN` | pass-through of `SQ_PS_GVT_JOB.SAL_ADMIN_PLAN` | |
| `GVT_RTND_PAY_PLAN` | INPUT/OUTPUT | string(2,0) | `GVT_RTND_PAY_PLAN` | pass-through of `SQ_PS_GVT_JOB.GVT_RTND_PAY_PLAN` | |
| `GVT_RTND_GRADE` | INPUT/OUTPUT | string(3,0) | `GVT_RTND_GRADE` | pass-through of `SQ_PS_GVT_JOB.GVT_RTND_GRADE` | |
| `GVT_RTND_STEP` | INPUT/OUTPUT | decimal(38,0) | `GVT_RTND_STEP` | pass-through of `SQ_PS_GVT_JOB.GVT_RTND_STEP` | |
| `HE_PP_UDED_AMT` | INPUT/OUTPUT | decimal(9,2) | `HE_PP_UDED_AMT` | pass-through of `SQ_PS_GVT_JOB.HE_PP_UDED_AMT` | |
| `HE_NO_TSP_PAYPER` | INPUT/OUTPUT | decimal(38,0) | `HE_NO_TSP_PAYPER` | pass-through of `SQ_PS_GVT_JOB.HE_NO_TSP_PAYPER` | |
| `HE_TSPA_SUB_YR` | INPUT/OUTPUT | decimal(7,2) | `HE_TSPA_SUB_YR` | pass-through of `SQ_PS_GVT_JOB.HE_TSPA_SUB_YR` | |
| `HE_EMP_UDED_AMT` | INPUT/OUTPUT | decimal(9,2) | `HE_EMP_UDED_AMT` | pass-through of `SQ_PS_GVT_JOB.HE_EMP_UDED_AMT` | |
| `HE_GVT_UDED_AMT` | INPUT/OUTPUT | decimal(9,2) | `HE_GVT_UDED_AMT` | pass-through of `SQ_PS_GVT_JOB.HE_GVT_UDED_AMT` | |
| `HE_TLTR_NO` | INPUT/OUTPUT | decimal(38,0) | `HE_TLTR_NO` | pass-through of `SQ_PS_GVT_JOB.HE_TLTR_NO` | |
| `HE_UDED_PAY_CD` | INPUT/OUTPUT | string(1,0) | `HE_UDED_PAY_CD` | pass-through of `SQ_PS_GVT_JOB.HE_UDED_PAY_CD` | |
| `HE_TSP_CANC_CD` | INPUT/OUTPUT | string(1,0) | `HE_TSP_CANC_CD` | pass-through of `SQ_PS_GVT_JOB.HE_TSP_CANC_CD` | |
| `CITIZENSHIP_STATUS_IN` | INPUT | string(1,0) | `CITIZENSHIP_STATUS_IN` | pass-through of `lkp_PS_GVT_CITIZENSHIP.CITIZENSHIP_STATUS` | |
| `o_CITIZENSHIP_STATUS` | OUTPUT | string(1,0) | `IIF(CITIZENSHIP_STATUS_IN = '1' or CITIZENSHIP_STATUS_IN = '2', '1', '8')` | `CASE WHEN CITIZENSHIP_STATUS_IN = '1' OR CITIZENSHIP_STATUS_IN = '2' THEN '1' ELSE '8' END` | Lookup miss (NULL) -> '8'. |
| `o_TSP_VESTING_CD` | OUTPUT | string(1,0) | `IIF(IN(GVT_RETIRE_PLAN, 'K', 'M'),IIF(IN(GVT_TYPE_OF_APPT, '55', '34', '36', '46', '44'), 2, 3), 0)` | `CAST(CASE WHEN GVT_RETIRE_PLAN IN ('K', 'M') THEN CASE WHEN GVT_TYPE_OF_APPT IN ('55', '34', '36', '46', '44') THEN 2 ELSE 3 END ELSE 0 END AS STRING)` |  |
| `REG_TEMP` | INPUT/OUTPUT | string(1,0) | `REG_TEMP` | pass-through of `SQ_PS_GVT_JOB.REG_TEMP` | |
| `o_PERMANENT_TEMP_POSITION_CD` | OUTPUT | string(1,0) | `IIF(REG_TEMP = 'R' AND GVT_WORK_SCHED = 'F', '1', IIF(IN(REG_TEMP, 'R', 'T' ) AND IN(GVT_WORK_SCHED, 'P', 'I'), '3', IIF(REG_TEMP = 'T' AND GVT_WORK_SCHED = 'F', '2', NULL)))` | `CASE WHEN REG_TEMP = 'R' AND GVT_WORK_SCHED = 'F' THEN '1' WHEN REG_TEMP IN ('R', 'T') AND GVT_WORK_SCHED IN ('P', 'I') THEN '3' WHEN REG_TEMP = 'T' AND GVT_WORK_SCHED = 'F' THEN '2' ELSE NULL END` | Unconnected port (dead logic); target PERMANENT_TEMP_POSITION_CD is fed by o_PERM_TEMP_POSITION_CD. |
| `GVT_DATE_WRK` | INPUT | date/time(29,9) | `GVT_DATE_WRK` | pass-through of `lkp_PS_GVT_EE_DATA_TRK.GVT_DATE_WRK` | |
| `ACTION_DT` | INPUT/OUTPUT | date/time(29,9) | `ACTION_DT` | pass-through of `SQ_PS_GVT_JOB.ACTION_DT` | |
| `o_EVENT_SUBMITTED_DTE` | OUTPUT | date/time(29,9) | `IIF(ISNULL(GVT_DATE_WRK), ACTION_DT, GVT_DATE_WRK)` | `CASE WHEN GVT_DATE_WRK IS NULL THEN ACTION_DT ELSE GVT_DATE_WRK END` |  |
| `o_RECRUITMENT_EXP_DTE` | OUTPUT | date/time(29,9) | `IIF(GVT_NOA_CODE = '815' OR (GVT_NOA_CODE = '948' AND HE_NOA_EXT = '0'), EARNINGS_END_DT, NULL)` | `CASE WHEN GVT_NOA_CODE = '815' OR (GVT_NOA_CODE = '948' AND HE_NOA_EXT = '0') THEN EARNINGS_END_DT ELSE NULL END` |  |
| `o_RELOCATION_EXP_DTE` | OUTPUT | date/time(29,9) | `IIF(GVT_NOA_CODE = '816' OR (GVT_NOA_CODE = '948' AND HE_NOA_EXT = '1'), EARNINGS_END_DT, NULL)` | `CASE WHEN GVT_NOA_CODE = '816' OR (GVT_NOA_CODE = '948' AND HE_NOA_EXT = '1') THEN EARNINGS_END_DT ELSE NULL END` |  |
| `GVT_SCD_RETIRE` | INPUT/OUTPUT | date/time(29,9) | `GVT_SCD_RETIRE` | pass-through of `lkp_PS_GVT_EMPLOYMENT.GVT_SCD_RETIRE` | |
| `GVT_SCD_TSP` | INPUT/OUTPUT | date/time(29,9) | `GVT_SCD_TSP` | pass-through of `lkp_PS_GVT_EMPLOYMENT.GVT_SCD_TSP` | |
| `GVT_RTND_GRADE_BEG` | INPUT/OUTPUT | date/time(29,9) | `GVT_RTND_GRADE_BEG` | pass-through of `lkp_PS_GVT_EMPLOYMENT.GVT_RTND_GRADE_BEG` | |
| `GVT_RTND_GRADE_EXP` | INPUT/OUTPUT | date/time(29,9) | `GVT_RTND_GRADE_EXP` | pass-through of `lkp_PS_GVT_EMPLOYMENT.GVT_RTND_GRADE_EXP` | |
| `GVT_SABBATIC_EXPIR` | INPUT/OUTPUT | date/time(29,9) | `GVT_SABBATIC_EXPIR` | pass-through of `lkp_PS_GVT_EMPLOYMENT.GVT_SABBATIC_EXPIR` | |
| `GVT_CURR_APT_AUTH1` | INPUT/OUTPUT | string(3,0) | `GVT_CURR_APT_AUTH1` | pass-through of `lkp_PS_GVT_EMPLOYMENT.GVT_CURR_APT_AUTH1` | |
| `o_GVT_CURR_APT_AUTH1` | OUTPUT | string(3,0) | `IIF(IS_SPACES(GVT_CURR_APT_AUTH1), NULL, GVT_CURR_APT_AUTH1)` | `CASE WHEN GVT_CURR_APT_AUTH1 RLIKE '^[ \\t\\n\\r\\f\\x0B]*$' THEN NULL ELSE GVT_CURR_APT_AUTH1 END` | PeopleSoft stores blank CHAR fields as a single space; IS_SPACES(' ') is TRUE -> NULL. |
| `GVT_CURR_APT_AUTH2` | INPUT/OUTPUT | string(3,0) | `GVT_CURR_APT_AUTH2` | pass-through of `lkp_PS_GVT_EMPLOYMENT.GVT_CURR_APT_AUTH2` | |
| `o_GVT_CURR_APT_AUTH2` | OUTPUT | string(3,0) | `IIF(IS_SPACES(GVT_CURR_APT_AUTH2), NULL, GVT_CURR_APT_AUTH2)` | `CASE WHEN GVT_CURR_APT_AUTH2 RLIKE '^[ \\t\\n\\r\\f\\x0B]*$' THEN NULL ELSE GVT_CURR_APT_AUTH2 END` | PeopleSoft stores blank CHAR fields as a single space; IS_SPACES(' ') is TRUE -> NULL. |
| `CMPNY_SENIORITY_DT` | INPUT/OUTPUT | date/time(29,9) | `CMPNY_SENIORITY_DT` | pass-through of `lkp_PS_GVT_EMPLOYMENT.CMPNY_SENIORITY_DT` | |
| `SETID_DEPT` | INPUT/OUTPUT | string(5,0) | `SETID_DEPT` | pass-through of `SQ_PS_GVT_JOB.SETID_DEPT` | |
| `FEGLI_LIVING_BNFT_CD` | OUTPUT | string(1,0) | `DECODE(GVT_NOA_CODE, '805', 'F', '806', 'P', NULL)` | `CASE GVT_NOA_CODE WHEN '805' THEN 'F' WHEN '806' THEN 'P' ELSE NULL END` | Unconnected port (dead logic); target FEGLI_LIVING_BNFT_CD is fed by GVT_FEGLI_LIVING. |
| `LWOP_START_DATE` | OUTPUT | date/time(29,9) | `IIF(IN(GVT_NOA_CODE, '460', '473'), EFFDT, NULL)` | `CASE WHEN GVT_NOA_CODE IN ('460', '473') THEN EFFDT ELSE NULL END` |  |
| `BUYOUT_EFFDT` | OUTPUT | date/time(29,9) | `IIF(GVT_NOA_CODE = '825', EFFDT, NULL)` | `CASE WHEN GVT_NOA_CODE = '825' THEN EFFDT ELSE NULL END` |  |
| `BUSINESS_TITLE` | INPUT | string(30,0) | `BUSINESS_TITLE` | pass-through of `lkp_PS_GVT_EMPLOYMENT.BUSINESS_TITLE` | |
| `o_BUSINESS_TITLE` | OUTPUT | string(30,0) | `UPPER(BUSINESS_TITLE)` | `upper(BUSINESS_TITLE)` |  |
| `GVT_PAR_NTE_DATE` | INPUT/OUTPUT | date/time(29,9) | `GVT_PAR_NTE_DATE` | pass-through of `SQ_PS_GVT_JOB.GVT_PAR_NTE_DATE` | |
| `GVT_LEO_POSITION` | INPUT/OUTPUT | string(1,0) | `GVT_LEO_POSITION` | pass-through of `SQ_PS_GVT_JOB.GVT_LEO_POSITION` | |
| `TEMP_GVT_EFFDT` | INPUT/OUTPUT | date/time(29,9) | `TEMP_GVT_EFFDT` | pass-through of `SQ_PS_GVT_JOB.GVT_EFFDT` | |
| `o_BUYOUT_AMT` | OUTPUT | decimal(14,4) | `IIF(GVT_NOA_CODE= '825' and ACTION='BON', GOAL_AMT,NULL)` | `CASE WHEN GVT_NOA_CODE = '825' AND ACTION = 'BON' THEN GOAL_AMT ELSE NULL END` |  |
| `GVT_APPT_LIMIT_HRS` | INPUT | decimal(38,0) | `GVT_APPT_LIMIT_HRS` | pass-through of `lkp_PS_GVT_EMPLOYMENT.GVT_APPT_LIMIT_HRS` | |
| `o_APPT_LIMIT_NTE_HRS` | OUTPUT | decimal(38,0) | `IIF(GVT_APPT_LIMIT_HRS=0,NULL, GVT_APPT_LIMIT_HRS)` | `CASE WHEN GVT_APPT_LIMIT_HRS = 0 THEN NULL ELSE GVT_APPT_LIMIT_HRS END` |  |
| `JPM_INTEGER_2` | INPUT | decimal(38,0) | `JPM_INTEGER_2` | pass-through of `lkp_PS_JPM_JP_ITEMS.JPM_INTEGER_2` | |
| `YEAR_DEGREE` | OUTPUT | decimal(38,0) | `IIF(JPM_INTEGER_2=0, NULL, JPM_INTEGER_2)` | `CASE WHEN JPM_INTEGER_2 = 0 THEN NULL ELSE JPM_INTEGER_2 END` |  |
| `OTH_PAY` | INPUT | decimal(10,2) | `OTH_PAY` | pass-through of `lkp_PS_GVT_AWD_DATA.OTH_PAY` | |
| `o_SEVERANCE_PAY_AMT` | OUTPUT | decimal(8,2) | `IIF(IN (GVT_NOA_CODE,'304', '312', '356' , '357') ,OTH_PAY,NULL)` | `CASE WHEN GVT_NOA_CODE IN ('304', '312', '356', '357') THEN OTH_PAY ELSE NULL END` |  |
| `o_SEVERANCE_TOTAL_AMT` | OUTPUT | decimal(7,2) | `IIF(IN (GVT_NOA_CODE,'304', '312', '356' , '357') ,GOAL_AMT,NULL)` | `CASE WHEN GVT_NOA_CODE IN ('304', '312', '356', '357') THEN GOAL_AMT ELSE NULL END` |  |
| `o_SEVERANCE_PAY_START_DTE` | OUTPUT | date/time(29,9) | `IIF(IN (GVT_NOA_CODE,'304', '312', '356' , '357') ,EFFDT,NULL)` | `CASE WHEN GVT_NOA_CODE IN ('304', '312', '356', '357') THEN EFFDT ELSE NULL END` |  |
| `PROBATION_DT` | INPUT | date/time(29,9) | `PROBATION_DT` | pass-through of `lkp_PS_GVT_EMPLOYMENT.PROBATION_DT` | |
| `o_PROBATION_DT` | OUTPUT | date/time(29,9) | `IIF(ISNULL(PROBATION_DT), NULL, ADD_TO_DATE(PROBATION_DT, 'MM', -12))` | `CASE WHEN PROBATION_DT IS NULL THEN NULL ELSE PROBATION_DT + make_interval(0, -12) END` | ADD_TO_DATE(...,'MM',-12): day clamped to month end (Feb 29 -> Feb 28), time kept. |
| `PAYGROUP` | INPUT/OUTPUT | string(3,0) | `PAYGROUP` | pass-through of `SQ_PS_GVT_JOB.PAYGROUP` | |
| `REPORTS_TO` | INPUT/OUTPUT | string(8,0) | `REPORTS_TO` | pass-through of `SQ_PS_GVT_JOB.REPORTS_TO` | |
| `POSITION_ENTRY_DT` | INPUT/OUTPUT | date/time(29,9) | `POSITION_ENTRY_DT` | pass-through of `SQ_PS_GVT_JOB.POSITION_ENTRY_DT` | |
| `GVT_SCD_LEO` | INPUT/OUTPUT | date/time(29,9) | `GVT_SCD_LEO` | pass-through of `lkp_PS_GVT_EMPLOYMENT.GVT_SCD_LEO` | |
| `GVT_WGI_DUE_DATE` | INPUT/OUTPUT | date/time(29,9) | `GVT_WGI_DUE_DATE` | pass-through of `lkp_PS_GVT_EMPLOYMENT.GVT_WGI_DUE_DATE` | |

## 6. Targets

### EHRP_RECS_TRACKING_TBL

Relational Writer: Insert = `YES`, Update as Update = `YES`, Update as Insert = `NO`, Update else Insert = `NO`, Delete = `YES`, Truncate target table option = `NO`, Reject file directory = `$PMBadFileDir\`

10 of 10 columns are connected; unconnected columns load NULL.

| Column | Oracle type | Nullable | Source port |
|---|---|---|---|
| EMPLID | varchar2(8,0) | NULL | `SQ_PS_GVT_JOB.EMPLID` |
| EMPL_RCD | number(p,s)(2,0) | NULL | `SQ_PS_GVT_JOB.EMPL_RCD` |
| EFFDT | date | NULL | `SQ_PS_GVT_JOB.EFFDT` |
| EFFSEQ | number(p,s)(3,0) | NULL | `SQ_PS_GVT_JOB.EFFSEQ` |
| EVENT_SUBMITTED_DT | date | NULL | `exp_MAIN2BIIS.o_EVENT_SUBMITTED_DTE` |
| GVT_WIP_STATUS | varchar2(3,0) | NULL | `SQ_PS_GVT_JOB.GVT_WIP_STATUS` |
| NOA_CD | varchar2(3,0) | NULL | `SQ_PS_GVT_JOB.GVT_NOA_CODE` |
| NOA_SUFFIX_CD | varchar2(1,0) | NULL | `SQ_PS_GVT_JOB.HE_NOA_EXT` |
| BIIS_EVENT_ID | number(p,s)(10,0) | NOTNULL | `exp_MAIN2BIIS.o_EVENT_ID` |
| LOAD_DATE | date | NULL | `exp_MAIN2BIIS.o_LOAD_DATE` |

### NWK_ACTION_PRIMARY_TBL

Relational Writer: Insert = `YES`, Update as Update = `YES`, Update as Insert = `NO`, Update else Insert = `NO`, Delete = `YES`, Truncate target table option = `NO`, Reject file directory = `$PMBadFileDir\`

124 of 260 columns are connected; unconnected columns load NULL.

| Column | Oracle type | Nullable | Source port |
|---|---|---|---|
| EVENT_ID | number(p,s)(10,0) | NOTNULL | `exp_MAIN2BIIS.o_EVENT_ID` |
| AGCY_ASSIGN_CD | varchar2(4,0) | NULL | `exp_MAIN2BIIS.o_AGCY_ASSIGN_CD` |
| AGCY_SUBELEMENT_CD | varchar2(15,0) | NULL | `exp_MAIN2BIIS.GVT_XFER_TO_AGCY` |
| AGCY_SUBELEMENT_PRIOR_CD | varchar2(15,0) | NULL | `exp_MAIN2BIIS.o_AGCY_SUBELEMENT_PRIOR_CD` |
| ANN_LV_45DAY_CEIL_CD | varchar2(1,0) | NULL | - |
| ANN_LV_CATGRY_CD | number(p,s)(1,0) | NULL | - |
| ANN_LV_CRDT_REDUCTN_HRS | number(p,s)(5,2) | NULL | `exp_MAIN2BIIS.o_HE_AL_RED_CRED` |
| ANN_LV_CUR_BAL_HRS | number(p,s)(6,0) | NULL | `exp_MAIN2BIIS.o_HE_AL_BALANCE` |
| ANN_LV_LUMP_SUM_PAID_HRS | number(p,s)(4,0) | NULL | `exp_MAIN2BIIS.o_HE_LUMP_HRS` |
| ANN_LV_PRIOR_YEAR_BAL_HRS | number(p,s)(6,2) | NULL | `exp_MAIN2BIIS.o_HE_AL_CARRYOVER` |
| ANN_LV_RESTORED_BAL_LV_HRS | number(p,s)(6,2) | NULL | `exp_MAIN2BIIS.o_HE_RES_BALANCE` |
| ANN_LV_RESTORED_BAL1_HRS | number(p,s)(6,2) | NULL | `exp_MAIN2BIIS.o_HE_RES_LASTYR` |
| ANN_LV_RESTORED_BAL2_HRS | number(p,s)(6,2) | NULL | `exp_MAIN2BIIS.o_HE_RES_TWOYRS` |
| ANN_LV_RESTORED_BAL3_HRS | number(p,s)(6,2) | NULL | - |
| ANN_LV_TRANFR_IN_BAL_HRS | number(p,s)(4,0) | NULL | - |
| ANN_LV_YTD_ACCRD_HRS | number(p,s)(5,2) | NULL | - |
| ANN_LV_YTD_USED_HRS | number(p,s)(5,2) | NULL | `exp_MAIN2BIIS.o_HE_AL_TOTAL` |
| ANN_SALARY_RATE_AMT | number(p,s)(14,4) | NULL | `exp_MAIN2BIIS.ANNL_BENEF_BASE_RT` |
| ANNUITANT_IND_CD | varchar2(1,0) | NULL | `exp_MAIN2BIIS.GVT_ANN_IND` |
| APPOINTING_OFFICE_CHANGE_CD | varchar2(3,0) | NULL | - |
| APPT_LMT_NTE_90DAY_CD | varchar2(1,0) | NULL | `lkp_PS_GVT_EMPLOYMENT.GVT_APPT_LIMIT_DYS` |
| APPT_LMT_NTE_90DAY_DTE | date | NULL | - |
| APPT_LMT_NTE_HRS | number(p,s)(10,0) | NULL | `exp_MAIN2BIIS.o_APPT_LIMIT_NTE_HRS` |
| APPT_NTE_DTE | date | NULL | `lkp_PS_GVT_EMPLOYMENT.GVT_APPT_EXPIR_DT` |
| APPT_STATUS_CD | varchar2(1,0) | NULL | - |
| APPT_TYPE_CD | varchar2(2,0) | NULL | `exp_MAIN2BIIS.GVT_TYPE_OF_APPT` |
| AUO_PAY_PCT | number(p,s)(15,4) | NULL | - |
| AUO_PAY_PRIOR_PCT | number(p,s)(15,4) | NULL | - |
| AUTHENTICATION_DTE | date | NULL | - |
| AVAILABILITY_PAY_PCT | number(p,s)(15,4) | NULL | - |
| AWOP_WGI_START_DTE | date | NULL | `lkp_PS_GVT_EMPLOYMENT.LAST_INCREASE_DT` |
| AWOP_YTD_HRS | number(p,s)(6,2) | NULL | `exp_MAIN2BIIS.o_HE_AWOP_SEP` |
| BARGAINING_UNIT_CD | varchar2(15,0) | NULL | `exp_MAIN2BIIS.BARG_UNIT` |
| BASE_HRS | number(p,s)(5,2) | NULL | `exp_MAIN2BIIS.o_BASE_HOURS` |
| BIRTH_DTE | date | NULL | `exp_PERS_DATA.BIRTHDATE` |
| CAN_CD | varchar2(15,0) | NULL | `exp_MAIN2BIIS.ACCT_CD` |
| CAN_NEW_CD | varchar2(15,0) | NULL | - |
| CAREER_START_DTE | date | NULL | `lkp_PS_GVT_EMPLOYMENT.GVT_CNV_BEGIN_DATE` |
| CASH_AWARD_AMT | number(p,s)(15,4) | NULL | `exp_MAIN2BIIS.o_CASH_AWARD_AMT` |
| CASH_AWARD_BNFT_AMT | number(p,s)(10,2) | NULL | `exp_MAIN2BIIS.o_CASH_AWARD_BNFT_AMT` |
| CEIL_REPORTING_CD | varchar2(1,0) | NULL | - |
| CHRTY_AREA_CD | varchar2(10,0) | NULL | - |
| CHRTY_DED_AMT | number(p,s)(14,4) | NULL | - |
| CITY_TAX_CD | varchar2(10,0) | NULL | - |
| CITY_TAX_DED_AMT | number(p,s)(5,2) | NULL | - |
| CITY_TAX_DED_PCT | number(p,s)(6,3) | NULL | - |
| CITY_TAX_MARTL_STATUS_CD | varchar2(17,0) | NULL | - |
| CITY_TAX_RESID_CD | varchar2(17,0) | NULL | - |
| CITY_TAX_TOT_EXEMPT_COUNT | number(p,s)(15,4) | NULL | - |
| CNTY_TAX_CD | varchar2(10,0) | NULL | - |
| CNTY_TAX_DED_PCT | number(p,s)(6,3) | NULL | - |
| CNTY_TAX_MARTL_STATUS_CD | varchar2(17,0) | NULL | - |
| CNTY_TAX_RESID_CD | varchar2(17,0) | NULL | - |
| CNTY_TAX_TOT_EXEMPT_COUNT | number(p,s)(15,4) | NULL | - |
| COMPETITIVE_LEVEL_CD | varchar2(4,0) | NULL | `lkp_PS_GVT_EMPLOYMENT.GVT_COMP_LVL_PERM` |
| COMPUTER_POSITION_CD | varchar2(1,0) | NULL | - |
| CORR_CANCL_EFF_DTE | date | NULL | - |
| CORR_CANCL_LEGAL_AUTH_CD | varchar2(3,0) | NULL | - |
| CORR_CANCL_LEGAL_AUTH_TXT | varchar2(250,0) | NULL | - |
| CORR_CANCL_LEGAL_AUTH2_CD | varchar2(3,0) | NULL | - |
| CORR_CANCL_LEGAL_AUTH2_TXT | varchar2(250,0) | NULL | - |
| CORR_CANCL_NOA_CD | varchar2(3,0) | NULL | - |
| CORR_CANCL_NOA_SUFFIX_CD | varchar2(1,0) | NULL | - |
| CRDTBL_MIL_SRVC_PERIOD | varchar2(8,0) | NULL | `exp_PERS_DATA.GVT_CRED_MIL_SVCE` |
| DETAIL_TIMEKEEPER_NUM | varchar2(10,0) | NULL | - |
| DUTY_STATION_CD | varchar2(15,0) | NULL | `exp_MAIN2BIIS.LOCATION` |
| DUTY_STATION_PRIOR_CD | varchar2(15,0) | NULL | - |
| EDUCATION_ALLOWC_AMT | number(p,s)(7,2) | NULL | - |
| EDUCATION_LEVEL_CD | varchar2(2,0) | NULL | `lkp_PS_JPM_JP_ITEMS.JPM_CAT_ITEM_ID` |
| EMP_EOD_DTE | date | NULL | `lkp_PS_GVT_EMPLOYMENT.HIRE_DT` |
| EMP_FIRST_NAME | varchar2(40,0) | NULL | `exp_PERS_DATA.FIRST_NAME` |
| EMP_GRADE_START_DTE | date | NULL | `exp_MAIN2BIIS.GRADE_ENTRY_DT` |
| EMP_LAST_NAME | varchar2(40,0) | NULL | `exp_PERS_DATA.LAST_NAME` |
| EMP_MID_INIT | varchar2(40,0) | NULL | `exp_PERS_DATA.MIDDLE_NAME` |
| EMP_PREV_FIRST_NAME | varchar2(40,0) | NULL | - |
| EMP_PREV_LAST_NAME | varchar2(40,0) | NULL | - |
| EMP_PREV_MID_INIT | varchar2(40,0) | NULL | - |
| EMP_RESID_CITY_ST_NAME | varchar2(60,0) | NULL | `exp_MAIN2BIIS.o_EMP_RESID_CITY_STATE_NAME` |
| EMP_RESID_GEOGPHCL_LOC_CD | varchar2(10,0) | NULL | `exp_PERS_DATA.GEO_CODE` |
| EMP_RESID_POSTAL_CD | varchar2(12,0) | NULL | `exp_PERS_DATA.POSTAL` |
| EMP_RESID_STREET_NAME | varchar2(40,0) | NULL | `exp_PERS_DATA.ADDRESS1` |
| EVENT_EFF_DTE | date | NULL | `exp_MAIN2BIIS.EFFDT` |
| EVENT_SUBMITTED_DTE | date | NULL | `exp_MAIN2BIIS.o_EVENT_SUBMITTED_DTE` |
| FED_TAX_EIC_CD | varchar2(17,0) | NULL | - |
| FED_TAX_EXEMPT_CD | varchar2(17,0) | NULL | - |
| FED_TAX_MARTL_STATUS_CD | varchar2(17,0) | NULL | - |
| FED_TAX_MEDICARE_NON_RESID_CD | varchar2(17,0) | NULL | - |
| FED_TAX_OPT_DED_AMT | number(p,s)(15,4) | NULL | - |
| FED_TAX_TOT_EXEMPT_COUNT | number(p,s)(15,4) | NULL | - |
| FEGLI_CD | varchar2(2,0) | NULL | `exp_MAIN2BIIS.GVT_FEGLI` |
| FEGLI_EFF_DTE | date | NULL | - |
| FEGLI_LIVING_BNFT_CD | varchar2(2,0) | NULL | `exp_MAIN2BIIS.GVT_FEGLI_LIVING` |
| FEGLI_LIVING_BNFT_REMAIN_AMT | number(p,s)(14,4) | NULL | `exp_MAIN2BIIS.GVT_LIVING_AMT` |
| FEHB_DED_AMT | number(p,s)(15,4) | NULL | - |
| FEHB_GOVT_CONTB_AMT | number(p,s)(15,4) | NULL | - |
| FEHB_PLAN_CD | varchar2(4,0) | NULL | - |
| FEHB_PREV_PLAN_CD | varchar2(4,0) | NULL | - |
| FERS_CONV_DTE | date | NULL | - |
| FERS_COV_CD | varchar2(2,0) | NULL | `exp_MAIN2BIIS.GVT_FERS_COVERAGE` |
| FERS_DISABILITY_SSA_BNFT_CD | varchar2(1,0) | NULL | - |
| FERS_SSA_BNFT_AMT | number(p,s)(4,0) | NULL | - |
| FILLING_POSITION_CD | varchar2(1,0) | NULL | `lkp_PS_HE_FILL_POS.HE_FILL_POSITION` |
| FLSA_CATGRY_CD | varchar2(2,0) | NULL | `exp_MAIN2BIIS.FLSA_STATUS` |
| FRINGE_BNFT_AMT | number(p,s)(5,0) | NULL | - |
| FROZEN_SERVICE_PERIOD | varchar2(4,0) | NULL | `exp_MAIN2BIIS.GVT_CSRS_FROZN_SVC` |
| FUNCTNL_CLASSFCTN_CD | varchar2(2,0) | NULL | - |
| FUNCTNL_CLASSFCTN_PRIOR_CD | varchar2(2,0) | NULL | - |
| GRADE_CD | varchar2(2,0) | NULL | `exp_MAIN2BIIS.GRADE` |
| GRADE_PRIOR_CD | varchar2(2,0) | NULL | - |
| HANDICAP_CD | varchar2(2,0) | NULL | `exp_PERS_DATA.GVT_DISABILITY_CD` |
| HRLY_RATE_AMT | number(p,s)(14,4) | NULL | `exp_MAIN2BIIS.HOURLY_RT` |
| INDIAN_PREFERENCE_CD | varchar2(1,0) | NULL | - |
| INSTRUCTIONAL_PROGRAM_CD | varchar2(6,0) | NULL | `lkp_PS_JPM_JP_ITEMS.MAJOR_CODE` |
| LEGAL_AUTH_CD | varchar2(3,0) | NULL | `exp_MAIN2BIIS.GVT_LEG_AUTH_1` |
| LEGAL_AUTH_TXT | varchar2(250,0) | NULL | `exp_MAIN2BIIS.o_LEG_AUTH_TXT_1` |
| LEGAL_AUTH2_CD | varchar2(3,0) | NULL | `exp_MAIN2BIIS.GVT_LEG_AUTH_2` |
| LEGAL_AUTH2_TXT | varchar2(250,0) | NULL | `exp_MAIN2BIIS.o_LEG_AUTH_TXT_2` |
| LOCALITY_PAY_AMT | number(p,s)(10,0) | NULL | `exp_MAIN2BIIS.GVT_LOCALITY_ADJ` |
| LOCALITY_PAY_PCT | number(p,s)(15,4) | NULL | - |
| LOCALITY_PAY_PRIOR_AMT | number(p,s)(10,0) | NULL | - |
| LV_REDUCTN_HRS | number(p,s)(5,2) | NULL | - |
| LV_SCD_DTE | date | NULL | `lkp_PS_GVT_EMPLOYMENT.SERVICE_DT` |
| LWOP_AWOP_WGI_HRS | number(p,s)(6,2) | NULL | `exp_MAIN2BIIS.o_HE_AWOP_WIGI` |
| MEDICAL_OFFICER_IND_CD | varchar2(1,0) | NULL | - |
| MEDICAL_OFFICER_IND_PRIOR_CD | varchar2(1,0) | NULL | - |
| MSA_CD | varchar2(4,0) | NULL | - |
| NATL_UNION_DUES_BWKLY_DED_AMT | number(p,s)(14,4) | NULL | - |
| NATL_UNION_DUES_CD | varchar2(2,0) | NULL | - |
| NATL_UNION_DUES_HRLY_RATE_AMT | number(p,s)(15,4) | NULL | - |
| NOA_CD | varchar2(3,0) | NULL | `exp_MAIN2BIIS.GVT_NOA_CODE` |
| NOA_SUFFIX_CD | varchar2(1,0) | NULL | `exp_MAIN2BIIS.HE_NOA_EXT` |
| OATH_DTE | date | NULL | - |
| OCCUPATION_CD | varchar2(10,0) | NULL | - |
| OCCUPATION_PRIOR_CD | varchar2(10,0) | NULL | - |
| OCCUPATIONAL_CATGRY_CD | varchar2(10,0) | NULL | - |
| ORGTNL_COMPONENT_CD | varchar2(15,0) | NULL | `exp_MAIN2BIIS.DEPTID` |
| ORGTNL_COMPONENT_PRIOR_CD | varchar2(15,0) | NULL | - |
| OT_RATE_AMT | number(p,s)(5,2) | NULL | - |
| PAY_BASIS_CD | varchar2(2,0) | NULL | `exp_MAIN2BIIS.GVT_PAY_BASIS` |
| PAY_BASIS_PRIOR_CD | varchar2(2,0) | NULL | - |
| PAY_PLAN_CD | varchar2(2,0) | NULL | `exp_MAIN2BIIS.GVT_PAY_PLAN` |
| PAY_PLAN_PRIOR_CD | varchar2(2,0) | NULL | - |
| PCA_CATGRY_CD | varchar2(15,0) | NULL | - |
| PCA_CONTRACT_LENGTH_YRS | varchar2(15,0) | NULL | - |
| PCA_CONTRACT_NTE_AMT | number(p,s)(14,4) | NULL | - |
| PCA_CONTRACT_NTE_DTE | date | NULL | - |
| PERMANENT_TEMP_POSITION_CD | varchar2(2,0) | NULL | `exp_MAIN2BIIS.o_PERM_TEMP_POSITION_CD` |
| PERSONNEL_OFFICE_ID_CD | varchar2(15,0) | NULL | `exp_MAIN2BIIS.GVT_POI` |
| PERSONNEL_OFFICE_ID_DIFFRNT_CD | varchar2(15,0) | NULL | - |
| POSITION_CHANGE_END_DTE | date | NULL | `lkp_PS_GVT_EMPLOYMENT.GVT_TEMP_PSN_EXPIR` |
| POSITION_NUM | varchar2(10,0) | NULL | `exp_MAIN2BIIS.JOBCODE` |
| POSITION_OCCUPIED_CD | varchar2(2,0) | NULL | `exp_MAIN2BIIS.GVT_POSN_OCCUPIED` |
| POSITION_PRIOR_NUM | varchar2(10,0) | NULL | - |
| POSITION_SENSITIVITY_CD | varchar2(2,0) | NULL | - |
| POSITION_TITLE_NAME | varchar2(32,0) | NULL | `exp_MAIN2BIIS.o_BUSINESS_TITLE` |
| POSITION_TITLE_PRIOR_NAME | varchar2(32,0) | NULL | - |
| PRD_CD | varchar2(1,0) | NULL | `exp_MAIN2BIIS.GVT_PAY_RATE_DETER` |
| PRD_EFF_DTE | date | NULL | - |
| PRD_PRIOR_CD | varchar2(1,0) | NULL | - |
| PREV_RETMT_COV_CD | varchar2(2,0) | NULL | `exp_MAIN2BIIS.GVT_PREV_RET_COVRG` |
| PROB_TRIAL_PERIOD_START_DTE | date | NULL | `exp_MAIN2BIIS.o_PROBATION_DT` |
| PSP_CONTRACT_EFF_DTE | date | NULL | - |
| PSP_CONTRACT_LENGTH_YRS | number(p,s)(1,0) | NULL | - |
| PSP_CONTRACT_NTE_DTE | date | NULL | - |
| PSP_LENGTH_SRVC_INCREASE_AMT | number(p,s)(7,0) | NULL | - |
| PSP_LENGTH_SRVC_CHANGE_DTE | date | NULL | - |
| PSP_TOT_AMT | number(p,s)(14,4) | NULL | - |
| QUARTERS_DED_AMT | number(p,s)(14,4) | NULL | - |
| RACE_NATL_ORIGIN_CD | varchar2(2,0) | NULL | `exp_PERS_DATA.ETHNIC_GROUP` |
| RATING_RECORD_PATTERN_CD | varchar2(1,0) | NULL | - |
| REEMP_ANNUITANT_HRLY_RATE_AMT | number(p,s)(14,4) | NULL | `exp_MAIN2BIIS.o_GVT_ANNUITY_OFFSET` |
| REEMP_ANNUITANT_MONTHLY_AMT | number(p,s)(15,4) | NULL | `exp_MAIN2BIIS.o_GVT_ANNUITY_OFFSET` |
| REGION_OPDIV_CD | varchar2(2,0) | NULL | - |
| RETMT_PLAN_CD | varchar2(4,0) | NULL | `exp_MAIN2BIIS.GVT_RETIRE_PLAN` |
| SCHLD_ANN_SALARY_AMT | number(p,s)(15,4) | NULL | `exp_MAIN2BIIS.GVT_COMPRATE` |
| SCHLD_ANN_SALARY_PRIOR_AMT | number(p,s)(14,4) | NULL | - |
| SCHLD_HRLY_RATE_AMT | number(p,s)(14,4) | NULL | `exp_MAIN2BIIS.GVT_HRLY_RT_NO_LOC` |
| SES_POSITION_LEAVING_REASON_CD | varchar2(1,0) | NULL | - |
| SES_SABBATICAL_NTE_DTE | date | NULL | - |
| SEVERANCE_PAY_AMT | number(p,s)(7,2) | NULL | `exp_MAIN2BIIS.o_SEVERANCE_PAY_AMT` |
| SEVERANCE_PAY_FINAL_DTE | date | NULL | - |
| SEVERANCE_PAY_START_DTE | date | NULL | `exp_MAIN2BIIS.o_SEVERANCE_PAY_START_DTE` |
| SEVERANCE_PAY_TOT_AMT | number(p,s)(8,2) | NULL | `exp_MAIN2BIIS.o_SEVERANCE_TOTAL_AMT` |
| SEVERANCE_PAY_WEEKS_COUNT | number(p,s)(2,0) | NULL | - |
| SEX_CD | varchar2(1,0) | NULL | `exp_PERS_DATA.SEX` |
| SICK_LV_CRDT_REDUCTN_HRS | number(p,s)(5,2) | NULL | `exp_MAIN2BIIS.o_HE_SL_RED_CRED` |
| SICK_LV_CUR_BAL_HRS | number(p,s)(6,0) | NULL | `exp_MAIN2BIIS.o_HE_SL_BALANCE` |
| SICK_LV_FERS_ELECT_BAL_HRS | number(p,s)(8,2) | NULL | `exp_MAIN2BIIS.o_HE_FROZEN_SL` |
| SICK_LV_PRIOR_YEAR_BAL_HRS | number(p,s)(6,2) | NULL | `exp_MAIN2BIIS.o_HE_SL_CARRYOVER` |
| SICK_LV_TRANFR_IN_BAL_HRS | number(p,s)(4,0) | NULL | - |
| SICK_LV_YTD_ACCRD_HRS | number(p,s)(5,2) | NULL | `exp_MAIN2BIIS.o_HE_SL_ACCRUAL` |
| SICK_LV_YTD_USED_HRS | number(p,s)(6,2) | NULL | `exp_MAIN2BIIS.o_HE_SL_TOTAL` |
| SPECIAL_PAY_CD | varchar2(2,0) | NULL | - |
| SPECIAL_PAY_REASON_CD | varchar2(1,0) | NULL | - |
| SPECIAL_PROGRAM_CD | number(p,s)(2,0) | NULL | `lkp_PS_GVT_EMPLOYMENT.GVT_SPEP` |
| SSN | varchar2(20,0) | NULL | `lkp_PS_GVT_PERS_NID.NATIONAL_ID` |
| ST_TAX_CD | varchar2(10,0) | NULL | - |
| ST_TAX_MARTL_STATUS_CD | varchar2(17,0) | NULL | - |
| ST_TAX_NON_RESID_CD | varchar2(17,0) | NULL | - |
| ST_TAX_OPT_DED_AMT | number(p,s)(15,4) | NULL | - |
| ST_TAX_TOT_EXEMPT_COUNT | number(p,s)(15,4) | NULL | - |
| STEP_CD | varchar2(15,0) | NULL | `exp_MAIN2BIIS.GVT_STEP` |
| STEP_PRIOR_CD | varchar2(15,0) | NULL | - |
| SUPERVSRY_MGRL_PROB_START_DTE | date | NULL | `lkp_PS_GVT_EMPLOYMENT.GVT_SUPV_PROB_DT` |
| SUPERVSRY_STATUS_CD | varchar2(1,0) | NULL | - |
| SUSPENSION_END_DTE | date | NULL | `lkp_PS_GVT_EMPLOYMENT.GVT_DETAIL_EXPIRES` |
| TEMP_PROMTN_EXP_DTE | date | NULL | `lkp_PS_GVT_EMPLOYMENT.GVT_TEMP_PRO_EXPIR` |
| TENURE_CD | varchar2(2,0) | NULL | `lkp_PS_GVT_EMPLOYMENT.GVT_TENURE` |
| TERMINAL_SITE_CD | number(p,s)(2,0) | NULL | - |
| TERMINAL_SITE_DIFFRNT_CD | number(p,s)(2,0) | NULL | - |
| TIMEKEEPER_NUM | varchar2(10,0) | NULL | - |
| TSTG_DESIGNTD_POSITION_CD | varchar2(2,0) | NULL | - |
| TSTG_DESIGNTD_POSITION_EFF_DTE | date | NULL | - |
| UNION_CD | varchar2(15,0) | NULL | `exp_MAIN2BIIS.o_UNION_CD` |
| UNION_DED_AMT | number(p,s)(14,4) | NULL | - |
| UNION_DED_TYPE_CD | varchar2(2,0) | NULL | - |
| US_CITIZENSHIP_CD | varchar2(2,0) | NULL | `exp_MAIN2BIIS.o_CITIZENSHIP_STATUS` |
| VETERANS_PREFERENCE_CD | varchar2(2,0) | NULL | `exp_PERS_DATA.GVT_VET_PREF_APPT` |
| VETERANS_STATUS_CD | varchar2(1,0) | NULL | `exp_PERS_DATA.MILITARY_STATUS` |
| VLNTRY_SEP_INCENTIVE_PYMT_CD | varchar2(1,0) | NULL | - |
| WAGE_BOARD_SHIFT2_AMT | number(p,s)(4,2) | NULL | - |
| WAGE_BOARD_SHIFT3_AMT | number(p,s)(4,2) | NULL | - |
| WGI_START_DTE | date | NULL | `exp_MAIN2BIIS.STEP_ENTRY_DT` |
| WGI_STATUS_CD | varchar2(2,0) | NULL | `lkp_PS_GVT_EMPLOYMENT.GVT_WGI_STATUS` |
| WORK_SCHEDULE_CD | varchar2(2,0) | NULL | `exp_MAIN2BIIS.GVT_WORK_SCHED` |
| WORK_SCHEDULE_CHANGE_CD | varchar2(2,0) | NULL | - |
| WORK_SCHEDULE_PRIOR_CD | varchar2(2,0) | NULL | - |
| YEAR_DEGREE_ATTAINED_DTE | number(p,s)(4,0) | NULL | `exp_MAIN2BIIS.YEAR_DEGREE` |
| AS_OF_DAY | varchar2(10,0) | NULL | `exp_MAIN2BIIS.o_LOAD_DATE` |
| LOAD_ID | varchar2(10,0) | NOTNULL | `exp_MAIN2BIIS.o_LOAD_ID` |
| HB_PRETAX_CODE | varchar2(1,0) | NULL | - |
| HB_PRETAX_PP_DED | number(p,s)(7,2) | NULL | - |
| HB_PRETAX_TOTAL_PREPAID | number(p,s)(7,2) | NULL | - |
| EMP_ID | varchar2(8,0) | NULL | `exp_MAIN2BIIS.EMPLID` |
| LOAD_DATE | date | NULL | `exp_MAIN2BIIS.o_LOAD_DATE` |
| NEW_SSN | varchar2(9,0) | NULL | - |
| OLD_SSN | varchar2(9,0) | NULL | - |
| EFFSEQ | number(p,s)(3,0) | NULL | `exp_MAIN2BIIS.o_EFFSEQ` |
| EMPL_REC_NO | number(p,s)(2,0) | NULL | `exp_MAIN2BIIS.EMPL_RCD` |
| EHRP_TYPE_ACTION | varchar2(3,0) | NULL | `exp_MAIN2BIIS.ACTION` |
| EHRP_ACTION_REASON | varchar2(3,0) | NULL | `exp_MAIN2BIIS.ACTION_REASON` |
| GVT_WIP_STATUS | varchar2(3,0) | NULL | `exp_MAIN2BIIS.GVT_WIP_STATUS` |
| GVT_STATUS_TYPE | varchar2(3,0) | NULL | `exp_MAIN2BIIS.GVT_STATUS_TYPE` |
| EHRP_POSITION_NUMBER | varchar2(8,0) | NULL | `exp_MAIN2BIIS.POSITION_NBR` |
| EHRP_POSITION_PRIOR_NUMBER | varchar2(8,0) | NULL | - |
| CORR_CANCL_EFFSEQ | number(p,s)(3,0) | NULL | - |
| TITLE42_IND | varchar2(1,0) | NULL | - |
| TITLE38_IND | varchar2(1,0) | NULL | - |
| OPDIV | varchar2(8,0) | NULL | - |
| OPDIV_PRIOR | varchar2(8,0) | NULL | - |
| SETID | varchar2(5,0) | NULL | `exp_MAIN2BIIS.SETID_DEPT` |
| CURR_APPT_AUTH1_CD | varchar2(3,0) | NULL | `exp_MAIN2BIIS.o_GVT_CURR_APT_AUTH1` |
| CURR_APPT_AUTH2_CD | varchar2(3,0) | NULL | `exp_MAIN2BIIS.o_GVT_CURR_APT_AUTH2` |
| LEO_POSITION_CD | varchar2(1,0) | NULL | `exp_MAIN2BIIS.GVT_LEO_POSITION` |
| TEMP_GVT_EFFDT | date | NULL | `exp_MAIN2BIIS.TEMP_GVT_EFFDT` |
| REPORTS_TO | varchar2(8,0) | NULL | `exp_MAIN2BIIS.REPORTS_TO` |
| POSITION_ENTRY_DT | date | NULL | `exp_MAIN2BIIS.POSITION_ENTRY_DT` |
| WGI_DUE_DT | date | NULL | `exp_MAIN2BIIS.GVT_WGI_DUE_DATE` |
| EHRP_CHANGED_WIP_STATUS_DT | date | NULL | - |
| BIIS_CHANGED_WIP_STATUS_DT | date | NULL | - |

### NWK_ACTION_SECONDARY_TBL

Relational Writer: Insert = `YES`, Update as Update = `YES`, Update as Insert = `NO`, Update else Insert = `NO`, Delete = `YES`, Truncate target table option = `NO`, Reject file directory = `$PMBadFileDir\`

34 of 209 columns are connected; unconnected columns load NULL.

| Column | Oracle type | Nullable | Source port |
|---|---|---|---|
| EVENT_ID | number(p,s)(10,0) | NOTNULL | `exp_MAIN2BIIS.o_EVENT_ID` |
| ADTNL_OPT_LIFE_INS_CD | varchar2(1,0) | NULL | - |
| ALLOT_CD | varchar2(2,0) | NULL | - |
| ALLOT_DED_AMT | number(p,s)(8,2) | NULL | - |
| ALLOT_EFF_DTE | date | NULL | - |
| ALLOT_EMP_ACCT_NUM | varchar2(17,0) | NULL | - |
| ALLOT_EMP_ACCT_TYPE_CD | varchar2(2,0) | NULL | - |
| ALLOT_FINCL_INSTN_CITY_ST_NAME | varchar2(60,0) | NULL | - |
| ALLOT_FINCL_INSTN_NAME | varchar2(43,0) | NULL | - |
| ALLOT_FINCL_INSTN_NUM | varchar2(10,0) | NULL | - |
| ALLOT_FINCL_INSTN_POSTAL_CD | varchar2(12,0) | NULL | - |
| ALLOT_FINCL_INSTN_STREET_NAME | varchar2(40,0) | NULL | - |
| BOND_COBEN_CD | varchar2(2,0) | NULL | - |
| BOND_COBEN_NAME | varchar2(120,0) | NULL | - |
| BOND_COBEN_SSN | varchar2(20,0) | NULL | - |
| BOND_DENOM_CD | varchar2(15,0) | NULL | - |
| BOND_INSCRIPTION_NUM | varchar2(15,0) | NULL | - |
| BOND_NOTE_TOT_DED_AMT | number(p,s)(7,2) | NULL | - |
| BOND_OWNER_CITY_NAME | varchar2(30,0) | NULL | - |
| BOND_OWNER_NAME | varchar2(120,0) | NULL | - |
| BOND_OWNER_POSTAL_CD | varchar2(12,0) | NULL | - |
| BOND_OWNER_SSN | varchar2(20,0) | NULL | - |
| BOND_OWNER_ST_NAME | varchar2(30,0) | NULL | - |
| BOND_OWNER_STREET_NAME | varchar2(40,0) | NULL | - |
| BOND_REFUND_IND | varchar2(1,0) | NULL | - |
| BUYOUT_AMT | number(p,s)(7,2) | NULL | `exp_MAIN2BIIS.o_BUYOUT_AMT` |
| BUYOUT_EFF_DTE | date | NULL | `exp_MAIN2BIIS.BUYOUT_EFFDT` |
| CLAIM_TYPE_CD | number(p,s)(1,0) | NULL | - |
| COLA_PCT | number(p,s)(8,2) | NULL | - |
| COURT_ORDER_APPLCTN_CD | varchar2(1,0) | NULL | - |
| CSA_CD | varchar2(2,0) | NULL | - |
| CSA_DED_AMT | number(p,s)(8,2) | NULL | - |
| CSA_DED_PCT | number(p,s)(8,2) | NULL | - |
| CSA_RECIPIENT_NAME | varchar2(120,0) | NULL | - |
| CSRS_ADMIN_FEE_CD | varchar2(1,0) | NULL | - |
| ENVIRN_DIF_PCT | number(p,s)(8,2) | NULL | - |
| FAMILY_LIFE_INS_CD | number(p,s)(1,0) | NULL | - |
| FORGN_LANG_AWARD_AMT | number(p,s)(5,0) | NULL | - |
| FORGN_LANG_AWARD_SEP_AMT | number(p,s)(5,0) | NULL | - |
| FORGN_POST_ALLOWC_TOT_AMT | number(p,s)(7,2) | NULL | - |
| FORGN_POST_DIF_PAY_PCT | number(p,s)(6,3) | NULL | - |
| FURLOUGH_EXP_DTE | date | NULL | - |
| HAZARD_DUTY_DIF_PCT | number(p,s)(8,2) | NULL | - |
| INDVDL_GROUP_AWARD | varchar2(4,0) | NULL | - |
| LAUNDRY_DED_AMT | number(p,s)(7,2) | NULL | - |
| LIFE_INS_AMT | number(p,s)(3,0) | NULL | - |
| LIFE_INS_REDUCTN_CD | number(p,s)(1,0) | NULL | - |
| LIVING_QUARTERS_ALLOWC_TOT_AMT | number(p,s)(7,2) | NULL | - |
| LWOP_EXP_DTE | date | NULL | - |
| LWP_EXP_DTE | date | NULL | - |
| MERIT_PAY_AGGREGATE_CD | varchar2(1,0) | NULL | - |
| MERIT_PAY_CERTIFICATE_IND | varchar2(1,0) | NULL | - |
| MERIT_PAY_CRITICAL_ELEMENT_CD | varchar2(1,0) | NULL | - |
| MERIT_PAY_GRADE_CD | number(p,s)(2,0) | NULL | - |
| MERIT_PAY_HRLY_RATE_AMT | number(p,s)(4,2) | NULL | - |
| MERIT_PAY_INCREASE_AMT | number(p,s)(8,2) | NULL | - |
| MERIT_PAY_PAID_OUT_IND | varchar2(1,0) | NULL | - |
| MERIT_PAY_PERF_PLAN_DTE | date | NULL | - |
| MERIT_PAY_PERF_PLAN_END_DTE | date | NULL | - |
| MERIT_PAY_PERF_SCORE_NUM | number(p,s)(3,0) | NULL | - |
| MERIT_PAY_POOL_DESIGNATOR_CD | varchar2(5,0) | NULL | - |
| MERIT_PAY_PRD_DTE | date | NULL | - |
| MERIT_PAY_SALARY_AMT | number(p,s)(8,2) | NULL | - |
| MERIT_PAY_SCALE_CD | varchar2(1,0) | NULL | - |
| MERIT_PAY_STEP_CD | number(p,s)(2,0) | NULL | - |
| MIL_LV_CUR_FY_HRS | number(p,s)(5,0) | NULL | `exp_MAIN2BIIS.HE_REG_MILITARY` |
| MIL_LV_EMERG_CUR_FY_HRS | number(p,s)(3,0) | NULL | `exp_MAIN2BIIS.HE_SPC_MILITARY` |
| MIL_RETIRED_PAY_RECPNT_CD | varchar2(1,0) | NULL | - |
| MIL_RETIRED_PAY_WAIVER_CD | varchar2(1,0) | NULL | - |
| MIL_RETMT_DTE | date | NULL | - |
| MIL_RETMT_OFFSET_CD | varchar2(1,0) | NULL | - |
| MIL_RETMT_TOT_DED_AMT | number(p,s)(7,2) | NULL | - |
| MIL_SRVC_ACHVD_RANK_CD | varchar2(2,0) | NULL | - |
| MIL_SRVC_BRANCH_CD | varchar2(2,0) | NULL | `exp_PERS_DATA.GVT_MILITARY_COMP` |
| MIL_SRVC_BRANCH_COMPONENT_CD | varchar2(2,0) | NULL | `exp_PERS_DATA.GVT_MILITARY_COMP` |
| NON_BAYLOR_BASE_HRLY_RATE_AMT | number(p,s)(5,2) | NULL | - |
| NON_FORGN_POST_DIF_PAY_PCT | number(p,s)(6,3) | NULL | - |
| NON_GOVT_RETMT_CD | varchar2(2,0) | NULL | - |
| OTHER_PAY_AMT | number(p,s)(5,0) | NULL | - |
| OWCP_CUR_RECEIPT_CD | varchar2(1,0) | NULL | - |
| PAY_TABLE_NUM | varchar2(10,0) | NULL | `exp_MAIN2BIIS.SAL_ADMIN_PLAN` |
| PERF_END1_DTE | date | NULL | - |
| PERF_END2_DTE | date | NULL | - |
| PERF_RATING_RECORD_LEVEL_CD | varchar2(15,0) | NULL | - |
| POST56_MIL_SRVC_DEPST_PAID_CD | varchar2(1,0) | NULL | - |
| PREMIUM_PAY_PCT | number(p,s)(8,2) | NULL | - |
| RECRUITMENT_BONUS_AMT | number(p,s)(6,0) | NULL | `exp_MAIN2BIIS.o_RECRUITMENT_BONUS_AMT` |
| RECRUITMENT_EXP_DTE | date | NULL | `exp_MAIN2BIIS.o_RECRUITMENT_EXP_DTE` |
| REG_SCHLD_STANDBY_DUTY_PAY_PCT | number(p,s)(8,2) | NULL | - |
| RELOCATION_BONUS_AMT | number(p,s)(6,0) | NULL | `exp_MAIN2BIIS.o_RELOCATION_BONUS_AMT` |
| RELOCATION_EXP_DTE | date | NULL | `exp_MAIN2BIIS.o_RELOCATION_EXP_DTE` |
| RETENTION_BONUS_BWKLY_AMT | number(p,s)(5,0) | NULL | - |
| RETENTION_BONUS_TOT_AMT | number(p,s)(6,0) | NULL | - |
| RETMT_REFUND_RECEIVED_CD | varchar2(1,0) | NULL | - |
| RETMT_SCD_DTE | date | NULL | `exp_MAIN2BIIS.GVT_SCD_RETIRE` |
| RETMT_SURVIVOR_ELECT_CD | varchar2(1,0) | NULL | - |
| RETMT_TAX_WITH_CD | varchar2(1,0) | NULL | - |
| RETND1_EFF_DTE | date | NULL | `exp_MAIN2BIIS.GVT_RTND_GRADE_BEG` |
| RETND1_EXP_DTE | date | NULL | `exp_MAIN2BIIS.GVT_RTND_GRADE_EXP` |
| RETND1_GRADE_CD | varchar2(15,0) | NULL | `exp_MAIN2BIIS.GVT_RTND_GRADE` |
| RETND1_MEDICAL_OFFICER_IND_CD | varchar2(1,0) | NULL | - |
| RETND1_OCCUPATION_CD | varchar2(4,0) | NULL | - |
| RETND1_ORGTNL_COMPONENT_CD | varchar2(11,0) | NULL | - |
| RETND1_PAY_PLAN_CD | varchar2(15,0) | NULL | `exp_MAIN2BIIS.GVT_RTND_PAY_PLAN` |
| RETND1_STEP_CD | varchar2(15,0) | NULL | `exp_MAIN2BIIS.GVT_RTND_STEP` |
| RETND2_EFF_DTE | date | NULL | - |
| RETND2_EXP_DTE | date | NULL | - |
| RETND2_GRADE_CD | varchar2(15,0) | NULL | - |
| RETND2_MEDICAL_OFFICER_IND_CD | varchar2(1,0) | NULL | - |
| RETND2_OCCUPATION_CD | varchar2(4,0) | NULL | - |
| RETND2_ORGTNL_COMPONENT_CD | varchar2(11,0) | NULL | - |
| RETND2_PAY_PLAN_CD | varchar2(15,0) | NULL | - |
| RETND2_STEP_CD | varchar2(15,0) | NULL | - |
| RIF_RELEASE_CD | varchar2(1,0) | NULL | - |
| RIF_SCD_DTE | date | NULL | `exp_MAIN2BIIS.CMPNY_SENIORITY_DT` |
| RIF_VETERANS_CD | varchar2(2,0) | NULL | - |
| SAVE_RATE_DTE | date | NULL | - |
| SBRS_ADTNL_BWKLY_DED_AMT | number(p,s)(8,2) | NULL | - |
| SBRS_ADTNL_BWKLY_REDUCTN_AMT | number(p,s)(8,2) | NULL | - |
| SBRS_BWKLY_DED_AMT | number(p,s)(8,2) | NULL | - |
| SBRS_BWKLY_REDUCTN_AMT | number(p,s)(8,2) | NULL | - |
| SBRS_CONTRACT_NUM | varchar2(8,0) | NULL | - |
| SBRS_EMP_DED_PCT | number(p,s)(3,3) | NULL | - |
| SBRS_EMP_OTHER_BWKLY_DED_AMT | number(p,s)(8,2) | NULL | - |
| SBRS_EMP_OTHER_DED_PCT | number(p,s)(3,3) | NULL | - |
| SBRS_EMP_REDUCTN_PCT | number(p,s)(3,3) | NULL | - |
| SBRS_EMPLYR_OTHER_DED_PCT | number(p,s)(3,3) | NULL | - |
| SBRS_EMPLYR_TIAA_CREF_DED_PCT | number(p,s)(3,3) | NULL | - |
| SBRS_INSTN_CITY_NAME | varchar2(23,0) | NULL | - |
| SBRS_INSTN_NAME | varchar2(30,0) | NULL | - |
| SBRS_INSTN_POSTAL_CD | varchar2(12,0) | NULL | - |
| SBRS_INSTN_ST_NAME | varchar2(2,0) | NULL | - |
| SBRS_INSTN_STREET1_NAME | varchar2(25,0) | NULL | - |
| SBRS_INSTN_STREET2_NAME | varchar2(25,0) | NULL | - |
| SBRS_SRA_BWKLY_REDUCTN_AMT | number(p,s)(8,2) | NULL | - |
| SBRS_SRA_REDUCTN_PCT | number(p,s)(3,3) | NULL | - |
| SEP_ACCT_TYPE_CD | varchar2(2,0) | NULL | - |
| SEP_EMP_ACCT_NUM | varchar2(17,0) | NULL | - |
| SEP_EMP_CITY_NAME | varchar2(30,0) | NULL | - |
| SEP_EMP_POSTAL_CD | varchar2(12,0) | NULL | - |
| SEP_EMP_ST_NAME | varchar2(30,0) | NULL | - |
| SEP_EMP_STREET_NAME | varchar2(40,0) | NULL | - |
| SEP_FED_TAX_MARTL_STATUS_CD | varchar2(17,0) | NULL | - |
| SEP_FED_TAX_TOT_EXEMPT_COUNT | number(p,s)(8,2) | NULL | - |
| SEP_FEHB_PLAN_CD | varchar2(4,0) | NULL | - |
| SEP_FINCL_INSTN_NUM | varchar2(10,0) | NULL | - |
| SEP_MAINT_ALLOWC_AMT | number(p,s)(7,2) | NULL | - |
| SEP_REASON_CD | varchar2(2,0) | NULL | - |
| STAFFING_DIF_BWKLY_AMT | number(p,s)(5,0) | NULL | - |
| STAFFING_DIF_TOT_AMT | number(p,s)(5,0) | NULL | - |
| STANDBY_PAY_PCT | number(p,s)(6,3) | NULL | - |
| STD_OPT_LIFE_INS_CD | number(p,s)(1,0) | NULL | - |
| SUBSISTENCE_DED_AMT | number(p,s)(7,2) | NULL | - |
| SUPERVSRY_DIF_BWKLY_AMT | number(p,s)(5,0) | NULL | - |
| SUPERVSRY_DIF_TOT_AMT | number(p,s)(5,0) | NULL | - |
| T38_PREM_HRLY_RATE_AMT | number(p,s)(5,2) | NULL | - |
| TIAA_CERTIFICATE_CD | varchar2(1,0) | NULL | - |
| TIME_OFF_AWARD_AMT | number(p,s)(7,2) | NULL | `exp_MAIN2BIIS.o_TIME_OFF_AWARD_AMT` |
| TIME_OFF_EFF_DTE | date | NULL | - |
| TIME_OFF_FORFEITED_HRS | number(p,s)(5,2) | NULL | - |
| TIME_OFF_FORFEITED_TOT_HRS | number(p,s)(5,2) | NULL | - |
| TIME_OFF_GRANTED_HRS | number(p,s)(5,2) | NULL | `exp_MAIN2BIIS.o_TIME_OFF_GRANTED_HRS` |
| TIME_OFF_GRANTED_TOT_HRS | number(p,s)(5,2) | NULL | - |
| TIME_OFF_USED_HRS | number(p,s)(5,2) | NULL | - |
| TIME_OFF_USED_TOT_HRS | number(p,s)(5,2) | NULL | - |
| TSP_ADJMT_AMT | number(p,s)(7,2) | NULL | - |
| TSP_ADJMT_START_DTE | date | NULL | - |
| TSP_ADJMT_STOP_DTE | date | NULL | - |
| TSP_EMP_DED_AMT | number(p,s)(7,2) | NULL | - |
| TSP_EMP_DED_RATE_PCT | number(p,s)(8,2) | NULL | - |
| TSP_EMP_PP_UND_DED_AMT | number(p,s)(9,2) | NULL | `exp_MAIN2BIIS.HE_PP_UDED_AMT` |
| TSP_EMP_PP_UND_DED_PYMT_COUNT | number(p,s)(3,0) | NULL | `exp_MAIN2BIIS.HE_NO_TSP_PAYPER` |
| TSP_EMP_PREV_GOVT_CONTB_AMT | number(p,s)(7,2) | NULL | `exp_MAIN2BIIS.HE_TSPA_SUB_YR` |
| TSP_EMP_UND_DED_OUTSTNDG_AMT | number(p,s)(9,2) | NULL | `exp_MAIN2BIIS.HE_EMP_UDED_AMT` |
| TSP_GOVT_UND_DED_OUTSTNDG_AMT | number(p,s)(9,2) | NULL | `exp_MAIN2BIIS.HE_GVT_UDED_AMT` |
| TSP_LOAN_PYMT_COUNT | number(p,s)(3,0) | NULL | - |
| TSP_PAY_SUBJ_YTD_AMT | number(p,s)(8,2) | NULL | - |
| TSP_PP_ADJMT_DED_AMT | number(p,s)(7,2) | NULL | - |
| TSP_SCD_DTE | date | NULL | `exp_MAIN2BIIS.GVT_SCD_TSP` |
| TSP_STATUS_CD | varchar2(2,0) | NULL | - |
| TSP_STATUS_CHANGE_EFF_DTE | date | NULL | - |
| TSP_UND_DED_DELAY_OPTION_CD | varchar2(1,0) | NULL | - |
| TSP_UND_DED_LTR_NUM | number(p,s)(6,0) | NULL | `exp_MAIN2BIIS.HE_TLTR_NO` |
| TSP_UND_DED_OPTION_CD | varchar2(1,0) | NULL | `exp_MAIN2BIIS.HE_UDED_PAY_CD` |
| TSP_UND_DED_STOP_OPTION_CD | varchar2(1,0) | NULL | `exp_MAIN2BIIS.HE_TSP_CANC_CD` |
| TSP_VESTING_CD | varchar2(2,0) | NULL | `exp_MAIN2BIIS.o_TSP_VESTING_CD` |
| TSPC_EMP_DIST_PCT | number(p,s)(8,2) | NULL | - |
| TSPC_GOVT_DIST_PCT | number(p,s)(5,2) | NULL | - |
| TSPF_EMP_DIST_PCT | number(p,s)(15,4) | NULL | - |
| TSPF_GOVT_DIST_PCT | number(p,s)(5,2) | NULL | - |
| TSPG_EMP_DIST_PCT | number(p,s)(8,2) | NULL | - |
| TSPG_GOVT_DIST_PCT | number(p,s)(5,2) | NULL | - |
| UNIF_ALLOWC_ANN_AMT | number(p,s)(7,2) | NULL | - |
| UNIF_ALLOWC_HRLY_RATE_AMT | number(p,s)(7,2) | NULL | - |
| UNIF_ALLOWC_PP_AMT | number(p,s)(7,2) | NULL | - |
| TSPA_HARDSHIP_STOP | varchar2(8,0) | NULL | - |
| TSPA_HARDSHIP_NOA | varchar2(4,0) | NULL | - |
| TEA_CHOICE_OPTION | varchar2(1,0) | NULL | - |
| TEA_TRANSPORT_MODE | varchar2(1,0) | NULL | - |
| TEA_PAYMENT_OPTION | varchar2(1,0) | NULL | - |
| TEA_EMPLOYER_CONTR_AMT | number(p,s)(7,2) | NULL | - |
| TEA_EMPLOYEE_DEDUCT | number(p,s)(7,2) | NULL | - |
| TEA_CASH_OUT | number(p,s)(7,2) | NULL | - |
| TEA_PRIOR_YR_HHS | number(p,s)(7,2) | NULL | - |
| TEA_COM_CHOICE_START_DTE | date | NULL | - |
| LWOP_START_DTE | date | NULL | `exp_MAIN2BIIS.LWOP_START_DATE` |
| RETENTION_BONUS_END_DATE | date | NULL | - |
| LEO_SCD_DT | date | NULL | `exp_MAIN2BIIS.GVT_SCD_LEO` |
| PAYGROUP | varchar2(3,0) | NULL | `exp_MAIN2BIIS.PAYGROUP` |

## 7. All TRANSFORMFIELDs (pass-through and lookup ports)

Complete port inventory for the remaining transformations (expression ports are in section 5).

<details><summary><code>lkp_OLD_SEQUENCE_NUMBER</code> (Lookup Procedure, 3 ports)</summary>

| Port | Port type | Datatype |
|---|---|---|
| EHRP_YEAR | LOOKUP | decimal(4,0) |
| EHRP_SEQ_NUMBER | LOOKUP/OUTPUT | decimal(4,0) |
| o_CURRENT_YEAR | INPUT/OUTPUT | decimal(10,0) |

</details>

<details><summary><code>SQ_PS_GVT_JOB</code> (Source Qualifier, 250 ports)</summary>

| Port | Port type | Datatype |
|---|---|---|
| EMPLID | INPUT/OUTPUT | string(11,0) |
| EMPL_RCD | INPUT/OUTPUT | decimal(38,0) |
| EFFDT | INPUT/OUTPUT | date/time(29,9) |
| EFFSEQ | INPUT/OUTPUT | decimal(38,0) |
| DEPTID | INPUT/OUTPUT | string(10,0) |
| JOBCODE | INPUT/OUTPUT | string(6,0) |
| POSITION_NBR | INPUT/OUTPUT | string(8,0) |
| POSITION_OVERRIDE | INPUT/OUTPUT | string(1,0) |
| POSN_CHANGE_RECORD | INPUT/OUTPUT | string(1,0) |
| EMPL_STATUS | INPUT/OUTPUT | string(1,0) |
| ACTION | INPUT/OUTPUT | string(3,0) |
| ACTION_DT | INPUT/OUTPUT | date/time(29,9) |
| ACTION_REASON | INPUT/OUTPUT | string(3,0) |
| LOCATION | INPUT/OUTPUT | string(10,0) |
| TAX_LOCATION_CD | INPUT/OUTPUT | string(10,0) |
| JOB_ENTRY_DT | INPUT/OUTPUT | date/time(29,9) |
| DEPT_ENTRY_DT | INPUT/OUTPUT | date/time(29,9) |
| POSITION_ENTRY_DT | INPUT/OUTPUT | date/time(29,9) |
| SHIFT | INPUT/OUTPUT | string(1,0) |
| REG_TEMP | INPUT/OUTPUT | string(1,0) |
| FULL_PART_TIME | INPUT/OUTPUT | string(1,0) |
| COMPANY | INPUT/OUTPUT | string(3,0) |
| PAYGROUP | INPUT/OUTPUT | string(3,0) |
| BAS_GROUP_ID | INPUT/OUTPUT | string(3,0) |
| ELIG_CONFIG1 | INPUT/OUTPUT | string(10,0) |
| ELIG_CONFIG2 | INPUT/OUTPUT | string(10,0) |
| ELIG_CONFIG3 | INPUT/OUTPUT | string(10,0) |
| ELIG_CONFIG4 | INPUT/OUTPUT | string(10,0) |
| ELIG_CONFIG5 | INPUT/OUTPUT | string(10,0) |
| ELIG_CONFIG6 | INPUT/OUTPUT | string(10,0) |
| ELIG_CONFIG7 | INPUT/OUTPUT | string(10,0) |
| ELIG_CONFIG8 | INPUT/OUTPUT | string(10,0) |
| ELIG_CONFIG9 | INPUT/OUTPUT | string(10,0) |
| BEN_STATUS | INPUT/OUTPUT | string(4,0) |
| BAS_ACTION | INPUT/OUTPUT | string(3,0) |
| COBRA_ACTION | INPUT/OUTPUT | string(3,0) |
| EMPL_TYPE | INPUT/OUTPUT | string(1,0) |
| HOLIDAY_SCHEDULE | INPUT/OUTPUT | string(6,0) |
| STD_HOURS | INPUT/OUTPUT | decimal(6,2) |
| STD_HRS_FREQUENCY | INPUT/OUTPUT | string(5,0) |
| OFFICER_CD | INPUT/OUTPUT | string(1,0) |
| EMPL_CLASS | INPUT/OUTPUT | string(3,0) |
| SAL_ADMIN_PLAN | INPUT/OUTPUT | string(4,0) |
| GRADE | INPUT/OUTPUT | string(3,0) |
| GRADE_ENTRY_DT | INPUT/OUTPUT | date/time(29,9) |
| STEP | INPUT/OUTPUT | decimal(38,0) |
| STEP_ENTRY_DT | INPUT/OUTPUT | date/time(29,9) |
| GL_PAY_TYPE | INPUT/OUTPUT | string(6,0) |
| ACCT_CD | INPUT/OUTPUT | string(25,0) |
| EARNS_DIST_TYPE | INPUT/OUTPUT | string(1,0) |
| COMP_FREQUENCY | INPUT/OUTPUT | string(5,0) |
| COMPRATE | INPUT/OUTPUT | decimal(18,6) |
| CHANGE_AMT | INPUT/OUTPUT | decimal(18,6) |
| CHANGE_PCT | INPUT/OUTPUT | decimal(6,3) |
| ANNUAL_RT | INPUT/OUTPUT | decimal(18,3) |
| MONTHLY_RT | INPUT/OUTPUT | decimal(18,3) |
| DAILY_RT | INPUT/OUTPUT | decimal(18,3) |
| HOURLY_RT | INPUT/OUTPUT | decimal(18,6) |
| ANNL_BENEF_BASE_RT | INPUT/OUTPUT | decimal(18,3) |
| SHIFT_RT | INPUT/OUTPUT | decimal(18,6) |
| SHIFT_FACTOR | INPUT/OUTPUT | decimal(4,3) |
| CURRENCY_CD | INPUT/OUTPUT | string(3,0) |
| BUSINESS_UNIT | INPUT/OUTPUT | string(5,0) |
| SETID_DEPT | INPUT/OUTPUT | string(5,0) |
| SETID_JOBCODE | INPUT/OUTPUT | string(5,0) |
| SETID_LOCATION | INPUT/OUTPUT | string(5,0) |
| SETID_SALARY | INPUT/OUTPUT | string(5,0) |
| REG_REGION | INPUT/OUTPUT | string(5,0) |
| DIRECTLY_TIPPED | INPUT/OUTPUT | string(1,0) |
| FLSA_STATUS | INPUT/OUTPUT | string(1,0) |
| EEO_CLASS | INPUT/OUTPUT | string(1,0) |
| FUNCTION_CD | INPUT/OUTPUT | string(2,0) |
| TARIFF_GER | INPUT/OUTPUT | string(2,0) |
| TARIFF_AREA_GER | INPUT/OUTPUT | string(3,0) |
| PERFORM_GROUP_GER | INPUT/OUTPUT | string(2,0) |
| LABOR_TYPE_GER | INPUT/OUTPUT | string(1,0) |
| SPK_COMM_ID_GER | INPUT/OUTPUT | string(9,0) |
| HOURLY_RT_FRA | INPUT/OUTPUT | string(3,0) |
| ACCDNT_CD_FRA | INPUT/OUTPUT | string(1,0) |
| VALUE_1_FRA | INPUT/OUTPUT | string(5,0) |
| VALUE_2_FRA | INPUT/OUTPUT | string(5,0) |
| VALUE_3_FRA | INPUT/OUTPUT | string(5,0) |
| VALUE_4_FRA | INPUT/OUTPUT | string(5,0) |
| VALUE_5_FRA | INPUT/OUTPUT | string(5,0) |
| CTG_RATE | INPUT/OUTPUT | decimal(38,0) |
| PAID_HOURS | INPUT/OUTPUT | decimal(6,2) |
| PAID_FTE | INPUT/OUTPUT | decimal(7,6) |
| PAID_HRS_FREQUENCY | INPUT/OUTPUT | string(5,0) |
| GVT_EFFDT | INPUT/OUTPUT | date/time(29,9) |
| GVT_EFFDT_PROPOSED | INPUT/OUTPUT | date/time(29,9) |
| GVT_TRANS_NBR | INPUT/OUTPUT | decimal(38,0) |
| GVT_TRANS_NBR_SEQ | INPUT/OUTPUT | decimal(38,0) |
| GVT_WIP_STATUS | INPUT/OUTPUT | string(3,0) |
| GVT_STATUS_TYPE | INPUT/OUTPUT | string(3,0) |
| GVT_NOA_CODE | INPUT/OUTPUT | string(3,0) |
| GVT_LEG_AUTH_1 | INPUT/OUTPUT | string(3,0) |
| GVT_PAR_AUTH_D1 | INPUT/OUTPUT | string(25,0) |
| GVT_PAR_AUTH_D1_2 | INPUT/OUTPUT | string(25,0) |
| GVT_LEG_AUTH_2 | INPUT/OUTPUT | string(3,0) |
| GVT_PAR_AUTH_D2 | INPUT/OUTPUT | string(25,0) |
| GVT_PAR_AUTH_D2_2 | INPUT/OUTPUT | string(25,0) |
| GVT_PAR_NTE_DATE | INPUT/OUTPUT | date/time(29,9) |
| GVT_WORK_SCHED | INPUT/OUTPUT | string(1,0) |
| GVT_SUB_AGENCY | INPUT/OUTPUT | string(2,0) |
| GVT_ELIG_FEHB | INPUT/OUTPUT | string(3,0) |
| GVT_FEHB_DT | INPUT/OUTPUT | date/time(29,9) |
| GVT_PAY_RATE_DETER | INPUT/OUTPUT | string(1,0) |
| GVT_STEP | INPUT/OUTPUT | string(2,0) |
| GVT_RTND_PAY_PLAN | INPUT/OUTPUT | string(2,0) |
| GVT_RTND_SAL_PLAN | INPUT/OUTPUT | string(4,0) |
| GVT_RTND_GRADE | INPUT/OUTPUT | string(3,0) |
| GVT_RTND_STEP | INPUT/OUTPUT | decimal(38,0) |
| GVT_RTND_GVT_STEP | INPUT/OUTPUT | string(2,0) |
| GVT_PAY_BASIS | INPUT/OUTPUT | string(2,0) |
| GVT_COMPRATE | INPUT/OUTPUT | decimal(18,6) |
| GVT_LOCALITY_ADJ | INPUT/OUTPUT | decimal(8,2) |
| GVT_BIWEEKLY_RT | INPUT/OUTPUT | decimal(9,2) |
| GVT_DAILY_RT | INPUT/OUTPUT | decimal(9,2) |
| GVT_HRLY_RT_NO_LOC | INPUT/OUTPUT | decimal(18,6) |
| GVT_DLY_RT_NO_LOC | INPUT/OUTPUT | decimal(9,2) |
| GVT_BW_RT_NO_LOC | INPUT/OUTPUT | decimal(9,2) |
| GVT_MNLY_RT_NO_LOC | INPUT/OUTPUT | decimal(18,3) |
| GVT_ANNL_RT_NO_LOC | INPUT/OUTPUT | decimal(18,3) |
| GVT_XFER_FROM_AGCY | INPUT/OUTPUT | string(2,0) |
| GVT_XFER_TO_AGCY | INPUT/OUTPUT | string(2,0) |
| GVT_RETIRE_PLAN | INPUT/OUTPUT | string(2,0) |
| GVT_ANN_IND | INPUT/OUTPUT | string(1,0) |
| GVT_FEGLI | INPUT/OUTPUT | string(2,0) |
| GVT_FEGLI_LIVING | INPUT/OUTPUT | string(1,0) |
| GVT_LIVING_AMT | INPUT/OUTPUT | decimal(38,0) |
| GVT_ANNUITY_OFFSET | INPUT/OUTPUT | decimal(10,2) |
| GVT_CSRS_FROZN_SVC | INPUT/OUTPUT | string(4,0) |
| GVT_PREV_RET_COVRG | INPUT/OUTPUT | string(1,0) |
| GVT_FERS_COVERAGE | INPUT/OUTPUT | string(1,0) |
| GVT_TYPE_OF_APPT | INPUT/OUTPUT | string(2,0) |
| GVT_POI | INPUT/OUTPUT | string(4,0) |
| GVT_POSN_OCCUPIED | INPUT/OUTPUT | string(1,0) |
| GVT_CONT_EMPLID | INPUT/OUTPUT | string(11,0) |
| GVT_ROUTE_NEXT | INPUT/OUTPUT | string(11,0) |
| GVT_CHANGE_FLAG | INPUT/OUTPUT | string(1,0) |
| GVT_TSP_UPD_IND | INPUT/OUTPUT | string(1,0) |
| GVT_PI_UPD_IND | INPUT/OUTPUT | string(1,0) |
| GVT_SF52_NBR | INPUT/OUTPUT | string(10,0) |
| GVT_S113G_CEILING | INPUT/OUTPUT | string(1,0) |
| GVT_LEO_POSITION | INPUT/OUTPUT | string(1,0) |
| GVT_ANNUIT_COM_DT | INPUT/OUTPUT | date/time(29,9) |
| GVT_BASIC_LIFE_RED | INPUT/OUTPUT | string(2,0) |
| GVT_DED_PRORT_DT | INPUT/OUTPUT | date/time(29,9) |
| GVT_FEGLI_BASC_PCT | INPUT/OUTPUT | decimal(7,6) |
| GVT_FEGLI_OPT_PCT | INPUT/OUTPUT | decimal(7,6) |
| GVT_FEHB_PCT | INPUT/OUTPUT | decimal(7,6) |
| GVT_RETRO_FLAG | INPUT/OUTPUT | string(1,0) |
| GVT_RETRO_DED_FLAG | INPUT/OUTPUT | string(1,0) |
| GVT_RETRO_JOB_FLAG | INPUT/OUTPUT | string(1,0) |
| GVT_RETRO_BSE_FLAG | INPUT/OUTPUT | string(1,0) |
| GVT_OTH_PAY_CHG | INPUT/OUTPUT | string(1,0) |
| GVT_DETL_POSN_NBR | INPUT/OUTPUT | string(8,0) |
| ANNL_BEN_BASE_OVRD | INPUT/OUTPUT | string(1,0) |
| BENEFIT_PROGRAM | INPUT/OUTPUT | string(3,0) |
| UPDATE_PAYROLL | INPUT/OUTPUT | string(1,0) |
| GVT_PAY_PLAN | INPUT/OUTPUT | string(2,0) |
| GVT_PAY_FLAG | INPUT/OUTPUT | string(1,0) |
| GVT_NID_CHANGE | INPUT/OUTPUT | string(1,0) |
| UNION_FULL_PART | INPUT/OUTPUT | string(1,0) |
| UNION_POS | INPUT/OUTPUT | string(1,0) |
| MATRICULA_NBR | INPUT/OUTPUT | decimal(38,0) |
| SOC_SEC_RISK_CODE | INPUT/OUTPUT | string(3,0) |
| UNION_FEE_AMOUNT | INPUT/OUTPUT | decimal(8,2) |
| UNION_FEE_START_DT | INPUT/OUTPUT | date/time(29,9) |
| UNION_FEE_END_DT | INPUT/OUTPUT | date/time(29,9) |
| EXEMPT_JOB_LBR | INPUT/OUTPUT | string(1,0) |
| EXEMPT_HOURS_MONTH | INPUT/OUTPUT | decimal(38,0) |
| WRKS_CNCL_FUNCTION | INPUT/OUTPUT | string(1,0) |
| INTERCTR_WRKS_CNCL | INPUT/OUTPUT | string(1,0) |
| CURRENCY_CD1 | INPUT/OUTPUT | string(3,0) |
| PAY_UNION_FEE | INPUT/OUTPUT | string(1,0) |
| UNION_CD | INPUT/OUTPUT | string(3,0) |
| BARG_UNIT | INPUT/OUTPUT | string(4,0) |
| UNION_SENIORITY_DT | INPUT/OUTPUT | date/time(29,9) |
| ENTRY_DATE | INPUT/OUTPUT | date/time(29,9) |
| LABOR_AGREEMENT | INPUT/OUTPUT | string(6,0) |
| EMPL_CTG | INPUT/OUTPUT | string(6,0) |
| EMPL_CTG_L1 | INPUT/OUTPUT | string(6,0) |
| EMPL_CTG_L2 | INPUT/OUTPUT | string(6,0) |
| SETID_LBR_AGRMNT | INPUT/OUTPUT | string(5,0) |
| WPP_STOP_FLAG | INPUT/OUTPUT | string(1,0) |
| LABOR_FACILITY_ID | INPUT/OUTPUT | string(10,0) |
| LBR_FAC_ENTRY_DT | INPUT/OUTPUT | date/time(29,9) |
| LAYOFF_EXEMPT_FLAG | INPUT/OUTPUT | string(1,0) |
| LAYOFF_EXEMPT_RSN | INPUT/OUTPUT | string(11,0) |
| GP_PAYGROUP | INPUT/OUTPUT | string(10,0) |
| GP_DFLT_ELIG_GRP | INPUT/OUTPUT | string(1,0) |
| GP_ELIG_GRP | INPUT/OUTPUT | string(10,0) |
| GP_DFLT_CURRTTYP | INPUT/OUTPUT | string(1,0) |
| CUR_RT_TYPE | INPUT/OUTPUT | string(5,0) |
| GP_DFLT_EXRTDT | INPUT/OUTPUT | string(1,0) |
| GP_ASOF_DT_EXG_RT | INPUT/OUTPUT | string(1,0) |
| ADDS_TO_FTE_ACTUAL | INPUT/OUTPUT | string(1,0) |
| CLASS_INDC | INPUT/OUTPUT | string(1,0) |
| ENCUMB_OVERRIDE | INPUT/OUTPUT | string(1,0) |
| FICA_STATUS_EE | INPUT/OUTPUT | string(1,0) |
| FTE | INPUT/OUTPUT | decimal(7,6) |
| PRORATE_CNT_AMT | INPUT/OUTPUT | string(1,0) |
| PAY_SYSTEM_FLG | INPUT/OUTPUT | string(2,0) |
| BORDER_WALKER | INPUT/OUTPUT | string(1,0) |
| LUMP_SUM_PAY | INPUT/OUTPUT | string(1,0) |
| CONTRACT_NUM | INPUT/OUTPUT | string(25,0) |
| JOB_INDICATOR | INPUT/OUTPUT | string(1,0) |
| WRKS_CNCL_ROLE_CHE | INPUT/OUTPUT | string(30,0) |
| BENEFIT_SYSTEM | INPUT/OUTPUT | string(2,0) |
| WORK_DAY_HOURS | INPUT/OUTPUT | decimal(6,2) |
| SUPERVISOR_ID | INPUT/OUTPUT | string(11,0) |
| REPORTS_TO | INPUT/OUTPUT | string(8,0) |
| ESTABID | INPUT/OUTPUT | string(12,0) |
| HE_NOA_EXT | INPUT/OUTPUT | string(1,0) |
| HE_AL_CARRYOVER | INPUT/OUTPUT | decimal(6,2) |
| HE_AL_ACCRUAL | INPUT/OUTPUT | decimal(5,2) |
| HE_AL_RED_CRED | INPUT/OUTPUT | decimal(5,2) |
| HE_AL_TOTAL | INPUT/OUTPUT | decimal(5,2) |
| HE_AL_BALANCE | INPUT/OUTPUT | decimal(6,2) |
| HE_SL_CARRYOVER | INPUT/OUTPUT | decimal(6,2) |
| HE_SL_ACCRUAL | INPUT/OUTPUT | decimal(5,2) |
| HE_SL_RED_CRED | INPUT/OUTPUT | decimal(5,2) |
| HE_SL_TOTAL | INPUT/OUTPUT | decimal(6,2) |
| HE_SL_BALANCE | INPUT/OUTPUT | decimal(6,2) |
| HE_RES_LASTYR | INPUT/OUTPUT | decimal(6,2) |
| HE_RES_TWOYRS | INPUT/OUTPUT | decimal(6,2) |
| HE_RES_THREEYRS | INPUT/OUTPUT | decimal(6,2) |
| HE_RES_BALANCE | INPUT/OUTPUT | decimal(6,2) |
| HE_LUMP_HRS | INPUT/OUTPUT | decimal(38,0) |
| HE_AWOP_SEP | INPUT/OUTPUT | decimal(6,2) |
| HE_AWOP_WIGI | INPUT/OUTPUT | decimal(6,2) |
| HE_REG_MILITARY | INPUT/OUTPUT | decimal(38,0) |
| HE_SPC_MILITARY | INPUT/OUTPUT | decimal(38,0) |
| HE_FROZEN_SL | INPUT/OUTPUT | decimal(6,2) |
| HE_TSPA_PR_YR | INPUT/OUTPUT | decimal(6,2) |
| HE_TSPA_SUB_YR | INPUT/OUTPUT | decimal(7,2) |
| HE_UNOFF_AL | INPUT/OUTPUT | decimal(38,0) |
| HE_UNOFF_SL | INPUT/OUTPUT | decimal(38,0) |
| HE_TLTR_NO | INPUT/OUTPUT | decimal(38,0) |
| HE_UDED_PAY_CD | INPUT/OUTPUT | string(1,0) |
| HE_TSP_CANC_CD | INPUT/OUTPUT | string(1,0) |
| HE_PP_UDED_AMT | INPUT/OUTPUT | decimal(9,2) |
| HE_EMP_UDED_AMT | INPUT/OUTPUT | decimal(9,2) |
| HE_GVT_UDED_AMT | INPUT/OUTPUT | decimal(9,2) |
| HE_NO_TSP_PAYPER | INPUT/OUTPUT | decimal(38,0) |
| EMPLID1 | INPUT/OUTPUT | string(11,0) |
| EMPL_RCD1 | INPUT/OUTPUT | decimal(38,0) |
| EFFDT1 | INPUT/OUTPUT | date/time(29,9) |
| EFFSEQ1 | INPUT/OUTPUT | decimal(38,0) |

</details>

<details><summary><code>lkp_PS_GVT_EMPLOYMENT</code> (Lookup Procedure, 85 ports)</summary>

| Port | Port type | Datatype |
|---|---|---|
| EMPLID_IN | INPUT | string(11,0) |
| EMPL_RCD_IN | INPUT | decimal(38,0) |
| EFFDT_IN | INPUT | date/time(29,9) |
| EFFSEQ_IN | INPUT | decimal(38,0) |
| EMPLID | LOOKUP/OUTPUT | string(11,0) |
| EMPL_RCD | LOOKUP/OUTPUT | decimal(38,0) |
| EFFDT | LOOKUP/OUTPUT | date/time(29,9) |
| EFFSEQ | LOOKUP/OUTPUT | decimal(38,0) |
| BENEFIT_RCD_NBR | LOOKUP/OUTPUT | decimal(38,0) |
| HOME_HOST_CLASS | LOOKUP/OUTPUT | string(1,0) |
| HIRE_DT | LOOKUP/OUTPUT | date/time(29,9) |
| REHIRE_DT | LOOKUP/OUTPUT | date/time(29,9) |
| CMPNY_SENIORITY_DT | LOOKUP/OUTPUT | date/time(29,9) |
| SERVICE_DT | LOOKUP/OUTPUT | date/time(29,9) |
| PROF_EXPERIENCE_DT | LOOKUP/OUTPUT | date/time(29,9) |
| LAST_VERIFICATN_DT | LOOKUP/OUTPUT | date/time(29,9) |
| EXPECTED_RETURN_DT | LOOKUP/OUTPUT | date/time(29,9) |
| TERMINATION_DT | LOOKUP/OUTPUT | date/time(29,9) |
| LAST_DATE_WORKED | LOOKUP/OUTPUT | date/time(29,9) |
| LAST_INCREASE_DT | LOOKUP/OUTPUT | date/time(29,9) |
| OWN_5PERCENT_CO | LOOKUP/OUTPUT | string(1,0) |
| BUSINESS_TITLE | LOOKUP/OUTPUT | string(30,0) |
| REPORTS_TO | LOOKUP/OUTPUT | string(8,0) |
| PROBATION_DT | LOOKUP/OUTPUT | date/time(29,9) |
| SECURITY_CLEARANCE | LOOKUP/OUTPUT | string(1,0) |
| DED_TAKEN | LOOKUP/OUTPUT | string(1,0) |
| DED_SUBSET_ID | LOOKUP/OUTPUT | string(3,0) |
| SETID | LOOKUP/OUTPUT | string(5,0) |
| GVT_SCD_RETIRE | LOOKUP/OUTPUT | date/time(29,9) |
| GVT_SCD_TSP | LOOKUP/OUTPUT | date/time(29,9) |
| GVT_SCD_LEO | LOOKUP/OUTPUT | date/time(29,9) |
| GVT_SCD_SEVPAY | LOOKUP/OUTPUT | date/time(29,9) |
| GVT_SEVPAY_PRV_WKS | LOOKUP/OUTPUT | decimal(38,0) |
| GVT_MAND_RET_DT | LOOKUP/OUTPUT | date/time(29,9) |
| GVT_WGI_STATUS | LOOKUP/OUTPUT | string(1,0) |
| GVT_INTRM_DAYS_WGI | LOOKUP/OUTPUT | decimal(38,0) |
| GVT_NONPAY_NOA | LOOKUP/OUTPUT | string(3,0) |
| GVT_NONPAY_HRS_WGI | LOOKUP/OUTPUT | decimal(6,2) |
| GVT_NONPAY_HRS_SCD | LOOKUP/OUTPUT | decimal(6,2) |
| GVT_NONPAY_HRS_TNR | LOOKUP/OUTPUT | decimal(6,2) |
| GVT_NONPAY_HRS_PRB | LOOKUP/OUTPUT | decimal(6,2) |
| GVT_TEMP_PRO_EXPIR | LOOKUP/OUTPUT | date/time(29,9) |
| GVT_TEMP_PSN_EXPIR | LOOKUP/OUTPUT | date/time(29,9) |
| GVT_DETAIL_EXPIRES | LOOKUP/OUTPUT | date/time(29,9) |
| GVT_SABBATIC_EXPIR | LOOKUP/OUTPUT | date/time(29,9) |
| GVT_RTND_GRADE_BEG | LOOKUP/OUTPUT | date/time(29,9) |
| GVT_RTND_GRADE_EXP | LOOKUP/OUTPUT | date/time(29,9) |
| GVT_NOA_CODE | LOOKUP/OUTPUT | string(3,0) |
| GVT_CURR_APT_AUTH1 | LOOKUP/OUTPUT | string(3,0) |
| GVT_CURR_APT_AUTH2 | LOOKUP/OUTPUT | string(3,0) |
| GVT_APPT_EXPIR_DT | LOOKUP/OUTPUT | date/time(29,9) |
| GVT_CNV_BEGIN_DATE | LOOKUP/OUTPUT | date/time(29,9) |
| GVT_CAREER_CNV_DUE | LOOKUP/OUTPUT | date/time(29,9) |
| GVT_CAREER_COND_DT | LOOKUP/OUTPUT | date/time(29,9) |
| GVT_APPT_LIMIT_HRS | LOOKUP/OUTPUT | decimal(38,0) |
| GVT_APPT_LIMIT_DYS | LOOKUP/OUTPUT | decimal(38,0) |
| GVT_APPT_LIMIT_AMT | LOOKUP/OUTPUT | decimal(38,0) |
| GVT_SUPV_PROB_DT | LOOKUP/OUTPUT | date/time(29,9) |
| GVT_SES_PROB_DT | LOOKUP/OUTPUT | date/time(29,9) |
| GVT_SEC_CLR_STATUS | LOOKUP/OUTPUT | string(1,0) |
| GVT_CLRNCE_STAT_DT | LOOKUP/OUTPUT | date/time(29,9) |
| GVT_ERN_PGM_PERM | LOOKUP/OUTPUT | string(2,0) |
| GVT_OCC_SERS_PERM | LOOKUP/OUTPUT | string(4,0) |
| GVT_GRADE_PERM | LOOKUP/OUTPUT | string(3,0) |
| GVT_COMP_AREA_PERM | LOOKUP/OUTPUT | string(2,0) |
| GVT_COMP_LVL_PERM | LOOKUP/OUTPUT | string(4,0) |
| GVT_CHANGE_FLAG | LOOKUP/OUTPUT | string(1,0) |
| GVT_SPEP | LOOKUP/OUTPUT | string(2,0) |
| GVT_WGI_DUE_DATE | LOOKUP/OUTPUT | date/time(29,9) |
| GVT_DT_LEI | LOOKUP/OUTPUT | date/time(29,9) |
| GVT_FIN_DISCLOSURE | LOOKUP/OUTPUT | string(1,0) |
| GVT_FIN_DISCL_DATE | LOOKUP/OUTPUT | date/time(29,9) |
| GVT_TENURE | LOOKUP/OUTPUT | string(1,0) |
| GVT_DETL_BARG_UNIT | LOOKUP/OUTPUT | string(4,0) |
| GVT_DETL_UNION_CD | LOOKUP/OUTPUT | string(3,0) |
| NEXT_REVIEW_DT | LOOKUP/OUTPUT | date/time(29,9) |
| GVT_WELFARE_WK_CD | LOOKUP/OUTPUT | string(2,0) |
| TENURE_ACCR_FLG | LOOKUP/OUTPUT | string(1,0) |
| FTE_TENURE | LOOKUP/OUTPUT | decimal(3,2) |
| EG_GROUP | LOOKUP/OUTPUT | string(6,0) |
| FTE_FLX_SRVC | LOOKUP/OUTPUT | decimal(3,2) |
| CONTRACT_LENGTH | LOOKUP/OUTPUT | string(1,0) |
| APPOINT_END_DT | LOOKUP/OUTPUT | date/time(29,9) |
| NEE_PROVIDER_ID | LOOKUP/OUTPUT | string(10,0) |
| POSITION_PHONE | LOOKUP/OUTPUT | string(24,0) |

</details>

<details><summary><code>lkp_PS_GVT_PERS_NID</code> (Lookup Procedure, 13 ports)</summary>

| Port | Port type | Datatype |
|---|---|---|
| EMPLID_IN | INPUT | string(11,0) |
| EMPL_RCD_IN | INPUT | decimal(38,0) |
| EFFDT_IN | INPUT | date/time(29,9) |
| EFFSEQ_IN | INPUT | decimal(38,0) |
| EMPLID | LOOKUP/OUTPUT | string(11,0) |
| EMPL_RCD | LOOKUP/OUTPUT | decimal(38,0) |
| EFFDT | LOOKUP/OUTPUT | date/time(29,9) |
| EFFSEQ | LOOKUP/OUTPUT | decimal(38,0) |
| COUNTRY | LOOKUP/OUTPUT | string(3,0) |
| NATIONAL_ID_TYPE | LOOKUP/OUTPUT | string(6,0) |
| NATIONAL_ID | LOOKUP/OUTPUT | string(20,0) |
| SSN_KEY_FRA | LOOKUP/OUTPUT | string(2,0) |
| PRIMARY_NID | LOOKUP/OUTPUT | string(1,0) |

</details>

<details><summary><code>lkp_PS_GVT_AWD_DATA</code> (Lookup Procedure, 30 ports)</summary>

| Port | Port type | Datatype |
|---|---|---|
| EMPLID_IN | INPUT | string(11,0) |
| EMPL_RCD_IN | INPUT | decimal(38,0) |
| EFFDT_IN | INPUT | date/time(29,9) |
| EFFSEQ_IN | INPUT | decimal(38,0) |
| EMPLID | LOOKUP/OUTPUT | string(11,0) |
| EMPL_RCD | LOOKUP/OUTPUT | decimal(38,0) |
| EFFDT | LOOKUP/OUTPUT | date/time(29,9) |
| EFFSEQ | LOOKUP/OUTPUT | decimal(38,0) |
| GVT_AWD_CLASS | LOOKUP/OUTPUT | string(1,0) |
| OTH_PAY | LOOKUP/OUTPUT | decimal(10,2) |
| OTH_HRS | LOOKUP/OUTPUT | decimal(6,2) |
| GOAL_AMT | LOOKUP/OUTPUT | decimal(10,2) |
| EARNINGS_END_DT | LOOKUP/OUTPUT | date/time(29,9) |
| GVT_USE_BY_DATE | LOOKUP/OUTPUT | date/time(29,9) |
| GVT_AWARD_GROUP | LOOKUP/OUTPUT | string(4,0) |
| GVT_TANG_BEN_AMT | LOOKUP/OUTPUT | decimal(38,0) |
| GVT_INTANG_BEN_AMT | LOOKUP/OUTPUT | decimal(38,0) |
| GVT_SUGGESTION_NBR | LOOKUP/OUTPUT | string(10,0) |
| GVT_OBLIG_EXPIR_DT | LOOKUP/OUTPUT | date/time(29,9) |
| GL_PAY_TYPE | LOOKUP/OUTPUT | string(6,0) |
| ACCT_CD | LOOKUP/OUTPUT | string(25,0) |
| ERNCD | LOOKUP/OUTPUT | string(3,0) |
| GVT_SEPCHK_IND | LOOKUP/OUTPUT | string(1,0) |
| SEPCHK | LOOKUP/OUTPUT | decimal(38,0) |
| DED_TAKEN | LOOKUP/OUTPUT | string(1,0) |
| DED_TAKEN_GENL | LOOKUP/OUTPUT | string(1,0) |
| DISABLE_DIR_DEP | LOOKUP/OUTPUT | string(1,0) |
| GROSSUP | LOOKUP/OUTPUT | string(1,0) |
| ADDL_SEQ | LOOKUP/OUTPUT | decimal(38,0) |
| COMMENTS | LOOKUP/OUTPUT | text(4000,0) |

</details>

<details><summary><code>lkp_PS_GVT_EE_DATA_TRK</code> (Lookup Procedure, 18 ports)</summary>

| Port | Port type | Datatype |
|---|---|---|
| EMPLID_IN | INPUT | string(11,0) |
| EMPL_RCD_IN | INPUT | decimal(38,0) |
| EFFDT_IN | INPUT | date/time(29,9) |
| EFFSEQ_IN | INPUT | decimal(38,0) |
| GVT_WIP_STATUS_IN | INPUT | string(3,0) |
| EMPLID | LOOKUP | string(11,0) |
| EMPL_RCD | LOOKUP | decimal(38,0) |
| EFFDT | LOOKUP | date/time(29,9) |
| EFFSEQ | LOOKUP | decimal(38,0) |
| GVT_TRK_SEQUENCE | LOOKUP | decimal(38,0) |
| GVT_DATE_WRK | LOOKUP/OUTPUT | date/time(29,9) |
| GVT_WIP_STATUS | LOOKUP | string(3,0) |
| OPRID | LOOKUP | string(30,0) |
| GVT_OPRID_OVERRIDE | LOOKUP | string(1,0) |
| GVT_TRK_EMPLID | LOOKUP | string(11,0) |
| GVT_TRK_EMPL_RCD | LOOKUP | decimal(38,0) |
| GVT_EE_TRK_DT | LOOKUP | date/time(29,9) |
| COMMENTS | LOOKUP | text(4000,0) |

</details>

<details><summary><code>lkp_PS_HE_FILL_POS</code> (Lookup Procedure, 9 ports)</summary>

| Port | Port type | Datatype |
|---|---|---|
| EMPLID_IN | INPUT | string(11,0) |
| EMPL_RCD_IN | INPUT | decimal(38,0) |
| EFFDT_IN | INPUT | date/time(29,9) |
| EFFSEQ_IN | INPUT | decimal(38,0) |
| EMPLID | LOOKUP | string(11,0) |
| EMPL_RCD | LOOKUP | decimal(38,0) |
| EFFDT | LOOKUP | date/time(29,9) |
| EFFSEQ | LOOKUP | decimal(38,0) |
| HE_FILL_POSITION | LOOKUP/OUTPUT | string(1,0) |

</details>

<details><summary><code>lkp_PS_GVT_CITIZENSHIP</code> (Lookup Procedure, 11 ports)</summary>

| Port | Port type | Datatype |
|---|---|---|
| EMPLID_IN | INPUT | string(11,0) |
| EMPL_RCD_IN | INPUT | decimal(38,0) |
| EFFDT_IN | INPUT | date/time(29,9) |
| EFFSEQ_IN | INPUT | decimal(38,0) |
| EMPLID | LOOKUP | string(11,0) |
| EMPL_RCD | LOOKUP | decimal(38,0) |
| EFFDT | LOOKUP | date/time(29,9) |
| EFFSEQ | LOOKUP | decimal(38,0) |
| DEPENDENT_ID | LOOKUP | string(2,0) |
| COUNTRY | LOOKUP | string(3,0) |
| CITIZENSHIP_STATUS | LOOKUP/OUTPUT | string(1,0) |

</details>

<details><summary><code>lkp_PS_GVT_PERS_DATA</code> (Lookup Procedure, 125 ports)</summary>

| Port | Port type | Datatype |
|---|---|---|
| EMPLID_IN | INPUT | string(11,0) |
| EMPL_RCD_IN | INPUT | decimal(38,0) |
| EFFDT_IN | INPUT | date/time(29,9) |
| EFFSEQ_IN | INPUT | decimal(38,0) |
| EMPLID | LOOKUP | string(11,0) |
| EMPL_RCD | LOOKUP | decimal(38,0) |
| EFFDT | LOOKUP | date/time(29,9) |
| EFFSEQ | LOOKUP | decimal(38,0) |
| COUNTRY_NM_FORMAT | LOOKUP | string(3,0) |
| NAME | LOOKUP | string(50,0) |
| NAME_INITIALS | LOOKUP | string(6,0) |
| NAME_PREFIX | LOOKUP | string(4,0) |
| NAME_SUFFIX | LOOKUP | string(15,0) |
| NAME_ROYAL_PREFIX | LOOKUP | string(15,0) |
| NAME_ROYAL_SUFFIX | LOOKUP | string(15,0) |
| NAME_TITLE | LOOKUP | string(30,0) |
| LAST_NAME_SRCH | LOOKUP | string(30,0) |
| FIRST_NAME_SRCH | LOOKUP | string(30,0) |
| LAST_NAME | LOOKUP/OUTPUT | string(30,0) |
| FIRST_NAME | LOOKUP/OUTPUT | string(30,0) |
| MIDDLE_NAME | LOOKUP/OUTPUT | string(30,0) |
| SECOND_LAST_NAME | LOOKUP | string(30,0) |
| SECOND_LAST_SRCH | LOOKUP | string(30,0) |
| NAME_AC | LOOKUP | string(50,0) |
| PREF_FIRST_NAME | LOOKUP | string(30,0) |
| PARTNER_LAST_NAME | LOOKUP | string(30,0) |
| PARTNER_ROY_PREFIX | LOOKUP | string(15,0) |
| LAST_NAME_PREF_NLD | LOOKUP | string(1,0) |
| NAME_DISPLAY | LOOKUP | string(50,0) |
| NAME_FORMAL | LOOKUP | string(60,0) |
| COUNTRY | LOOKUP | string(3,0) |
| ADDRESS1 | LOOKUP/OUTPUT | string(55,0) |
| ADDRESS2 | LOOKUP | string(55,0) |
| ADDRESS3 | LOOKUP | string(55,0) |
| ADDRESS4 | LOOKUP | string(55,0) |
| CITY | LOOKUP/OUTPUT | string(30,0) |
| NUM1 | LOOKUP | string(6,0) |
| NUM2 | LOOKUP | string(6,0) |
| HOUSE_TYPE | LOOKUP | string(2,0) |
| ADDR_FIELD1 | LOOKUP | string(2,0) |
| ADDR_FIELD2 | LOOKUP | string(4,0) |
| ADDR_FIELD3 | LOOKUP | string(4,0) |
| COUNTY | LOOKUP | string(30,0) |
| STATE | LOOKUP/OUTPUT | string(6,0) |
| POSTAL | LOOKUP/OUTPUT | string(12,0) |
| GEO_CODE | LOOKUP/OUTPUT | string(11,0) |
| IN_CITY_LIMIT | LOOKUP | string(1,0) |
| COUNTRY_OTHER | LOOKUP | string(3,0) |
| ADDRESS1_OTHER | LOOKUP | string(55,0) |
| ADDRESS2_OTHER | LOOKUP | string(55,0) |
| ADDRESS3_OTHER | LOOKUP | string(55,0) |
| ADDRESS4_OTHER | LOOKUP | string(55,0) |
| CITY_OTHER | LOOKUP | string(30,0) |
| COUNTY_OTHER | LOOKUP | string(30,0) |
| STATE_OTHER | LOOKUP | string(6,0) |
| POSTAL_OTHER | LOOKUP | string(12,0) |
| NUM1_OTHER | LOOKUP | string(6,0) |
| NUM2_OTHER | LOOKUP | string(6,0) |
| HOUSE_TYPE_OTHER | LOOKUP | string(2,0) |
| ADDR_FIELD1_OTHER | LOOKUP | string(2,0) |
| ADDR_FIELD2_OTHER | LOOKUP | string(4,0) |
| ADDR_FIELD3_OTHER | LOOKUP | string(4,0) |
| IN_CITY_LMT_OTHER | LOOKUP | string(1,0) |
| GEO_CODE_OTHER | LOOKUP | string(11,0) |
| COUNTRY_CODE | LOOKUP | string(3,0) |
| PHONE | LOOKUP | string(24,0) |
| PER_ORG | LOOKUP | string(3,0) |
| ORIG_HIRE_DT | LOOKUP | date/time(29,9) |
| SEX | LOOKUP/OUTPUT | string(1,0) |
| MAR_STATUS | LOOKUP | string(1,0) |
| MAR_STATUS_DT | LOOKUP | date/time(29,9) |
| BIRTHDATE | LOOKUP/OUTPUT | date/time(29,9) |
| BIRTHPLACE | LOOKUP | string(30,0) |
| BIRTHCOUNTRY | LOOKUP | string(3,0) |
| BIRTHSTATE | LOOKUP | string(6,0) |
| DT_OF_DEATH | LOOKUP | date/time(29,9) |
| HIGHEST_EDUC_LVL | LOOKUP | string(2,0) |
| FT_STUDENT | LOOKUP | string(1,0) |
| US_WORK_ELIGIBILTY | LOOKUP | string(1,0) |
| MILITARY_STATUS | LOOKUP/OUTPUT | string(1,0) |
| CITIZEN_PROOF1 | LOOKUP | string(10,0) |
| CITIZEN_PROOF2 | LOOKUP | string(10,0) |
| SMOKER | LOOKUP | string(1,0) |
| MEDICARE_ENTLD_DT | LOOKUP | date/time(29,9) |
| SMOKER_DT | LOOKUP | date/time(29,9) |
| ADDRESS1_AC | LOOKUP | string(55,0) |
| ADDRESS2_AC | LOOKUP | string(55,0) |
| ADDRESS3_AC | LOOKUP | string(55,0) |
| CITY_AC | LOOKUP | string(30,0) |
| LANG_CD | LOOKUP | string(3,0) |
| YEARS_OF_EXP | LOOKUP | decimal(4,1) |
| GVT_CRED_MIL_SVCE | LOOKUP/OUTPUT | string(6,0) |
| GVT_MILITARY_COMP | LOOKUP/OUTPUT | string(1,0) |
| GVT_MIL_GRADE | LOOKUP | string(3,0) |
| GVT_MIL_RESRVE_CAT | LOOKUP/OUTPUT | string(1,0) |
| GVT_MIL_SEP_RET | LOOKUP | string(1,0) |
| GVT_MIL_SVCE_END | LOOKUP | date/time(29,9) |
| GVT_MIL_SVCE_START | LOOKUP | date/time(29,9) |
| GVT_MIL_VERIFY | LOOKUP | string(1,0) |
| GVT_PAR_NBR_LAST | LOOKUP | decimal(38,0) |
| GVT_UNIF_SVC_CTR | LOOKUP | string(1,0) |
| GVT_VET_PREF_APPT | LOOKUP/OUTPUT | string(1,0) |
| GVT_VET_PREF_RIF | LOOKUP | string(1,0) |
| GVT_CHANGE_FLAG | LOOKUP | string(1,0) |
| GVT_DRAFT_STATUS | LOOKUP | string(1,0) |
| GVT_YR_ATTAINED | LOOKUP | date/time(29,9) |
| DISABLED_VET | LOOKUP | string(1,0) |
| DISABLED | LOOKUP | string(1,0) |
| ETHNIC_GROUP | LOOKUP/OUTPUT | string(1,0) |
| GVT_DISABILITY_CD | LOOKUP/OUTPUT | string(2,0) |
| GRADE | LOOKUP | string(3,0) |
| SAL_ADMIN_PLAN | LOOKUP | string(4,0) |
| GVT_CURR_AGCY_EMPL | LOOKUP | string(1,0) |
| GVT_CURR_FED_EMPL | LOOKUP | string(1,0) |
| GVT_HIGH_PAY_PLAN | LOOKUP | string(2,0) |
| GVT_HIGH_GRADE | LOOKUP | string(3,0) |
| GVT_PREV_AGCY_EMPL | LOOKUP | string(1,0) |
| GVT_PREV_FED_EMPL | LOOKUP | string(1,0) |
| GVT_SEP_INCENTIVE | LOOKUP | string(1,0) |
| GVT_SEP_INCENT_DT | LOOKUP | date/time(29,9) |
| GVT_TENURE | LOOKUP | string(1,0) |
| GVT_PAY_PLAN | LOOKUP | string(2,0) |
| BARG_UNIT | LOOKUP | string(4,0) |
| ALTER_EMPLID | LOOKUP | string(11,0) |
| HE_ERI | LOOKUP | string(6,0) |

</details>

<details><summary><code>lkp_PS_JPM_JP_ITEMS</code> (Lookup Procedure, 124 ports)</summary>

| Port | Port type | Datatype |
|---|---|---|
| EMPLID_IN | INPUT | string(10,0) |
| JPM_PROFILE_ID | LOOKUP/OUTPUT | string(12,0) |
| JPM_CAT_TYPE | LOOKUP/OUTPUT | string(12,0) |
| JPM_CAT_ITEM_ID | LOOKUP/OUTPUT | string(12,0) |
| JPM_CAT_ITEM_QUAL | LOOKUP | string(12,0) |
| JPM_CAT_ITEM_QUAL2 | LOOKUP/OUTPUT | string(12,0) |
| EFFDT | LOOKUP/OUTPUT | date/time(29,9) |
| EFF_STATUS | LOOKUP/OUTPUT | string(1,0) |
| JPM_ITEM_KEY_ID | LOOKUP | decimal(12,0) |
| JPM_JP_ITEM_SRC | LOOKUP | string(4,0) |
| JPM_SOURCE_ID1 | LOOKUP | string(12,0) |
| JPM_SOURCE_ID2 | LOOKUP | string(12,0) |
| JPM_SOURCE_ID3 | LOOKUP | string(12,0) |
| JPM_JP_QUAL_SET | LOOKUP | string(12,0) |
| JPM_JP_QUAL_SET2 | LOOKUP | string(12,0) |
| JPM_PARENT_KEY_ID | LOOKUP | decimal(12,0) |
| JPM_WF_STATUS | LOOKUP | string(2,0) |
| COUNTRY | LOOKUP | string(3,0) |
| JPM_DATE_1 | LOOKUP | date/time(29,9) |
| JPM_DATE_2 | LOOKUP | date/time(29,9) |
| JPM_EVAL_MTHD | LOOKUP | string(4,0) |
| JPM_TEXT1325_1 | LOOKUP | string(1325,0) |
| JPM_TEXT1325_2 | LOOKUP | string(1325,0) |
| JPM_TEXT254_1 | LOOKUP | string(254,0) |
| JPM_TEXT254_2 | LOOKUP | string(254,0) |
| JPM_TEXT254_3 | LOOKUP | string(254,0) |
| JPM_TEXT254_4 | LOOKUP | string(254,0) |
| JPM_YN_1 | LOOKUP | string(1,0) |
| JPM_YN_2 | LOOKUP | string(1,0) |
| JPM_YN_3 | LOOKUP | string(1,0) |
| JPM_YN_4 | LOOKUP | string(1,0) |
| JPM_YN_5 | LOOKUP | string(1,0) |
| RATING_MODEL | LOOKUP | string(4,0) |
| JPM_INTEREST_LEVEL | LOOKUP | string(1,0) |
| JPM_MANDATORY | LOOKUP | string(1,0) |
| JPM_RATING1 | LOOKUP | string(1,0) |
| JPM_RATING2 | LOOKUP | string(1,0) |
| JPM_RATING3 | LOOKUP | string(1,0) |
| JPM_TEXT254_5 | LOOKUP | string(254,0) |
| JPM_DATE_3 | LOOKUP | date/time(29,9) |
| JPM_DATE_4 | LOOKUP | date/time(29,9) |
| JPM_DATE_5 | LOOKUP | date/time(29,9) |
| JPM_DATE_6 | LOOKUP | date/time(29,9) |
| JPM_INTEGER_1 | LOOKUP | decimal(38,0) |
| JPM_INTEGER_2 | LOOKUP/OUTPUT | decimal(38,0) |
| JPM_PCT_1 | LOOKUP | decimal(38,0) |
| JPM_PCT_2 | LOOKUP | decimal(38,0) |
| JPM_DECIMAL_1 | LOOKUP | decimal(9,2) |
| JPM_DECIMAL_2 | LOOKUP | decimal(9,2) |
| JPM_PROMPT_1 | LOOKUP | string(12,0) |
| JPM_PROMPT_2 | LOOKUP | string(12,0) |
| JPM_PROMPT_3 | LOOKUP | string(12,0) |
| JPM_PROMPT_4 | LOOKUP | string(12,0) |
| JPM_PROMPT_5 | LOOKUP | string(12,0) |
| JPM_PROMPT_6 | LOOKUP | string(12,0) |
| JPM_PROMPT_7 | LOOKUP | string(12,0) |
| JPM_PROMPT_8 | LOOKUP | string(12,0) |
| JPM_PROMPT_9 | LOOKUP | string(12,0) |
| JPM_PROMPT_10 | LOOKUP | string(12,0) |
| JPM_PROMPT_11 | LOOKUP | string(12,0) |
| JPM_PROMPT_12 | LOOKUP | string(12,0) |
| JPM_PROMPT_13 | LOOKUP | string(12,0) |
| JPM_PROMPT_14 | LOOKUP | string(12,0) |
| JPM_PROMPT_15 | LOOKUP | string(12,0) |
| JPM_PROMPT_16 | LOOKUP | string(12,0) |
| JPM_PROMPT_17 | LOOKUP | string(12,0) |
| JPM_PROMPT_18 | LOOKUP | string(12,0) |
| JPM_PROMPT_19 | LOOKUP | string(12,0) |
| JPM_PROMPT_20 | LOOKUP | string(12,0) |
| JPM_PERSON_ID_1 | LOOKUP | string(11,0) |
| JPM_IMPORTANCE | LOOKUP | string(1,0) |
| JPM_VERIFY_METHOD | LOOKUP | string(1,0) |
| STATE | LOOKUP | string(6,0) |
| NVQ_STATUS | LOOKUP | string(1,0) |
| SCHOOL_CODE | LOOKUP | string(10,0) |
| SCHOOL_DESCR | LOOKUP | string(100,0) |
| MAJOR_CODE | LOOKUP/OUTPUT | string(10,0) |
| MAJOR_DESCR | LOOKUP/OUTPUT | string(100,0) |
| JPM_MINOR_CD | LOOKUP | string(10,0) |
| MINOR_DESCR | LOOKUP | string(100,0) |
| AVERAGE_GRADE | LOOKUP | string(5,0) |
| PRACTIC_GRADE_GER | LOOKUP | string(4,0) |
| THEORY_GRADE_GER | LOOKUP | string(4,0) |
| IPE_SW | LOOKUP | string(1,0) |
| EVALUATION_ID | LOOKUP | string(2,0) |
| GVT_CREDIT_HOURS | LOOKUP | string(3,0) |
| GVT_CRED_HRS_TYPE | LOOKUP | string(1,0) |
| EDUC_LVL_AUS | LOOKUP | string(1,0) |
| APS_HEDUC_CD_AUS | LOOKUP | string(2,0) |
| FACULTY_CODE | LOOKUP | string(10,0) |
| FACULTY_DESCR | LOOKUP | string(100,0) |
| SUBFACULTY_CODE | LOOKUP | string(10,0) |
| SUBFACULTY_DESCR | LOOKUP | string(100,0) |
| MAJOR_CATEGORY | LOOKUP | string(1,0) |
| FP_SUBJECT_CD | LOOKUP | string(3,0) |
| EP_APPRAISAL_ID | LOOKUP | decimal(38,0) |
| SCHOOL_TYPE | LOOKUP | string(3,0) |
| SETID_DEPT | LOOKUP | string(5,0) |
| DEPTID | LOOKUP | string(10,0) |
| BUSINESS_UNIT | LOOKUP | string(5,0) |
| SETID_LOCATION | LOOKUP | string(5,0) |
| LOCATION | LOOKUP | string(10,0) |
| JPM_ADHOC_DESCR | LOOKUP | string(80,0) |
| FP_SKIL_HIR | LOOKUP | string(1,0) |
| FP_SKIL_PRM | LOOKUP | string(1,0) |
| FP_SKIL_TEN | LOOKUP | string(1,0) |
| FP_DEGR_REQUIRED | LOOKUP | string(1,0) |
| BONUS_AMOUNT_FRA | LOOKUP | decimal(7,2) |
| BONUS_DT_FRA | LOOKUP | date/time(29,9) |
| JPM_OBSTACLE_1 | LOOKUP | string(2,0) |
| JPM_AREA_PREF_1 | LOOKUP | string(2,0) |
| JPM_AREA_PREF_2 | LOOKUP | string(2,0) |
| JPM_AREA_PREF_3 | LOOKUP | string(2,0) |
| JPM_CNTRY_PREF_1 | LOOKUP | string(3,0) |
| JPM_CNTRY_PREF_2 | LOOKUP | string(3,0) |
| JPM_CNTRY_PREF_3 | LOOKUP | string(3,0) |
| JPM_LOC_BUNIT_1 | LOOKUP | string(5,0) |
| JPM_SETID_LOC_1 | LOOKUP | string(5,0) |
| JPM_LOCATION_1 | LOOKUP | string(10,0) |
| JPM_LOC_BUNIT_2 | LOOKUP | string(5,0) |
| JPM_SETID_LOC_2 | LOOKUP | string(5,0) |
| JPM_LOCATION_2 | LOOKUP | string(10,0) |
| LASTUPDDTTM | LOOKUP | date/time(29,9) |
| LASTUPDOPRID | LOOKUP/OUTPUT | string(30,0) |

</details>

