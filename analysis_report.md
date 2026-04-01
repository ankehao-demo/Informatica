# EHRP2BIIS_UPDATE Informatica Pipeline - Comprehensive Migration Analysis Report

## Table of Contents

1. [Pipeline Overview](#1-pipeline-overview)
2. [Sources](#2-sources)
3. [Targets](#3-targets)
4. [Transformations](#4-transformations)
5. [Data Flow DAG](#5-data-flow-dag)
6. [Lookup Configurations](#6-lookup-configurations)
7. [Expression Details (Business Logic)](#7-expression-details-business-logic)
8. [Pre/Post-Load Orchestration](#8-prepost-load-orchestration)
9. [Session Configuration](#9-session-configuration)
10. [PySpark Migration Notes](#10-pyspark-migration-notes)

---

## 1. Pipeline Overview

| Property | Value |
|----------|-------|
| **XML File** | `XML/EHRP2BIIS_UPDATE` (2,827 lines) |
| **Repository** | `Prd_Repo_Srvc` (Oracle, Latin1) |
| **Folder** | `EHRP2BIIS` (Owner: NKNIGHT) |
| **Mapping** | `m_EHRP2BIIS_UPDATE` |
| **Session** | `s_m_EHRP2BIIS_UPDATE` |
| **Workflow** | `wf_EHRP2BIIS_UPDATE` |
| **Server** | `Prd_IS` |
| **Sources** | 2 (Oracle tables) |
| **Targets** | 3 (Oracle tables) |
| **Transformations** | 13 |
| **Connectors** | 589 |
| **Instances** | 18 |

### Purpose

This pipeline extracts employee HR action data from PeopleSoft (EHRP) tables, enriches it with lookups against multiple reference tables, applies business logic transformations, and loads the results into three BIIS (Benefits Information Integration System) target tables. It processes government employee personnel actions including awards, transfers, retirements, and various HR transactions.

---

## 2. Sources

### 2.1 NWK_NEW_EHRP_ACTIONS_TBL

- **Database**: Oracle
- **DBDNAME**: `ORA_BIISPRD_SRC`
- **Owner**: `NKNIGHT`
- **Fields**: 4

| # | Field Name | Data Type | Precision | Scale | Nullable | Key |
|---|-----------|-----------|-----------|-------|----------|-----|
| 1 | EMPLID | varchar2 | 11 | 0 | NOTNULL | NOT A KEY |
| 2 | EMPL_RCD | number(p,s) | 38 | 0 | NOTNULL | NOT A KEY |
| 3 | EFFDT | date | 19 | 0 | NOTNULL | NOT A KEY |
| 4 | EFFSEQ | number(p,s) | 38 | 0 | NOTNULL | NOT A KEY |

> **Purpose**: This is a staging/driver table that identifies which employee records need to be processed in the current run. It acts as a filter join table in the Source Qualifier.

### 2.2 PS_GVT_JOB

- **Database**: Oracle
- **DBDNAME**: `ORA_BIISPRD_SRC`
- **Owner**: `EHRP`
- **Fields**: 246

This is the main PeopleSoft Government Job table containing the full employee action record. Key fields include:

| # | Field Name | Data Type | Precision | Scale | Description |
|---|-----------|-----------|-----------|-------|-------------|
| 1 | EMPLID | varchar2 | 11 | 0 | Employee ID |
| 2 | EMPL_RCD | number(p,s) | 38 | 0 | Employee Record Number |
| 3 | EFFDT | date | 19 | 0 | Effective Date |
| 4 | EFFSEQ | number(p,s) | 38 | 0 | Effective Sequence |
| 5 | DEPTID | varchar2 | 10 | 0 | Department ID |
| 6 | JOBCODE | varchar2 | 6 | 0 | Job Code |
| 7 | POSITION_NBR | varchar2 | 8 | 0 | Position Number |
| 8-10 | POSITION_OVERRIDE, POSN_CHANGE_RECORD, EMPL_STATUS | varchar2 | 1 | 0 | Position flags |
| 11 | ACTION | varchar2 | 3 | 0 | HR Action Code |
| 12 | ACTION_DT | date | 19 | 0 | Action Date |
| 13 | ACTION_REASON | varchar2 | 3 | 0 | Action Reason |
| 14 | LOCATION | varchar2 | 10 | 0 | Duty Station |
| 19 | SHIFT | varchar2 | 1 | 0 | Work Shift |
| 20 | REG_TEMP | varchar2 | 1 | 0 | Regular/Temporary |
| 21 | FULL_PART_TIME | varchar2 | 1 | 0 | Full/Part Time |
| 22 | COMPANY | varchar2 | 3 | 0 | Agency Code |
| 39 | STD_HOURS | number(p,s) | 6 | 2 | Standard Hours |
| 43 | SAL_ADMIN_PLAN | varchar2 | 4 | 0 | Salary Admin Plan |
| 44 | GRADE | varchar2 | 3 | 0 | Grade |
| 52 | COMPRATE | number(p,s) | 18 | 6 | Compensation Rate |
| 55 | ANNUAL_RT | number(p,s) | 18 | 3 | Annual Rate |
| 58 | HOURLY_RT | number(p,s) | 18 | 6 | Hourly Rate |
| 59 | ANNL_BENEF_BASE_RT | number(p,s) | 18 | 3 | Annual Benefit Base Rate |

The table contains 246 fields total including GVT (Government) specific fields for NOA codes, pay plans, retirement, FEGLI, leave balances, TSP, and HHS-specific extension fields (HE_*).

> **Full field list**: All 246 source fields from PS_GVT_JOB flow through the Source Qualifier into the mapping. The Source Qualifier SQL override restricts them to only the records matching NWK_NEW_EHRP_ACTIONS_TBL.

---

## 3. Targets

### 3.1 NWK_ACTION_PRIMARY_TBL

- **Database**: Oracle
- **Owner**: `NKNIGHT`
- **Fields**: 260
- **Purpose**: Primary action record for BIIS. Contains the main personnel action data including employee demographics, position info, pay data, leave balances, and benefits.

Key target fields (260 total):

| # | Field Name | Data Type | Precision | Scale |
|---|-----------|-----------|-----------|-------|
| 1 | EVENT_ID | number(p,s) | 10 | 0 |
| 2 | LOAD_ID | varchar2 | 10 | 0 |
| 3 | LOAD_DATE | date | 19 | 0 |
| 4 | AS_OF_DAY | date | 19 | 0 |
| 5 | EMP_ID | varchar2 | 11 | 0 |
| 6-7 | SSN, PSEUDO_SSN | varchar2 | 11/9 | 0 |
| 8 | AGCY_ASSIGN_CD | varchar2 | 10 | 0 |
| 9 | AGCY_SUBELEMENT_CD | varchar2 | 4 | 0 |
| 10 | NOA_CD | varchar2 | 3 | 0 |
| 11 | NOA_SUFFIX_CD | varchar2 | 1 | 0 |
| 12 | EHRP_TYPE_ACTION | varchar2 | 3 | 0 |
| 13 | EHRP_ACTION_REASON | varchar2 | 3 | 0 |
| 14 | EVENT_EFF_DTE | date | 19 | 0 |
| 15 | EVENT_SUBMITTED_DTE | date | 19 | 0 |
| ... | *(260 fields total)* | | | |

### 3.2 NWK_ACTION_SECONDARY_TBL

- **Database**: Oracle
- **Owner**: `NKNIGHT`
- **Fields**: 209
- **Purpose**: Secondary action record containing supplementary benefits data including TSP, retirement, military service, leave, awards, bonuses, and retained grade info.

Key target fields:

| # | Field Name | Data Type | Precision | Scale |
|---|-----------|-----------|-----------|-------|
| 1 | EVENT_ID | number(p,s) | 10 | 0 |
| 2 | RETMT_SCD_DTE | date | 19 | 0 |
| 3 | TSP_SCD_DTE | date | 19 | 0 |
| 4 | RIF_SCD_DTE | date | 19 | 0 |
| 5 | RETND1_PAY_PLAN_CD | varchar2 | 15 | 0 |
| 6 | RETND1_GRADE_CD | varchar2 | 15 | 0 |
| 7 | RETND1_STEP_CD | varchar2 | 15 | 0 |
| ... | *(209 fields total)* | | | |

### 3.3 EHRP_RECS_TRACKING_TBL

- **Database**: Oracle
- **Owner**: `NKNIGHT`
- **Fields**: 10
- **Purpose**: Tracking/audit table recording each processed record with its BIIS event ID and WIP status.

| # | Field Name | Data Type | Precision | Scale |
|---|-----------|-----------|-----------|-------|
| 1 | BIIS_EVENT_ID | number(p,s) | 10 | 0 |
| 2 | EMPLID | varchar2 | 11 | 0 |
| 3 | EMPL_RCD | number(p,s) | 38 | 0 |
| 4 | EFFDT | date | 19 | 0 |
| 5 | EFFSEQ | number(p,s) | 38 | 0 |
| 6 | NOA_CD | varchar2 | 3 | 0 |
| 7 | NOA_SUFFIX_CD | varchar2 | 1 | 0 |
| 8 | GVT_WIP_STATUS | varchar2 | 3 | 0 |
| 9 | EVENT_SUBMITTED_DT | date | 19 | 0 |
| 10 | LOAD_DATE | date | 19 | 0 |

---

## 4. Transformations

### 4.1 Transformation Inventory

| # | Name | Type | Purpose |
|---|------|------|---------|
| 1 | SQ_PS_GVT_JOB | Source Qualifier | Joins PS_GVT_JOB with NWK_NEW_EHRP_ACTIONS_TBL, provides source data |
| 2 | exp_GET_EFFDT_YEAR | Expression | Extracts year from EFFDT for sequence number lookup |
| 3 | lkp_OLD_SEQUENCE_NUMBER | Lookup Procedure | Looks up last sequence number from SEQUENCE_NUM_TBL |
| 4 | exp_MAIN2BIIS | Expression | **Core business logic** - transforms source fields to target format |
| 5 | exp_PERS_DATA | Expression | Pass-through for personal data from PS_GVT_PERS_DATA lookup |
| 6 | lkp_PS_GVT_EMPLOYMENT | Lookup Procedure | Enriches with employment data (hire date, tenure, WGI, etc.) |
| 7 | lkp_PS_GVT_PERS_NID | Lookup Procedure | Looks up National ID / SSN |
| 8 | lkp_PS_GVT_AWD_DATA | Lookup Procedure | Looks up award/bonus data |
| 9 | lkp_PS_GVT_EE_DATA_TRK | Lookup Procedure | Looks up employee data tracking (GVT_DATE_WRK) |
| 10 | lkp_PS_HE_FILL_POS | Lookup Procedure | Looks up fill position indicator |
| 11 | lkp_PS_GVT_CITIZENSHIP | Lookup Procedure | Looks up citizenship status |
| 12 | lkp_PS_GVT_PERS_DATA | Lookup Procedure | Looks up personal data (name, address, demographics) |
| 13 | lkp_PS_JPM_JP_ITEMS | Lookup Procedure | Looks up education/degree information |

### 4.2 Source Qualifier: SQ_PS_GVT_JOB

**Type**: Source Qualifier

**SQL Query Override**:
```sql
SELECT PS_GVT_JOB.EMPLID, PS_GVT_JOB.EMPL_RCD, PS_GVT_JOB.EFFDT, PS_GVT_JOB.EFFSEQ,
       PS_GVT_JOB.DEPTID, PS_GVT_JOB.JOBCODE, PS_GVT_JOB.POSITION_NBR,
       PS_GVT_JOB.ACTION, PS_GVT_JOB.ACTION_DT, PS_GVT_JOB.ACTION_REASON,
       PS_GVT_JOB.LOCATION, PS_GVT_JOB.POSITION_ENTRY_DT, PS_GVT_JOB.REG_TEMP,
       PS_GVT_JOB.COMPANY, PS_GVT_JOB.PAYGROUP, PS_GVT_JOB.STD_HOURS,
       PS_GVT_JOB.STD_HRS_FREQUENCY, PS_GVT_JOB.SAL_ADMIN_PLAN, PS_GVT_JOB.GRADE,
       PS_GVT_JOB.GRADE_ENTRY_DT, PS_GVT_JOB.STEP_ENTRY_DT, PS_GVT_JOB.ACCT_CD,
       PS_GVT_JOB.COMPRATE, PS_GVT_JOB.HOURLY_RT, PS_GVT_JOB.ANNL_BENEF_BASE_RT,
       PS_GVT_JOB.SETID_DEPT, PS_GVT_JOB.FLSA_STATUS, PS_GVT_JOB.GVT_EFFDT,
       PS_GVT_JOB.GVT_WIP_STATUS, PS_GVT_JOB.GVT_STATUS_TYPE, PS_GVT_JOB.GVT_NOA_CODE,
       PS_GVT_JOB.GVT_LEG_AUTH_1, PS_GVT_JOB.GVT_PAR_AUTH_D1, PS_GVT_JOB.GVT_PAR_AUTH_D1_2,
       PS_GVT_JOB.GVT_LEG_AUTH_2, PS_GVT_JOB.GVT_PAR_AUTH_D2, PS_GVT_JOB.GVT_PAR_AUTH_D2_2,
       PS_GVT_JOB.GVT_PAR_NTE_DATE, PS_GVT_JOB.GVT_WORK_SCHED, PS_GVT_JOB.GVT_SUB_AGENCY,
       PS_GVT_JOB.GVT_PAY_RATE_DETER, PS_GVT_JOB.GVT_STEP, PS_GVT_JOB.GVT_RTND_PAY_PLAN,
       PS_GVT_JOB.GVT_RTND_GRADE, PS_GVT_JOB.GVT_RTND_STEP, PS_GVT_JOB.GVT_PAY_BASIS,
       PS_GVT_JOB.GVT_COMPRATE, PS_GVT_JOB.GVT_LOCALITY_ADJ, PS_GVT_JOB.GVT_HRLY_RT_NO_LOC,
       PS_GVT_JOB.GVT_XFER_FROM_AGCY, PS_GVT_JOB.GVT_XFER_TO_AGCY,
       PS_GVT_JOB.GVT_RETIRE_PLAN, PS_GVT_JOB.GVT_ANN_IND, PS_GVT_JOB.GVT_FEGLI,
       PS_GVT_JOB.GVT_FEGLI_LIVING, PS_GVT_JOB.GVT_LIVING_AMT, PS_GVT_JOB.GVT_ANNUITY_OFFSET,
       PS_GVT_JOB.GVT_CSRS_FROZN_SVC, PS_GVT_JOB.GVT_PREV_RET_COVRG,
       PS_GVT_JOB.GVT_FERS_COVERAGE, PS_GVT_JOB.GVT_TYPE_OF_APPT, PS_GVT_JOB.GVT_POI,
       PS_GVT_JOB.GVT_POSN_OCCUPIED, PS_GVT_JOB.GVT_LEO_POSITION, PS_GVT_JOB.GVT_PAY_PLAN,
       PS_GVT_JOB.UNION_CD, PS_GVT_JOB.BARG_UNIT, PS_GVT_JOB.REPORTS_TO,
       PS_GVT_JOB.HE_NOA_EXT, PS_GVT_JOB.HE_AL_CARRYOVER, PS_GVT_JOB.HE_AL_ACCRUAL,
       PS_GVT_JOB.HE_AL_RED_CRED, PS_GVT_JOB.HE_AL_TOTAL, PS_GVT_JOB.HE_AL_BALANCE,
       PS_GVT_JOB.HE_SL_CARRYOVER, PS_GVT_JOB.HE_SL_ACCRUAL, PS_GVT_JOB.HE_SL_RED_CRED,
       PS_GVT_JOB.HE_SL_TOTAL, PS_GVT_JOB.HE_SL_BALANCE, PS_GVT_JOB.HE_RES_LASTYR,
       PS_GVT_JOB.HE_RES_TWOYRS, PS_GVT_JOB.HE_RES_THREEYRS, PS_GVT_JOB.HE_RES_BALANCE,
       PS_GVT_JOB.HE_LUMP_HRS, PS_GVT_JOB.HE_AWOP_SEP, PS_GVT_JOB.HE_AWOP_WIGI,
       PS_GVT_JOB.HE_REG_MILITARY, PS_GVT_JOB.HE_SPC_MILITARY, PS_GVT_JOB.HE_FROZEN_SL,
       PS_GVT_JOB.HE_TSPA_SUB_YR, PS_GVT_JOB.HE_TLTR_NO, PS_GVT_JOB.HE_UDED_PAY_CD,
       PS_GVT_JOB.HE_TSP_CANC_CD, PS_GVT_JOB.HE_PP_UDED_AMT, PS_GVT_JOB.HE_EMP_UDED_AMT,
       PS_GVT_JOB.HE_GVT_UDED_AMT, PS_GVT_JOB.HE_NO_TSP_PAYPER
FROM PS_GVT_JOB, NWK_NEW_EHRP_ACTIONS_TBL
WHERE NWK_NEW_EHRP_ACTIONS_TBL.EMPLID = PS_GVT_JOB.EMPLID
  AND NWK_NEW_EHRP_ACTIONS_TBL.EMPL_RCD = PS_GVT_JOB.EMPL_RCD
  AND NWK_NEW_EHRP_ACTIONS_TBL.EFFDT = PS_GVT_JOB.EFFDT
  AND NWK_NEW_EHRP_ACTIONS_TBL.EFFSEQ = PS_GVT_JOB.EFFSEQ
ORDER BY PS_GVT_JOB.EFFDT
```

**User Defined Join**:
```sql
NWK_NEW_EHRP_ACTIONS_TBL.EMPLID = PS_GVT_JOB.EMPLID AND
NWK_NEW_EHRP_ACTIONS_TBL.EMPL_RCD = PS_GVT_JOB.EMPL_RCD AND
NWK_NEW_EHRP_ACTIONS_TBL.EFFDT = PS_GVT_JOB.EFFDT AND
NWK_NEW_EHRP_ACTIONS_TBL.EFFSEQ = PS_GVT_JOB.EFFSEQ
```

**Key Properties**:
- Select Distinct: NO
- Is Partitionable: NO
- Output is deterministic: NO
- Output is repeatable: Never

> **PySpark equivalent**: Inner join between `PS_GVT_JOB` and `NWK_NEW_EHRP_ACTIONS_TBL` on (EMPLID, EMPL_RCD, EFFDT, EFFSEQ), ordered by EFFDT.

---

## 5. Data Flow DAG

### 5.1 High-Level Data Flow Diagram

```
                     NWK_NEW_EHRP_ACTIONS_TBL (4 fields)
                              |
                              v
    PS_GVT_JOB (246 fields) --> SQ_PS_GVT_JOB (Source Qualifier - JOIN)
                              |
                    +---------+---------+------------------+
                    |         |         |                  |
                    v         v         v                  v
            exp_GET_EFFDT   exp_MAIN2BIIS    [8 Lookup     SQ direct to
            _YEAR          (97 ports in)     Transformations] EHRP_RECS_
                    |                                      TRACKING_TBL
                    v                                      (7 ports)
            lkp_OLD_SEQUENCE                               
            _NUMBER                                        
                    |                                      
                    v                                      
            exp_MAIN2BIIS <--- lkp_PS_GVT_EMPLOYMENT
            (core logic)  <--- lkp_PS_GVT_AWD_DATA
                          <--- lkp_PS_GVT_EE_DATA_TRK
                          <--- lkp_PS_GVT_CITIZENSHIP ---> exp_PERS_DATA
                          <--- lkp_PS_GVT_PERS_DATA  ---> exp_PERS_DATA
                          <--- lkp_PS_HE_FILL_POS
                          <--- lkp_PS_GVT_PERS_NID
                          <--- lkp_PS_JPM_JP_ITEMS
                          <--- exp_PERS_DATA (CITY, STATE)
                    |
         +----------+-----------+
         |          |           |
         v          v           v
    NWK_ACTION  NWK_ACTION   EHRP_RECS_
    _PRIMARY    _SECONDARY   TRACKING_TBL
    _TBL        _TBL
```

### 5.2 Detailed Edge Summary

#### Source to Source Qualifier
| From | To | Ports |
|------|-----|-------|
| NWK_NEW_EHRP_ACTIONS_TBL | SQ_PS_GVT_JOB | 4 (EMPLID, EMPL_RCD, EFFDT, EFFSEQ) |
| PS_GVT_JOB | SQ_PS_GVT_JOB | 246 (all fields) |

#### Source Qualifier to Downstream
| From | To | Ports | Purpose |
|------|-----|-------|---------|
| SQ_PS_GVT_JOB | exp_GET_EFFDT_YEAR | 1 (EFFDT) | Extract year for sequence lookup |
| SQ_PS_GVT_JOB | exp_MAIN2BIIS | 97 | Main transformation input |
| SQ_PS_GVT_JOB | EHRP_RECS_TRACKING_TBL | 7 | Direct to tracking target |
| SQ_PS_GVT_JOB | lkp_PS_GVT_EMPLOYMENT | 4 (EMPLID, EMPL_RCD, EFFDT, EFFSEQ) | Employment lookup keys |
| SQ_PS_GVT_JOB | lkp_PS_GVT_PERS_NID | 4 (EMPLID, EMPL_RCD, EFFDT, EFFSEQ) | NID lookup keys |
| SQ_PS_GVT_JOB | lkp_PS_GVT_AWD_DATA | 4 (EMPLID, EMPL_RCD, EFFDT, EFFSEQ) | Award data lookup keys |
| SQ_PS_GVT_JOB | lkp_PS_GVT_EE_DATA_TRK | 5 (EMPLID, EMPL_RCD, EFFDT, EFFSEQ, GVT_WIP_STATUS) | EE tracking lookup keys |
| SQ_PS_GVT_JOB | lkp_PS_HE_FILL_POS | 4 (EMPLID, EMPL_RCD, EFFDT, EFFSEQ) | Fill position lookup keys |
| SQ_PS_GVT_JOB | lkp_PS_GVT_CITIZENSHIP | 4 (EMPLID, EMPL_RCD, EFFDT, EFFSEQ) | Citizenship lookup keys |
| SQ_PS_GVT_JOB | lkp_PS_GVT_PERS_DATA | 4 (EMPLID, EMPL_RCD, EFFDT, EFFSEQ) | Personal data lookup keys |
| SQ_PS_GVT_JOB | lkp_PS_JPM_JP_ITEMS | 1 (EMPLID) | Education lookup key |

#### Sequence Number Chain
| From | To | Ports |
|------|-----|-------|
| exp_GET_EFFDT_YEAR | lkp_OLD_SEQUENCE_NUMBER | 1 (o_CURRENT_YEAR) |
| lkp_OLD_SEQUENCE_NUMBER | exp_MAIN2BIIS | 1 (EHRP_SEQ_NUMBER) |

#### Lookups to exp_MAIN2BIIS
| From | To | Key Ports |
|------|-----|-----------|
| lkp_PS_GVT_EMPLOYMENT | exp_MAIN2BIIS | GVT_SCD_RETIRE, GVT_SCD_TSP, GVT_RTND_GRADE_BEG, GVT_RTND_GRADE_EXP, GVT_SABBATIC_EXPIR, GVT_CURR_APT_AUTH1, GVT_CURR_APT_AUTH2, CMPNY_SENIORITY_DT, BUSINESS_TITLE, GVT_APPT_LIMIT_HRS, PROBATION_DT, GVT_SCD_LEO, GVT_WGI_DUE_DATE |
| lkp_PS_GVT_AWD_DATA | exp_MAIN2BIIS | GOAL_AMT, OTH_HRS, EARNINGS_END_DT, ERNCD, GVT_TANG_BEN_AMT, OTH_PAY |
| lkp_PS_GVT_EE_DATA_TRK | exp_MAIN2BIIS | GVT_DATE_WRK |
| lkp_PS_GVT_CITIZENSHIP | exp_MAIN2BIIS | CITIZENSHIP_STATUS (as CITIZENSHIP_STATUS_IN) |
| lkp_PS_JPM_JP_ITEMS | exp_MAIN2BIIS | JPM_INTEGER_2 |
| exp_PERS_DATA | exp_MAIN2BIIS | CITY, STATE |

#### Lookups to exp_PERS_DATA
| From | To | Ports |
|------|-----|-------|
| lkp_PS_GVT_PERS_DATA | exp_PERS_DATA | BIRTHDATE, FIRST_NAME, LAST_NAME, MIDDLE_NAME, STATE, POSTAL, ADDRESS1, GVT_DISABILITY_CD, ETHNIC_GROUP, GVT_VET_PREF_APPT, MILITARY_STATUS, GVT_MIL_RESRVE_CAT, GEO_CODE, SEX, GVT_MILITARY_COMP, GVT_CRED_MIL_SVCE, CITY |

#### To Target: NWK_ACTION_PRIMARY_TBL
| Source Transformation | Key Mapped Fields |
|----------------------|-------------------|
| exp_MAIN2BIIS | EVENT_ID, LOAD_ID, LOAD_DATE, AS_OF_DAY, EMP_ID, AGCY_ASSIGN_CD, AGCY_SUBELEMENT_CD, NOA_CD, NOA_SUFFIX_CD, EVENT_EFF_DTE, EVENT_SUBMITTED_DTE, BASE_HRS, GRADE_CD, STEP_CD, PAY_PLAN_CD, PAY_BASIS_CD, HRLY_RATE_AMT, SCHLD_ANN_SALARY_AMT, LOCALITY_PAY_AMT, LEGAL_AUTH_CD/TXT, LEGAL_AUTH2_CD/TXT, CASH_AWARD_AMT, leave balances (ANN_LV_*, SICK_LV_*), AWOP_*, TSP fields, SEVERANCE fields, CITIZENSHIP, POSITION_TITLE_NAME, all 163+ mapped ports |
| exp_PERS_DATA | EMP_RESID_STREET_NAME, HANDICAP_CD, RACE_NATL_ORIGIN_CD, VETERANS_PREFERENCE_CD, VETERANS_STATUS_CD, SEX_CD, CRDTBL_MIL_SRVC_PERIOD, BIRTH_DTE, EMP_FIRST_NAME, EMP_LAST_NAME, EMP_MID_INIT, EMP_RESID_GEOGPHCL_LOC_CD, EMP_RESID_POSTAL_CD |
| lkp_PS_GVT_EMPLOYMENT | CAREER_START_DTE, POSITION_CHANGE_END_DTE, APPT_LMT_NTE_90DAY_CD, EMP_EOD_DTE, SPECIAL_PROGRAM_CD, SUPERVSRY_MGRL_PROB_START_DTE, APPT_NTE_DTE, AWOP_WGI_START_DTE, COMPETITIVE_LEVEL_CD, SUSPENSION_END_DTE, TEMP_PROMTN_EXP_DTE, TENURE_CD, WGI_STATUS_CD, LV_SCD_DTE |
| lkp_PS_HE_FILL_POS | FILLING_POSITION_CD |
| lkp_PS_GVT_PERS_NID | SSN (from NATIONAL_ID) |
| lkp_PS_JPM_JP_ITEMS | INSTRUCTIONAL_PROGRAM_CD (from MAJOR_CODE), EDUCATION_LEVEL_CD (from JPM_CAT_ITEM_ID) |

#### To Target: NWK_ACTION_SECONDARY_TBL
| Source Transformation | Key Mapped Fields |
|----------------------|-------------------|
| exp_MAIN2BIIS | EVENT_ID, TIME_OFF_GRANTED_HRS, RELOCATION_BONUS_AMT, RECRUITMENT_BONUS_AMT, TIME_OFF_AWARD_AMT, MIL_LV_CUR_FY_HRS, MIL_LV_EMERG_CUR_FY_HRS, PAY_TABLE_NUM, RETND1_* (grade/step/pay plan), TSP_* fields, RECRUITMENT_EXP_DTE, RELOCATION_EXP_DTE, RETMT_SCD_DTE, TSP_SCD_DTE, RIF_SCD_DTE, LWOP_START_DTE, BUYOUT fields, PAYGROUP, LEO_SCD_DT, TSP_VESTING_CD |
| exp_PERS_DATA | MIL_SRVC_BRANCH_CD, MIL_SRVC_BRANCH_COMPONENT_CD (both from GVT_MILITARY_COMP) |

#### To Target: EHRP_RECS_TRACKING_TBL
| Source Transformation | Mapped Fields |
|----------------------|---------------|
| exp_MAIN2BIIS | BIIS_EVENT_ID (from o_EVENT_ID), EVENT_SUBMITTED_DT (from o_EVENT_SUBMITTED_DTE), LOAD_DATE (from o_LOAD_DATE) |
| SQ_PS_GVT_JOB | EMPLID, EMPL_RCD, EFFDT, EFFSEQ, NOA_SUFFIX_CD (from HE_NOA_EXT), NOA_CD (from GVT_NOA_CODE), GVT_WIP_STATUS |

---

## 6. Lookup Configurations

### 6.1 lkp_PS_GVT_EMPLOYMENT

| Property | Value |
|----------|-------|
| **Lookup Table** | `PS_GVT_EMPLOYMENT` |
| **Connection** | `$Source` |
| **SQL Override** | None (standard table lookup) |
| **Lookup Condition** | `EMPLID = EMPLID_IN AND EMPL_RCD = EMPL_RCD_IN AND EFFDT = EFFDT_IN AND EFFSEQ = EFFSEQ_IN` |
| **Caching** | YES |
| **Multiple Match** | Use Any Value |
| **Dynamic Cache** | NO |

**Return Ports** (key fields used downstream):
- `GVT_SCD_RETIRE` (date) - Retirement SCD
- `GVT_SCD_TSP` (date) - TSP SCD
- `GVT_SCD_LEO` (date) - LEO SCD
- `GVT_RTND_GRADE_BEG` (date) - Retained Grade Begin
- `GVT_RTND_GRADE_EXP` (date) - Retained Grade Expiration
- `GVT_SABBATIC_EXPIR` (date) - Sabbatic Expiration
- `GVT_CURR_APT_AUTH1` (string) - Current Appointment Authority 1
- `GVT_CURR_APT_AUTH2` (string) - Current Appointment Authority 2
- `CMPNY_SENIORITY_DT` (date) - Company Seniority Date
- `BUSINESS_TITLE` (string) - Business Title
- `GVT_APPT_LIMIT_HRS` (decimal) - Appointment Limit Hours
- `PROBATION_DT` (date) - Probation Date
- `GVT_WGI_DUE_DATE` (date) - WGI Due Date
- `HIRE_DT` (date) - Hire Date
- `SERVICE_DT` (date) - Service Date
- `LAST_INCREASE_DT` (date) - Last Increase Date
- `GVT_WGI_STATUS` (string) - WGI Status
- `GVT_TENURE` (string) - Tenure
- `GVT_SPEP` (string) - Special Employment Program
- `GVT_TEMP_PRO_EXPIR` (date) - Temp Promotion Expiration
- `GVT_TEMP_PSN_EXPIR` (date) - Temp Position Expiration
- `GVT_DETAIL_EXPIRES` (date) - Detail Expiration
- `GVT_APPT_EXPIR_DT` (date) - Appointment Expiration
- `GVT_SUPV_PROB_DT` (date) - Supervisory Probation Date
- `GVT_CNV_BEGIN_DATE` (date) - Career Conversion Begin
- `GVT_COMP_LVL_PERM` (string) - Competitive Level
- `GVT_APPT_LIMIT_DYS` (decimal) - Appointment Limit Days
- Plus ~50 more output ports (full PS_GVT_EMPLOYMENT record)

### 6.2 lkp_PS_GVT_PERS_NID

| Property | Value |
|----------|-------|
| **Lookup Table** | `PS_GVT_PERS_NID` |
| **Connection** | `$Source` |
| **SQL Override** | None |
| **Lookup Condition** | `EMPLID = EMPLID_IN AND EMPL_RCD = EMPL_RCD_IN AND EFFDT = EFFDT_IN AND EFFSEQ = EFFSEQ_IN` |
| **Caching** | YES |
| **Multiple Match** | Use Any Value |

**Return Ports**:
- `NATIONAL_ID` (string[20]) - Maps to SSN in NWK_ACTION_PRIMARY_TBL
- `COUNTRY`, `NATIONAL_ID_TYPE`, `SSN_KEY_FRA`, `PRIMARY_NID`

### 6.3 lkp_PS_GVT_AWD_DATA

| Property | Value |
|----------|-------|
| **Lookup Table** | `PS_GVT_AWD_DATA` |
| **Connection** | `$Source` |
| **SQL Override** | None |
| **Lookup Condition** | `EMPLID = EMPLID_IN AND EMPL_RCD = EMPL_RCD_IN AND EFFDT = EFFDT_IN AND EFFSEQ = EFFSEQ_IN` |
| **Caching** | YES |
| **Multiple Match** | Use Any Value |

**Return Ports** (key fields):
- `GOAL_AMT` (decimal[10]) - Award goal amount (used for cash awards, recruitment/relocation bonuses, buyout)
- `OTH_HRS` (decimal[6]) - Other hours (used for time-off awards)
- `OTH_PAY` (decimal[10]) - Other pay (used for severance)
- `EARNINGS_END_DT` (date) - Earnings end date (used for bonus expiration dates)
- `ERNCD` (string[3]) - Earnings code (used to exclude RCR/RLC from cash awards)
- `GVT_TANG_BEN_AMT` (decimal[38]) - Tangible benefit amount (cash award benefit)

### 6.4 lkp_PS_GVT_EE_DATA_TRK

| Property | Value |
|----------|-------|
| **Lookup Table** | `PS_GVT_EE_DATA_TRK` |
| **Connection** | `$Source` |
| **Caching** | YES |
| **Multiple Match** | Use Any Value |

**SQL Override**:
```sql
SELECT DISTINCT A.GVT_DATE_WRK as GVT_DATE_WRK,
       A.EMPLID as EMPLID, A.EMPL_RCD as EMPL_RCD,
       A.EFFDT as EFFDT, A.EFFSEQ as EFFSEQ,
       A.GVT_WIP_STATUS as GVT_WIP_STATUS
FROM PS_GVT_EE_DATA_TRK A
```

**Lookup Condition**: `EMPLID = EMPLID_IN AND EMPL_RCD = EMPL_RCD_IN AND EFFDT = EFFDT_IN AND EFFSEQ = EFFSEQ_IN`

**Input Ports**: EMPLID_IN, EMPL_RCD_IN, EFFDT_IN, EFFSEQ_IN, GVT_WIP_STATUS_IN

**Return Port**: `GVT_DATE_WRK` (date) - Work date for event submitted date fallback

### 6.5 lkp_PS_HE_FILL_POS

| Property | Value |
|----------|-------|
| **Lookup Table** | `PS_HE_FILL_POS` |
| **Connection** | `$Source` |
| **SQL Override** | None |
| **Lookup Condition** | `EMPLID = EMPLID_IN AND EMPL_RCD = EMPL_RCD_IN AND EFFDT = EFFDT_IN AND EFFSEQ = EFFSEQ_IN` |
| **Caching** | YES |

**Return Port**: `HE_FILL_POSITION` (string[1]) - Fill position indicator, maps to FILLING_POSITION_CD

### 6.6 lkp_PS_GVT_CITIZENSHIP

| Property | Value |
|----------|-------|
| **Lookup Table** | `PS_GVT_CITIZENSHIP` |
| **Connection** | `$Source` |
| **SQL Override** | None |
| **Lookup Condition** | `EMPLID = EMPLID_IN AND EMPL_RCD = EMPL_RCD_IN AND EFFDT = EFFDT_IN AND EFFSEQ = EFFSEQ_IN` |
| **Caching** | YES |

**Return Port**: `CITIZENSHIP_STATUS` (string[1]) - Citizenship status, transformed in exp_MAIN2BIIS

### 6.7 lkp_PS_GVT_PERS_DATA

| Property | Value |
|----------|-------|
| **Lookup Table** | `PS_GVT_PERS_DATA` |
| **Connection** | `$Source` |
| **Caching** | YES |

**SQL Override**:
```sql
SELECT
    p.LAST_NAME as LAST_NAME,
    p.FIRST_NAME as FIRST_NAME,
    p.MIDDLE_NAME as MIDDLE_NAME,
    p.ADDRESS1 as ADDRESS1,
    p.CITY as CITY,
    p.STATE as STATE,
    p.POSTAL as POSTAL,
    p.GEO_CODE as GEO_CODE,
    p.SEX as SEX,
    p.BIRTHDATE as BIRTHDATE,
    p.MILITARY_STATUS as MILITARY_STATUS,
    p.GVT_CRED_MIL_SVCE as GVT_CRED_MIL_SVCE,
    p.GVT_MILITARY_COMP as GVT_MILITARY_COMP,
    p.GVT_MIL_RESRVE_CAT as GVT_MIL_RESRVE_CAT,
    p.GVT_VET_PREF_APPT as GVT_VET_PREF_APPT,
    p.ETHNIC_GROUP as ETHNIC_GROUP,
    p.GVT_DISABILITY_CD as GVT_DISABILITY_CD,
    p.EMPLID as EMPLID,
    p.EMPL_RCD as EMPL_RCD,
    p.EFFDT as EFFDT,
    p.EFFSEQ as EFFSEQ
FROM
    PS_GVT_PERS_DATA p, NWK_NEW_EHRP_ACTIONS_TBL n
WHERE
    p.EMPLID = n.EMPLID and
    p.EMPL_RCD = n.EMPL_RCD and
    p.EFFDT = n.EFFDT and
    p.EFFSEQ = n.EFFSEQ
```

**Lookup Condition**: `EMPLID = EMPLID_IN AND EMPL_RCD = EMPL_RCD_IN AND EFFDT = EFFDT_IN AND EFFSEQ = EFFSEQ_IN`

**Return Ports**:
- `LAST_NAME`, `FIRST_NAME`, `MIDDLE_NAME` (string[30])
- `ADDRESS1` (string[55])
- `CITY` (string[30]), `STATE` (string[6]), `POSTAL` (string[12])
- `GEO_CODE` (string[11])
- `SEX` (string[1]), `BIRTHDATE` (date)
- `MILITARY_STATUS` (string[1])
- `GVT_CRED_MIL_SVCE` (string[6])
- `GVT_MILITARY_COMP` (string[1])
- `GVT_MIL_RESRVE_CAT` (string[1])
- `GVT_VET_PREF_APPT` (string[1])
- `ETHNIC_GROUP` (string[1])
- `GVT_DISABILITY_CD` (string[2])

### 6.8 lkp_PS_JPM_JP_ITEMS

| Property | Value |
|----------|-------|
| **Lookup Table** | `PS_JPM_JP_ITEMS` |
| **Connection** | `BIISPRD` (note: different connection than $Source) |
| **Caching** | YES |

**SQL Override**:
```sql
SELECT PS_JPM_JP_ITEMS.JPM_CAT_TYPE as JPM_CAT_TYPE,
       PS_JPM_JP_ITEMS.JPM_CAT_ITEM_ID as JPM_CAT_ITEM_ID,
       PS_JPM_JP_ITEMS.JPM_CAT_ITEM_QUAL2 as JPM_CAT_ITEM_QUAL2,
       PS_JPM_JP_ITEMS.EFFDT as EFFDT,
       PS_JPM_JP_ITEMS.EFF_STATUS as EFF_STATUS,
       PS_JPM_JP_ITEMS.JPM_INTEGER_2 as JPM_INTEGER_2,
       PS_JPM_JP_ITEMS.MAJOR_CODE as MAJOR_CODE,
       PS_JPM_JP_ITEMS.MAJOR_DESCR as MAJOR_DESCR,
       PS_JPM_JP_ITEMS.LASTUPDOPRID as LASTUPDOPRID,
       PS_JPM_JP_ITEMS.JPM_PROFILE_ID as JPM_PROFILE_ID
FROM EHRP.PS_JPM_JP_ITEMS
WHERE jpm_cat_type = 'DEG' AND eff_status = 'A'
```

**Lookup Condition**: `JPM_PROFILE_ID = EMPLID_IN`

> **Note**: This lookup uses EMPLID as profile ID and filters for active degree records only.

**Return Ports**:
- `JPM_INTEGER_2` (decimal[38]) - Year degree attained (maps to YEAR_DEGREE_ATTAINED_DTE)
- `MAJOR_CODE` (string[10]) - Maps to INSTRUCTIONAL_PROGRAM_CD
- `JPM_CAT_ITEM_ID` (string[12]) - Maps to EDUCATION_LEVEL_CD

### 6.9 lkp_OLD_SEQUENCE_NUMBER

| Property | Value |
|----------|-------|
| **Lookup Table** | `SEQUENCE_NUM_TBL` |
| **Connection** | `$Source` |
| **SQL Override** | None |
| **Lookup Condition** | `EHRP_YEAR = o_CURRENT_YEAR` |
| **Caching** | YES |

**Ports**:
- Input: `o_CURRENT_YEAR` (decimal[10]) - from exp_GET_EFFDT_YEAR
- Lookup: `EHRP_YEAR` (decimal[4])
- Output: `EHRP_SEQ_NUMBER` (decimal[4]) - Last used sequence number for the year

---

## 7. Expression Details (Business Logic)

### 7.1 exp_GET_EFFDT_YEAR

**Purpose**: Extracts the year from EFFDT to use as lookup key for sequence numbers.

| Port | Type | Data Type | Expression |
|------|------|-----------|------------|
| `EFFDT` | INPUT/OUTPUT | date/time(29,9) | `EFFDT` |
| `v_curr_year` | LOCAL VARIABLE | decimal(10,0) | `GET_DATE_PART(EFFDT, 'YYYY')` |
| `o_CURRENT_YEAR` | OUTPUT | decimal(10,0) | `v_curr_year` |

**PySpark**: `year(col("EFFDT"))`

### 7.2 exp_PERS_DATA

**Purpose**: Pass-through transformation for personal data from the PS_GVT_PERS_DATA lookup. All expressions are identity pass-throughs.

| Port | Type | Expression |
|------|------|------------|
| `BIRTHDATE` | INPUT/OUTPUT | `BIRTHDATE` |
| `FIRST_NAME` | INPUT/OUTPUT | `FIRST_NAME` |
| `LAST_NAME` | INPUT/OUTPUT | `LAST_NAME` |
| `MIDDLE_NAME` | INPUT/OUTPUT | `MIDDLE_NAME` |
| `CITY` | INPUT/OUTPUT | `CITY` |
| `STATE` | INPUT/OUTPUT | `STATE` |
| `GEO_CODE` | INPUT/OUTPUT | `GEO_CODE` |
| `POSTAL` | INPUT/OUTPUT | `POSTAL` |
| `ADDRESS1` | INPUT/OUTPUT | `ADDRESS1` |
| `GVT_DISABILITY_CD` | INPUT/OUTPUT | `GVT_DISABILITY_CD` |
| `ETHNIC_GROUP` | INPUT/OUTPUT | `ETHNIC_GROUP` |
| `GVT_VET_PREF_APPT` | INPUT/OUTPUT | `GVT_VET_PREF_APPT` |
| `MILITARY_STATUS` | INPUT/OUTPUT | `MILITARY_STATUS` |
| `SEX` | INPUT/OUTPUT | `SEX` |
| `GVT_MILITARY_COMP` | INPUT/OUTPUT | `GVT_MILITARY_COMP` |
| `GVT_MIL_RESRVE_CAT` | INPUT/OUTPUT | `GVT_MIL_RESRVE_CAT` |
| `GVT_CRED_MIL_SVCE` | INPUT/OUTPUT | `GVT_CRED_MIL_SVCE` |

### 7.3 exp_MAIN2BIIS - Complete Port Listing

This is the **core business logic transformation** with 196 ports. Below is the complete listing grouped by functional area.

#### 7.3.1 Event ID Generation (Sequence Logic)

| Port | Type | Data Type | Expression | Notes |
|------|------|-----------|------------|-------|
| `EHRP_SEQ_NUMBER` | INPUT | decimal(4,0) | *(from lkp_OLD_SEQUENCE_NUMBER)* | Last sequence number |
| `v_CURRENT_YEAR` | LOCAL VAR | integer(10,0) | `GET_DATE_PART(EFFDT, 'YYYY')` | Current record year |
| `v_EVENT_ID` | LOCAL VAR | integer(10,0) | `IIF(v_CURRENT_YEAR = v_PREVIOUS_YEAR, v_EVENT_ID + 1, EHRP_SEQ_NUMBER + 1)` | **Stateful**: Increments within year, resets on year boundary |
| `v_PREVIOUS_YEAR` | LOCAL VAR | integer(10,0) | `v_CURRENT_YEAR` | Tracks previous year for year-change detection |
| `o_EVENT_ID` | OUTPUT | integer(10,0) | `v_EVENT_ID` | Final event ID |

> **CRITICAL PySpark Note**: This is a **stateful sequential counter**. It increments by 1 for each row within the same year and resets to `EHRP_SEQ_NUMBER + 1` when the year changes. Data must be processed in EFFDT order (matching the `ORDER BY PS_GVT_JOB.EFFDT` in the SQ). Use a window function with `row_number()` or a cumulative approach.

#### 7.3.2 Agency Assignment

| Port | Type | Data Type | Expression |
|------|------|-----------|------------|
| `COMPANY` | INPUT/OUTPUT | string(3,0) | `COMPANY` |
| `GVT_SUB_AGENCY` | INPUT | string(2,0) | *(input only)* |
| `o_AGCY_ASSIGN_CD` | OUTPUT | string(10,0) | `COMPANY \|\| GVT_SUB_AGENCY` |
| `GVT_XFER_TO_AGCY` | INPUT/OUTPUT | string(2,0) | `GVT_XFER_TO_AGCY` |
| `GVT_XFER_FROM_AGCY` | INPUT | string(2,0) | *(input only)* |
| `o_AGCY_SUBELEMENT_PRIOR_CD` | OUTPUT | string(4,0) | `IIF(IS_SPACES(GVT_XFER_FROM_AGCY) or ISNULL(GVT_XFER_FROM_AGCY), NULL, GVT_XFER_FROM_AGCY \|\| '00')` |

#### 7.3.3 Leave Balance Transformations (Zero-to-NULL)

All leave balance fields follow the pattern: if value is 0, output NULL; otherwise pass through.

| Port | Output Port | Expression |
|------|-------------|------------|
| `HE_AL_RED_CRED` | `o_HE_AL_RED_CRED` | `IIF(HE_AL_RED_CRED = 0, NULL, HE_AL_RED_CRED)` |
| `HE_AL_BALANCE` | `o_HE_AL_BALANCE` | `IIF(HE_AL_BALANCE = 0, NULL, HE_AL_BALANCE)` |
| `HE_LUMP_HRS` | `o_HE_LUMP_HRS` | `IIF(HE_LUMP_HRS = 0, NULL, HE_LUMP_HRS)` |
| `HE_AL_CARRYOVER` | `o_HE_AL_CARRYOVER` | `IIF(HE_AL_CARRYOVER = 0, NULL, HE_AL_CARRYOVER)` |
| `HE_RES_LASTYR` | `o_HE_RES_LASTYR` | `IIF(HE_RES_LASTYR = 0, NULL, HE_RES_LASTYR)` |
| `HE_RES_BALANCE` | `o_HE_RES_BALANCE` | `IIF(HE_RES_BALANCE = 0, NULL, HE_RES_BALANCE)` |
| `HE_RES_TWOYRS` | `o_HE_RES_TWOYRS` | `IIF(HE_RES_TWOYRS = 0, NULL, HE_RES_TWOYRS)` |
| `HE_RES_THREEYRS` | `o_HE_RES_THREEYRS` | `IIF(HE_RES_THREEYRS = 0, NULL, HE_RES_THREEYRS)` |
| `HE_AL_ACCRUAL` | `o_HE_AL_ACCRUAL` | `IIF(HE_AL_ACCRUAL = 0, NULL, HE_AL_ACCRUAL)` |
| `HE_AL_TOTAL` | `o_HE_AL_TOTAL` | `IIF(HE_AL_TOTAL = 0, NULL, HE_AL_TOTAL)` |
| `HE_AWOP_SEP` | `o_HE_AWOP_SEP` | `IIF(HE_AWOP_SEP = 0, NULL, HE_AWOP_SEP)` |
| `HE_AWOP_WIGI` | `o_HE_AWOP_WIGI` | `IIF(HE_AWOP_WIGI = 0, NULL, HE_AWOP_WIGI)` |
| `HE_SL_RED_CRED` | `o_HE_SL_RED_CRED` | `IIF(HE_SL_RED_CRED = 0, NULL, HE_SL_RED_CRED)` |
| `HE_SL_BALANCE` | `o_HE_SL_BALANCE` | `IIF(HE_SL_BALANCE = 0, NULL, HE_SL_BALANCE)` |
| `HE_FROZEN_SL` | `o_HE_FROZEN_SL` | `IIF(HE_FROZEN_SL = 0, NULL, HE_FROZEN_SL)` |
| `HE_SL_CARRYOVER` | `o_HE_SL_CARRYOVER` | `IIF(HE_SL_CARRYOVER = 0, NULL, HE_SL_CARRYOVER)` |
| `HE_SL_ACCRUAL` | `o_HE_SL_ACCRUAL` | `IIF(HE_SL_ACCRUAL = 0, NULL, HE_SL_ACCRUAL)` |
| `HE_SL_TOTAL` | `o_HE_SL_TOTAL` | `IIF(HE_SL_TOTAL = 0, NULL, HE_SL_TOTAL)` |

**PySpark pattern**: `when(col("field") == 0, lit(None)).otherwise(col("field"))`

#### 7.3.4 Base Hours Calculation

| Port | Expression |
|------|------------|
| `o_BASE_HOURS` | `IIF(GVT_WORK_SCHED = 'I', STD_HOURS, STD_HOURS * 2)` |

> Intermittent workers (I) get actual hours; all others get biweekly (x2).

#### 7.3.5 Permanent/Temp Position Code

| Port | Expression |
|------|------------|
| `o_PERM_TEMP_POSITION_CD` | `IIF(STD_HOURS < 40, 3, IIF(STD_HOURS >= 40, 1, 0))` |
| `o_PERMANENT_TEMP_POSITION_CD` | `IIF(REG_TEMP = 'R' AND GVT_WORK_SCHED = 'F', '1', IIF(IN(REG_TEMP, 'R', 'T') AND IN(GVT_WORK_SCHED, 'P', 'I'), '3', IIF(REG_TEMP = 'T' AND GVT_WORK_SCHED = 'F', '2', NULL)))` |

> **Note**: Two different position code calculations exist. `o_PERM_TEMP_POSITION_CD` maps to `PERMANENT_TEMP_POSITION_CD` in PRIMARY target, and `o_PERMANENT_TEMP_POSITION_CD` is an additional output.

#### 7.3.6 Legal Authority Text

| Port | Expression |
|------|------------|
| `o_LEG_AUTH_TXT_1` | `UPPER(GVT_PAR_AUTH_D1 \|\| RTRIM(GVT_PAR_AUTH_D1_2))` |
| `o_LEG_AUTH_TXT_2` | `UPPER(GVT_PAR_AUTH_D2 \|\| RTRIM(GVT_PAR_AUTH_D2_2))` |

#### 7.3.7 Load ID and Date Generation

| Port | Expression |
|------|------------|
| `v_MONTH_NUMBER` | `GET_DATE_PART(SYSDATE, 'MM')` |
| `v_DAY_NUMBER` | `GET_DATE_PART(SYSDATE, 'DD')` |
| `v_MONTH_CHAR` | `IIF(v_MONTH_NUMBER < 10, '0' \|\| TO_CHAR(v_MONTH_NUMBER), TO_CHAR(v_MONTH_NUMBER))` |
| `v_DAY_CHAR` | `IIF(v_DAY_NUMBER < 10, '0' \|\| TO_CHAR(v_DAY_NUMBER), TO_CHAR(v_DAY_NUMBER))` |
| `o_LOAD_ID` | `'NK' \|\| GET_DATE_PART(SYSDATE, 'YYYY') \|\| v_MONTH_CHAR \|\| v_DAY_CHAR` |
| `o_LOAD_DATE` | `trunc(sysdate)` |
| `o_LINE_SEQ` | `'01N'` (constant) |

> **PySpark**: `concat(lit("NK"), date_format(current_date(), "yyyyMMdd"))` and `current_date()`

#### 7.3.8 Cash Award Logic (NOA-based)

| Port | Expression |
|------|------------|
| `o_CASH_AWARD_AMT` | `DECODE(TRUE, IN(GVT_NOA_CODE, '840','841','842','843','844','845','848','849','873','874','875','876','877','878','879') AND NOT(IN(ERNCD, 'RCR', 'RLC')), GOAL_AMT, IN(GVT_NOA_CODE, '817', '889'), GOAL_AMT, NULL)` |
| `o_CASH_AWARD_BNFT_AMT` | `IIF(IN(GVT_NOA_CODE, '840','841','842','843','844','845','848','849','873','874','875','876','877','878','879') AND NOT(IN(ERNCD, 'RCR', 'RLC')), GVT_TANG_BEN_AMT, NULL)` |

> Cash awards are set for NOA codes 840-849, 873-879, 817, 889 but **exclude** earnings codes RCR and RLC. NOA 817 and 889 always get GOAL_AMT.

#### 7.3.9 Recruitment/Relocation Bonus Logic

| Port | Expression |
|------|------------|
| `o_RECRUITMENT_BONUS_AMT` | `IIF(GVT_NOA_CODE = '815' OR (GVT_NOA_CODE = '948' AND HE_NOA_EXT = '0'), GOAL_AMT, NULL)` |
| `o_RELOCATION_BONUS_AMT` | `IIF(IN(GVT_NOA_CODE, '816'), GOAL_AMT, NULL)` |
| `o_RECRUITMENT_EXP_DTE` | `IIF(GVT_NOA_CODE = '815' OR (GVT_NOA_CODE = '948' AND HE_NOA_EXT = '0'), EARNINGS_END_DT, NULL)` |
| `o_RELOCATION_EXP_DTE` | `IIF(GVT_NOA_CODE = '816' OR (GVT_NOA_CODE = '948' AND HE_NOA_EXT = '1'), EARNINGS_END_DT, NULL)` |

> - **Recruitment**: NOA 815, or NOA 948 with extension '0'
> - **Relocation**: NOA 816, or NOA 948 with extension '1'

#### 7.3.10 Time Off Awards

| Port | Expression |
|------|------------|
| `o_TIME_OFF_AWARD_AMT` | `IIF(IN(GVT_NOA_CODE, '846', '847'), OTH_HRS, NULL)` |
| `o_TIME_OFF_GRANTED_HRS` | `IIF(IN(GVT_NOA_CODE, '846', '847'), OTH_HRS, NULL)` |

#### 7.3.11 Severance Pay

| Port | Expression |
|------|------------|
| `o_SEVERANCE_PAY_AMT` | `IIF(IN(GVT_NOA_CODE, '304', '312', '356', '357'), OTH_PAY, NULL)` |
| `o_SEVERANCE_TOTAL_AMT` | `IIF(IN(GVT_NOA_CODE, '304', '312', '356', '357'), GOAL_AMT, NULL)` |
| `o_SEVERANCE_PAY_START_DTE` | `IIF(IN(GVT_NOA_CODE, '304', '312', '356', '357'), EFFDT, NULL)` |

#### 7.3.12 Buyout Logic

| Port | Expression |
|------|------------|
| `BUYOUT_EFFDT` | `IIF(GVT_NOA_CODE = '825', EFFDT, NULL)` |
| `o_BUYOUT_AMT` | `IIF(GVT_NOA_CODE = '825' and ACTION = 'BON', GOAL_AMT, NULL)` |

#### 7.3.13 Citizenship Status

| Port | Expression |
|------|------------|
| `o_CITIZENSHIP_STATUS` | `IIF(CITIZENSHIP_STATUS_IN = '1' or CITIZENSHIP_STATUS_IN = '2', '1', '8')` |

> Citizens (1) and nationals (2) map to '1'; all others map to '8'.

#### 7.3.14 TSP Vesting Code

| Port | Expression |
|------|------------|
| `o_TSP_VESTING_CD` | `IIF(IN(GVT_RETIRE_PLAN, 'K', 'M'), IIF(IN(GVT_TYPE_OF_APPT, '55', '34', '36', '46', '44'), 2, 3), 0)` |

> FERS plans (K, M) with certain appointment types get vesting code 2; other FERS get 3; non-FERS get 0.

#### 7.3.15 Event Submitted Date

| Port | Expression |
|------|------------|
| `o_EVENT_SUBMITTED_DTE` | `IIF(ISNULL(GVT_DATE_WRK), ACTION_DT, GVT_DATE_WRK)` |

> Falls back to ACTION_DT if GVT_DATE_WRK (from EE_DATA_TRK lookup) is null.

#### 7.3.16 Probation Date

| Port | Expression |
|------|------------|
| `o_PROBATION_DT` | `IIF(ISNULL(PROBATION_DT), NULL, ADD_TO_DATE(PROBATION_DT, 'MM', -12))` |

> Subtracts 12 months from probation date (converts end date to start date).

#### 7.3.17 FEGLI Living Benefit Code

| Port | Expression |
|------|------------|
| `FEGLI_LIVING_BNFT_CD` | `DECODE(GVT_NOA_CODE, '805', 'F', '806', 'P', NULL)` |

> NOA 805 = Full ('F'), NOA 806 = Partial ('P').

#### 7.3.18 LWOP Start Date

| Port | Expression |
|------|------------|
| `LWOP_START_DATE` | `IIF(IN(GVT_NOA_CODE, '460', '473'), EFFDT, NULL)` |

#### 7.3.19 Null/Space Handling

| Port | Expression |
|------|------------|
| `o_UNION_CD` | `IIF(IS_SPACES(UNION_CD), NULL, UNION_CD)` |
| `o_GVT_CURR_APT_AUTH1` | `IIF(IS_SPACES(GVT_CURR_APT_AUTH1), NULL, GVT_CURR_APT_AUTH1)` |
| `o_GVT_CURR_APT_AUTH2` | `IIF(IS_SPACES(GVT_CURR_APT_AUTH2), NULL, GVT_CURR_APT_AUTH2)` |
| `o_GVT_ANNUITY_OFFSET` | `IIF(GVT_ANNUITY_OFFSET = 0, NULL, GVT_ANNUITY_OFFSET)` |
| `o_APPT_LIMIT_NTE_HRS` | `IIF(GVT_APPT_LIMIT_HRS = 0, NULL, GVT_APPT_LIMIT_HRS)` |
| `YEAR_DEGREE` | `IIF(JPM_INTEGER_2 = 0, NULL, JPM_INTEGER_2)` |

#### 7.3.20 Other Transformations

| Port | Expression |
|------|------------|
| `o_EMP_RESID_CITY_STATE_NAME` | `CITY \|\| ', ' \|\| STATE` |
| `o_EFFSEQ` | `EFFSEQ` (decimal to string cast) |
| `o_BUSINESS_TITLE` | `UPPER(BUSINESS_TITLE)` |

#### 7.3.21 Pass-Through Ports (INPUT/OUTPUT)

The following ports pass through unchanged from source to downstream. They have identity expressions (e.g., `EMPLID` = `EMPLID`):

`COMPANY`, `GVT_XFER_TO_AGCY`, `ANNL_BENEF_BASE_RT`, `GVT_ANN_IND`, `GVT_TYPE_OF_APPT`, `HE_AWOP_SEP`, `BARG_UNIT`, `ACCT_CD`, `LOCATION`, `GRADE_ENTRY_DT`, `EFFDT`, `GVT_FEGLI`, `GVT_FEGLI_LIVING`, `GVT_LIVING_AMT`, `GVT_FERS_COVERAGE`, `FLSA_STATUS`, `GVT_CSRS_FROZN_SVC`, `GRADE`, `HOURLY_RT`, `GVT_LEG_AUTH_1`, `GVT_LEG_AUTH_2`, `GVT_LOCALITY_ADJ`, `GVT_NOA_CODE`, `HE_NOA_EXT`, `DEPTID`, `GVT_PAY_BASIS`, `GVT_PAY_PLAN`, `GVT_POI`, `JOBCODE`, `POSITION_NBR`, `GVT_POSN_OCCUPIED`, `GVT_PAY_RATE_DETER`, `GVT_PREV_RET_COVRG`, `GVT_RETIRE_PLAN`, `GVT_COMPRATE`, `GVT_HRLY_RT_NO_LOC`, `GVT_STEP`, `UNION_CD`, `STEP_ENTRY_DT`, `GVT_WORK_SCHED`, `EMPLID`, `EFFSEQ`, `EMPL_RCD`, `ACTION`, `ACTION_REASON`, `GVT_WIP_STATUS`, `GVT_STATUS_TYPE`, `HE_REG_MILITARY`, `HE_SPC_MILITARY`, `SAL_ADMIN_PLAN`, `GVT_RTND_PAY_PLAN`, `GVT_RTND_GRADE`, `GVT_RTND_STEP`, `HE_PP_UDED_AMT`, `HE_NO_TSP_PAYPER`, `HE_TSPA_SUB_YR`, `HE_EMP_UDED_AMT`, `HE_GVT_UDED_AMT`, `HE_TLTR_NO`, `HE_UDED_PAY_CD`, `HE_TSP_CANC_CD`, `REG_TEMP`, `ACTION_DT`, `GVT_SCD_RETIRE`, `GVT_SCD_TSP`, `GVT_RTND_GRADE_BEG`, `GVT_RTND_GRADE_EXP`, `GVT_SABBATIC_EXPIR`, `GVT_CURR_APT_AUTH1`, `GVT_CURR_APT_AUTH2`, `CMPNY_SENIORITY_DT`, `SETID_DEPT`, `GVT_PAR_NTE_DATE`, `GVT_LEO_POSITION`, `TEMP_GVT_EFFDT`, `PAYGROUP`, `REPORTS_TO`, `POSITION_ENTRY_DT`, `GVT_SCD_LEO`, `GVT_WGI_DUE_DATE`

---

## 8. Pre/Post-Load Orchestration

### 8.1 Pre-Load Script: `ehrp2biis_preload`

**File**: `ehrp2biis_preload` (ksh shell script)

**Purpose**: Executes Oracle SQL*Plus pre-load procedures before the Informatica session runs.

**Key Operations**:
1. Sets environment (sources `SETENV`, sets `INFA_HOME`, `ORACLE_HOME`, `LD_LIBRARY_PATH`)
2. Reads Oracle credentials from `$HOME/.use`, `$HOME/.pw`, `$HOME/.use1`, `$HOME/.pw1`
3. Connects to Oracle via SQL*Plus and executes `$homedir/step01` script
4. Logs output to `/home/sa-biisint/data/int/log/ehrp2biis_preload_<timestamp>.log`
5. Checks for errors in log; sends email notification on success or failure
6. Recipients: `peter.chen@hhs.gov nathan.knight@hhs.gov marvin.simon@hhs.gov`

**Environment Variables**:
- `HOME=/home/sa-biisint`
- `homedir=/data/BIISINT/bin/EHRP2BIIS`
- `INFA_HOME=/informatica/PowerCenter9.6.1`
- `logdir=/home/sa-biisint/data/int/log`

### 8.2 Post-Load Script: `ehrp2biis_afterload.sql`

**File**: `ehrp2biis_afterload.sql` (Oracle SQL*Plus script, 289 lines)

**Purpose**: Performs post-load data processing, quality fixes, and data promotion to production tables.

**Execution Steps** (in order):

#### Step 04: Fix Retained Step Code
```sql
UPDATE nknight.nwk_action_secondary_tbl a
SET a.retnd1_step_cd = NULL
WHERE a.event_id IN (SELECT b.event_id
                     FROM nknight.nwk_action_primary_tbl b
                     WHERE b.load_date = trunc(SYSDATE)
                       AND b.event_id < 9000000000)
  AND a.retnd1_step_cd = '0.0000000000000';
```

#### Step 05: Main After-Load Processing

1. **Display and update sequence numbers**:
   ```sql
   SELECT * FROM NKNIGHT.SEQUENCE_NUM_TBL ORDER BY 1;
   ALTER PROCEDURE update_sequence_number_tbl_p COMPILE;
   EXECUTE update_sequence_number_tbl_p;
   ```

2. **Execute formatting procedures** (4 procedures):
   ```sql
   EXEC HISTDBA.UPDT_ERP2BIIS_CRE8_REMARKS01_P;
   EXEC HISTDBA.UPDATE_ERP2BIIS_NO900S01_p;
   EXEC HISTDBA.ERP2BIIS_CRE8_REMARKS_900s01;
   EXEC HISTDBA.UPDATE_ERP2BIIS_900SONLY01_P;
   ```

3. **Update original cancelled transactions**:
   ```sql
   EXEC HISTDBA.UPDT_ORIG_CANCELLED_TRANS01_P;
   ```

4. **Update PROCESS_TABLE with start date** for WIP status change detection:
   ```sql
   UPDATE PROCESS_TABLE SET P_STARTDT = NULL;
   -- Then set to earliest EFFDT where WIP status changed
   UPDATE PROCESS_TABLE SET P_STARTDT = (
     SELECT effdt FROM (
       SELECT a.biis_event_id, a.emplid, a.empl_rcd, a.effdt, a.effseq,
              b.deptid, a.gvt_wip_status "OLD STATUS", b.gvt_wip_status "NEW STATUS"
       FROM nknight.ehrp_recs_tracking_tbl a, ehrp.ps_gvt_job b
       WHERE a.emplid = b.emplid AND a.empl_rcd = b.empl_rcd
         AND a.effdt = b.effdt AND a.effseq = b.effseq
         AND a.gvt_wip_status <> b.gvt_wip_status
         AND a.changed_wip_status IS NULL
       ORDER BY 4, 1
     ) WHERE ROWNUM < 2
   );
   -- Default to future date if no changes found
   UPDATE PROCESS_TABLE SET P_STARTDT = trunc(sysdate) + 10000
   WHERE P_STARTDT IS NULL;
   ```

5. **Check WIP status changes**:
   ```sql
   ALTER PROCEDURE chk_ehrp2biis_wip_status_p COMPILE;
   EXEC chk_ehrp2biis_wip_status_p;
   ```

6. **Promote today's data to production ALL tables**:
   ```sql
   INSERT INTO action_primary_all SELECT * FROM nknight.nwk_action_primary_tbl
     WHERE load_date = trunc(sysdate);
   INSERT INTO action_secondary_all SELECT * FROM nknight.nwk_action_secondary_tbl
     WHERE event_id IN (SELECT event_id FROM nknight.nwk_action_primary_tbl
                        WHERE load_date = trunc(sysdate));
   INSERT INTO action_remarks_all SELECT * FROM nknight.nwk_action_remarks_tbl
     WHERE event_id IN (SELECT event_id FROM nknight.nwk_action_primary_tbl
                        WHERE load_date = trunc(sysdate));
   ```

7. **Gather run counts**:
   ```sql
   EXEC HISTDBA.GATHER_EHRP2BIIS_RUNCOUNTS_P(NULL);
   ```

8. **Update cancelled actions** (delete old, re-insert corrected):
   ```sql
   -- Delete from ALL tables where WIP status changed today
   DELETE FROM ACTION_secondary_aLL WHERE EVENT_ID IN
     (SELECT biis_event_id FROM nknight.ehrp_recs_tracking_tbl
      WHERE biis_wip_status_changed_dt = trunc(sysdate));
   DELETE FROM ACTION_remarks_aLL WHERE EVENT_ID IN (...);
   DELETE FROM ACTION_primary_aLL WHERE EVENT_ID IN (...);

   -- Re-insert corrected records
   INSERT INTO ACTION_primary_aLL SELECT * FROM NKNIGHT.NWK_ACTION_primary_TBL
     WHERE EVENT_ID IN (...);
   INSERT INTO ACTION_secondary_aLL SELECT * FROM NKNIGHT.NWK_ACTION_secondary_TBL
     WHERE EVENT_ID IN (...);
   INSERT INTO ACTION_remarks_aLL SELECT * FROM NKNIGHT.NWK_ACTION_remarks_TBL
     WHERE EVENT_ID IN (...);
   ```

9. **Truncate staging table for next run**:
   ```sql
   TRUNCATE TABLE nknight.nwk_new_ehrp_actions_tbl;
   ```

---

## 9. Session Configuration

### 9.1 Session: s_m_EHRP2BIIS_UPDATE

| Property | Value |
|----------|-------|
| **Mapping** | `m_EHRP2BIIS_UPDATE` |
| **Config Reference** | `default_session_config` |
| **Reusable** | NO |

### 9.2 Connection References

The session uses connection variables that are resolved at runtime:

- **$Source** - Used by the Source Qualifier and most lookups (PS_GVT_EMPLOYMENT, PS_GVT_PERS_NID, PS_GVT_AWD_DATA, PS_GVT_EE_DATA_TRK, PS_HE_FILL_POS, PS_GVT_CITIZENSHIP, PS_GVT_PERS_DATA, SEQUENCE_NUM_TBL)
- **BIISPRD** - Used by lkp_PS_JPM_JP_ITEMS (different database)

### 9.3 Workflow: wf_EHRP2BIIS_UPDATE

| Property | Value |
|----------|-------|
| **Server** | `Prd_IS` |
| **Tasks** | Start -> s_m_EHRP2BIIS_UPDATE |

Simple linear workflow: Start task triggers the session.

---

## 10. PySpark Migration Notes

### 10.1 Overall Architecture

```
Pre-Load Script (PySpark/SQL equivalent)
    |
    v
Main PySpark Job:
    1. Read source tables (PS_GVT_JOB, NWK_NEW_EHRP_ACTIONS_TBL)
    2. Inner join on (EMPLID, EMPL_RCD, EFFDT, EFFSEQ)
    3. Left join 8 lookup tables
    4. Apply exp_MAIN2BIIS business logic
    5. Apply exp_PERS_DATA pass-through
    6. Write to 3 target tables
    |
    v
Post-Load Script (PySpark/SQL equivalent)
```

### 10.2 Critical Implementation Considerations

1. **Event ID Generation**: The Informatica mapping uses stateful local variables that maintain state across rows. In PySpark, implement with:
   ```python
   # Window-based approach
   window = Window.orderBy("EFFDT")
   # Partition by year, use row_number + base sequence number
   ```
   The sequence resets when the year changes, using the last known sequence from `SEQUENCE_NUM_TBL`.

2. **Source Qualifier Join**: Replace the Informatica SQ with a standard inner join:
   ```python
   df = ps_gvt_job.join(nwk_new_ehrp_actions,
       ["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"]).orderBy("EFFDT")
   ```

3. **Lookup Transformations**: Replace with left joins:
   ```python
   df = df.join(ps_gvt_employment, ["EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ"], "left")
   ```
   Note: lkp_PS_GVT_EE_DATA_TRK has a SQL override with `SELECT DISTINCT` - deduplicate first.
   Note: lkp_PS_GVT_PERS_DATA has a SQL override that pre-joins with NWK_NEW_EHRP_ACTIONS_TBL.
   Note: lkp_PS_JPM_JP_ITEMS uses a **different connection** (BIISPRD) and filters `jpm_cat_type = 'DEG' AND eff_status = 'A'`, joining on `JPM_PROFILE_ID = EMPLID`.

4. **Zero-to-NULL Pattern**: Create a reusable function:
   ```python
   def zero_to_null(col_name):
       return when(col(col_name) == 0, lit(None)).otherwise(col(col_name))
   ```

5. **IS_SPACES Pattern**: Informatica's `IS_SPACES()` checks if a string contains only spaces:
   ```python
   def is_spaces_to_null(col_name):
       return when(trim(col(col_name)) == "", lit(None)).otherwise(col(col_name))
   ```

6. **NOA Code Conditional Logic**: The cash award, bonus, severance, and other NOA-dependent fields use `IN()` checks that should be implemented with `.isin()`:
   ```python
   award_noa_codes = ['840','841','842','843','844','845','848','849',
                      '873','874','875','876','877','878','879']
   ```

7. **Multiple Target Tables**: The single pipeline writes to 3 targets simultaneously. In PySpark, compute all output columns first, then select appropriate columns for each target write.

8. **Data Order**: The `ORDER BY PS_GVT_JOB.EFFDT` in the Source Qualifier is critical for the Event ID sequence logic. Ensure the DataFrame is sorted before applying the sequence.

### 10.3 Connection Mapping

| Informatica Connection | Oracle Schema | PySpark Equivalent |
|----------------------|---------------|-------------------|
| `$Source` (ORA_BIISPRD_SRC) | EHRP / NKNIGHT | Databricks table or JDBC source |
| `BIISPRD` | EHRP (for PS_JPM_JP_ITEMS) | Separate Databricks table or JDBC source |

### 10.4 Pre/Post-Load Migration

The pre-load and post-load scripts contain Oracle PL/SQL procedures that need to be migrated separately:

**Pre-Load** (`ehrp2biis_preload`):
- Execute `step01` SQL script (not in repo - needs to be obtained)
- Notification via email on success/failure

**Post-Load** (`ehrp2biis_afterload.sql`):
- All SQL operations need Oracle-to-Spark SQL translation
- Stored procedures (HISTDBA.*) need to be reverse-engineered or obtained
- The pattern of promote-to-ALL-tables and handle-cancelled-actions must be preserved
- `TRUNCATE TABLE nwk_new_ehrp_actions_tbl` at end clears the staging table

### 10.5 Tables Referenced (Complete List)

| Table | Schema | Usage |
|-------|--------|-------|
| PS_GVT_JOB | EHRP | Source (main) |
| NWK_NEW_EHRP_ACTIONS_TBL | NKNIGHT | Source (driver/staging) |
| PS_GVT_EMPLOYMENT | EHRP | Lookup |
| PS_GVT_PERS_NID | EHRP | Lookup |
| PS_GVT_AWD_DATA | EHRP | Lookup |
| PS_GVT_EE_DATA_TRK | EHRP | Lookup |
| PS_HE_FILL_POS | EHRP | Lookup |
| PS_GVT_CITIZENSHIP | EHRP | Lookup |
| PS_GVT_PERS_DATA | EHRP | Lookup |
| PS_JPM_JP_ITEMS | EHRP | Lookup (via BIISPRD connection) |
| SEQUENCE_NUM_TBL | NKNIGHT | Lookup (sequence numbers) |
| NWK_ACTION_PRIMARY_TBL | NKNIGHT | Target |
| NWK_ACTION_SECONDARY_TBL | NKNIGHT | Target |
| EHRP_RECS_TRACKING_TBL | NKNIGHT | Target |
| ACTION_PRIMARY_ALL | HISTDBA | Post-load promotion target |
| ACTION_SECONDARY_ALL | HISTDBA | Post-load promotion target |
| ACTION_REMARKS_ALL | HISTDBA | Post-load promotion target |
| NWK_ACTION_REMARKS_TBL | NKNIGHT | Post-load (referenced) |
| PROCESS_TABLE | NKNIGHT | Post-load (WIP status tracking) |

### 10.6 Stored Procedures Referenced

These Oracle stored procedures are called in `ehrp2biis_afterload.sql` and need migration:

1. `update_sequence_number_tbl_p` - Updates sequence numbers after load
2. `HISTDBA.UPDT_ERP2BIIS_CRE8_REMARKS01_P` - Creates remarks for new records
3. `HISTDBA.UPDATE_ERP2BIIS_NO900S01_p` - Updates non-900 series records
4. `HISTDBA.ERP2BIIS_CRE8_REMARKS_900s01` - Creates remarks for 900-series
5. `HISTDBA.UPDATE_ERP2BIIS_900SONLY01_P` - Updates 900-series only records
6. `HISTDBA.UPDT_ORIG_CANCELLED_TRANS01_P` - Updates original cancelled transactions
7. `chk_ehrp2biis_wip_status_p` - Checks/updates WIP status changes
8. `HISTDBA.GATHER_EHRP2BIIS_RUNCOUNTS_P` - Gathers run statistics
