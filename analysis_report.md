# Informatica PowerCenter Pipeline Analysis Report

## Pipeline: EHRP2BIIS_UPDATE

**Purpose**: Extract HR/Personnel action data from PeopleSoft (EHRP) Oracle tables, transform and load into BIIS (Benefits Information Integration System) staging tables for federal government employee personnel actions processing.

**Platform**: Informatica PowerCenter 9.6.1  
**Source Database**: Oracle (ORA_BIISPRD_SRC)  
**Target Database**: Oracle (INFO_NATE)

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Sources](#2-sources)
3. [Targets](#3-targets)
4. [Transformations](#4-transformations)
5. [Data Flow DAG](#5-data-flow-dag)
6. [Mapping, Session, and Workflow](#6-mapping-session-and-workflow)
7. [Pre/Post-Load Scripts](#7-prepost-load-scripts)
8. [Migration Considerations](#8-migration-considerations)

---

## 1. Executive Summary

| Component | Count |
|-----------|-------|
| Source Tables | 2 |
| Target Tables | 3 |
| Transformations | 13 |
| Mapping Instances | 18 |
| Data Flow Connectors | 589 |
| Sessions | 1 |
| Workflows | 1 |

### Transformation Breakdown

| Type | Count | Names |
|------|-------|-------|
| Expression | 3 | exp_GET_EFFDT_YEAR, exp_MAIN2BIIS, exp_PERS_DATA |
| Lookup Procedure | 9 | lkp_OLD_SEQUENCE_NUMBER, lkp_PS_GVT_EMPLOYMENT, lkp_PS_GVT_PERS_NID, lkp_PS_GVT_AWD_DATA, lkp_PS_GVT_EE_DATA_TRK, lkp_PS_HE_FILL_POS, lkp_PS_GVT_CITIZENSHIP, lkp_PS_GVT_PERS_DATA, lkp_PS_JPM_JP_ITEMS |
| Source Qualifier | 1 | SQ_PS_GVT_JOB |

---

## 2. Sources

### 2.1. NWK_NEW_EHRP_ACTIONS_TBL

- **Database Type**: Oracle
- **Database Name**: ORA_BIISPRD_SRC
- **Owner**: NKNIGHT
- **Field Count**: 4

| # | Field Name | Datatype | Precision | Scale | Nullable |
|---|-----------|----------|-----------|-------|----------|
| 1 | EMPLID | varchar2 | 11 | 0 | NOTNULL |
| 2 | EMPL_RCD | number(p,s) | 38 | 0 | NOTNULL |
| 3 | EFFDT | date | 19 | 0 | NOTNULL |
| 4 | EFFSEQ | number(p,s) | 38 | 0 | NOTNULL |

### 2.2. PS_GVT_JOB

- **Database Type**: Oracle
- **Database Name**: ORA_BIISPRD_SRC
- **Owner**: EHRP
- **Field Count**: 246

| # | Field Name | Datatype | Precision | Scale | Nullable |
|---|-----------|----------|-----------|-------|----------|
| 1 | EMPLID | varchar2 | 11 | 0 | NOTNULL |
| 2 | EMPL_RCD | number(p,s) | 38 | 0 | NOTNULL |
| 3 | EFFDT | date | 19 | 0 | NOTNULL |
| 4 | EFFSEQ | number(p,s) | 38 | 0 | NOTNULL |
| 5 | DEPTID | varchar2 | 10 | 0 | NOTNULL |
| 6 | JOBCODE | varchar2 | 6 | 0 | NOTNULL |
| 7 | POSITION_NBR | varchar2 | 8 | 0 | NOTNULL |
| 8 | POSITION_OVERRIDE | varchar2 | 1 | 0 | NOTNULL |
| 9 | POSN_CHANGE_RECORD | varchar2 | 1 | 0 | NOTNULL |
| 10 | EMPL_STATUS | varchar2 | 1 | 0 | NOTNULL |
| 11 | ACTION | varchar2 | 3 | 0 | NOTNULL |
| 12 | ACTION_DT | date | 19 | 0 | NULL |
| 13 | ACTION_REASON | varchar2 | 3 | 0 | NOTNULL |
| 14 | LOCATION | varchar2 | 10 | 0 | NOTNULL |
| 15 | TAX_LOCATION_CD | varchar2 | 10 | 0 | NOTNULL |
| 16 | JOB_ENTRY_DT | date | 19 | 0 | NULL |
| 17 | DEPT_ENTRY_DT | date | 19 | 0 | NULL |
| 18 | POSITION_ENTRY_DT | date | 19 | 0 | NULL |
| 19 | SHIFT | varchar2 | 1 | 0 | NOTNULL |
| 20 | REG_TEMP | varchar2 | 1 | 0 | NOTNULL |
| 21 | FULL_PART_TIME | varchar2 | 1 | 0 | NOTNULL |
| 22 | COMPANY | varchar2 | 3 | 0 | NOTNULL |
| 23 | PAYGROUP | varchar2 | 3 | 0 | NOTNULL |
| 24 | BAS_GROUP_ID | varchar2 | 3 | 0 | NOTNULL |
| 25 | ELIG_CONFIG1 | varchar2 | 10 | 0 | NOTNULL |
| 26 | ELIG_CONFIG2 | varchar2 | 10 | 0 | NOTNULL |
| 27 | ELIG_CONFIG3 | varchar2 | 10 | 0 | NOTNULL |
| 28 | ELIG_CONFIG4 | varchar2 | 10 | 0 | NOTNULL |
| 29 | ELIG_CONFIG5 | varchar2 | 10 | 0 | NOTNULL |
| 30 | ELIG_CONFIG6 | varchar2 | 10 | 0 | NOTNULL |
| 31 | ELIG_CONFIG7 | varchar2 | 10 | 0 | NOTNULL |
| 32 | ELIG_CONFIG8 | varchar2 | 10 | 0 | NOTNULL |
| 33 | ELIG_CONFIG9 | varchar2 | 10 | 0 | NOTNULL |
| 34 | BEN_STATUS | varchar2 | 4 | 0 | NOTNULL |
| 35 | BAS_ACTION | varchar2 | 3 | 0 | NOTNULL |
| 36 | COBRA_ACTION | varchar2 | 3 | 0 | NOTNULL |
| 37 | EMPL_TYPE | varchar2 | 1 | 0 | NOTNULL |
| 38 | HOLIDAY_SCHEDULE | varchar2 | 6 | 0 | NOTNULL |
| 39 | STD_HOURS | number(p,s) | 6 | 2 | NOTNULL |
| 40 | STD_HRS_FREQUENCY | varchar2 | 5 | 0 | NOTNULL |
| 41 | OFFICER_CD | varchar2 | 1 | 0 | NOTNULL |
| 42 | EMPL_CLASS | varchar2 | 3 | 0 | NOTNULL |
| 43 | SAL_ADMIN_PLAN | varchar2 | 4 | 0 | NOTNULL |
| 44 | GRADE | varchar2 | 3 | 0 | NOTNULL |
| 45 | GRADE_ENTRY_DT | date | 19 | 0 | NULL |
| 46 | STEP | number(p,s) | 38 | 0 | NOTNULL |
| 47 | STEP_ENTRY_DT | date | 19 | 0 | NULL |
| 48 | GL_PAY_TYPE | varchar2 | 6 | 0 | NOTNULL |
| 49 | ACCT_CD | varchar2 | 25 | 0 | NOTNULL |
| 50 | EARNS_DIST_TYPE | varchar2 | 1 | 0 | NOTNULL |
| 51 | COMP_FREQUENCY | varchar2 | 5 | 0 | NOTNULL |
| 52 | COMPRATE | number(p,s) | 18 | 6 | NOTNULL |
| 53 | CHANGE_AMT | number(p,s) | 18 | 6 | NOTNULL |
| 54 | CHANGE_PCT | number(p,s) | 6 | 3 | NOTNULL |
| 55 | ANNUAL_RT | number(p,s) | 18 | 3 | NOTNULL |
| 56 | MONTHLY_RT | number(p,s) | 18 | 3 | NOTNULL |
| 57 | DAILY_RT | number(p,s) | 18 | 3 | NOTNULL |
| 58 | HOURLY_RT | number(p,s) | 18 | 6 | NOTNULL |
| 59 | ANNL_BENEF_BASE_RT | number(p,s) | 18 | 3 | NOTNULL |
| 60 | SHIFT_RT | number(p,s) | 18 | 6 | NOTNULL |
| 61 | SHIFT_FACTOR | number(p,s) | 4 | 3 | NOTNULL |
| 62 | CURRENCY_CD | varchar2 | 3 | 0 | NOTNULL |
| 63 | BUSINESS_UNIT | varchar2 | 5 | 0 | NOTNULL |
| 64 | SETID_DEPT | varchar2 | 5 | 0 | NOTNULL |
| 65 | SETID_JOBCODE | varchar2 | 5 | 0 | NOTNULL |
| 66 | SETID_LOCATION | varchar2 | 5 | 0 | NOTNULL |
| 67 | SETID_SALARY | varchar2 | 5 | 0 | NOTNULL |
| 68 | REG_REGION | varchar2 | 5 | 0 | NOTNULL |
| 69 | DIRECTLY_TIPPED | varchar2 | 1 | 0 | NOTNULL |
| 70 | FLSA_STATUS | varchar2 | 1 | 0 | NOTNULL |
| 71 | EEO_CLASS | varchar2 | 1 | 0 | NOTNULL |
| 72 | FUNCTION_CD | varchar2 | 2 | 0 | NOTNULL |
| 73 | TARIFF_GER | varchar2 | 2 | 0 | NOTNULL |
| 74 | TARIFF_AREA_GER | varchar2 | 3 | 0 | NOTNULL |
| 75 | PERFORM_GROUP_GER | varchar2 | 2 | 0 | NOTNULL |
| 76 | LABOR_TYPE_GER | varchar2 | 1 | 0 | NOTNULL |
| 77 | SPK_COMM_ID_GER | varchar2 | 9 | 0 | NOTNULL |
| 78 | HOURLY_RT_FRA | varchar2 | 3 | 0 | NOTNULL |
| 79 | ACCDNT_CD_FRA | varchar2 | 1 | 0 | NOTNULL |
| 80 | VALUE_1_FRA | varchar2 | 5 | 0 | NOTNULL |
| 81 | VALUE_2_FRA | varchar2 | 5 | 0 | NOTNULL |
| 82 | VALUE_3_FRA | varchar2 | 5 | 0 | NOTNULL |
| 83 | VALUE_4_FRA | varchar2 | 5 | 0 | NOTNULL |
| 84 | VALUE_5_FRA | varchar2 | 5 | 0 | NOTNULL |
| 85 | CTG_RATE | number(p,s) | 38 | 0 | NOTNULL |
| 86 | PAID_HOURS | number(p,s) | 6 | 2 | NOTNULL |
| 87 | PAID_FTE | number(p,s) | 7 | 6 | NOTNULL |
| 88 | PAID_HRS_FREQUENCY | varchar2 | 5 | 0 | NOTNULL |
| 89 | GVT_EFFDT | date | 19 | 0 | NULL |
| 90 | GVT_EFFDT_PROPOSED | date | 19 | 0 | NULL |
| 91 | GVT_TRANS_NBR | number(p,s) | 38 | 0 | NOTNULL |
| 92 | GVT_TRANS_NBR_SEQ | number(p,s) | 38 | 0 | NOTNULL |
| 93 | GVT_WIP_STATUS | varchar2 | 3 | 0 | NOTNULL |
| 94 | GVT_STATUS_TYPE | varchar2 | 3 | 0 | NOTNULL |
| 95 | GVT_NOA_CODE | varchar2 | 3 | 0 | NOTNULL |
| 96 | GVT_LEG_AUTH_1 | varchar2 | 3 | 0 | NOTNULL |
| 97 | GVT_PAR_AUTH_D1 | varchar2 | 25 | 0 | NOTNULL |
| 98 | GVT_PAR_AUTH_D1_2 | varchar2 | 25 | 0 | NOTNULL |
| 99 | GVT_LEG_AUTH_2 | varchar2 | 3 | 0 | NOTNULL |
| 100 | GVT_PAR_AUTH_D2 | varchar2 | 25 | 0 | NOTNULL |
| 101 | GVT_PAR_AUTH_D2_2 | varchar2 | 25 | 0 | NOTNULL |
| 102 | GVT_PAR_NTE_DATE | date | 19 | 0 | NULL |
| 103 | GVT_WORK_SCHED | varchar2 | 1 | 0 | NOTNULL |
| 104 | GVT_SUB_AGENCY | varchar2 | 2 | 0 | NOTNULL |
| 105 | GVT_ELIG_FEHB | varchar2 | 3 | 0 | NOTNULL |
| 106 | GVT_FEHB_DT | date | 19 | 0 | NULL |
| 107 | GVT_PAY_RATE_DETER | varchar2 | 1 | 0 | NOTNULL |
| 108 | GVT_STEP | varchar2 | 2 | 0 | NOTNULL |
| 109 | GVT_RTND_PAY_PLAN | varchar2 | 2 | 0 | NOTNULL |
| 110 | GVT_RTND_SAL_PLAN | varchar2 | 4 | 0 | NOTNULL |
| 111 | GVT_RTND_GRADE | varchar2 | 3 | 0 | NOTNULL |
| 112 | GVT_RTND_STEP | number(p,s) | 38 | 0 | NOTNULL |
| 113 | GVT_RTND_GVT_STEP | varchar2 | 2 | 0 | NOTNULL |
| 114 | GVT_PAY_BASIS | varchar2 | 2 | 0 | NOTNULL |
| 115 | GVT_COMPRATE | number(p,s) | 18 | 6 | NOTNULL |
| 116 | GVT_LOCALITY_ADJ | number(p,s) | 8 | 2 | NOTNULL |
| 117 | GVT_BIWEEKLY_RT | number(p,s) | 9 | 2 | NOTNULL |
| 118 | GVT_DAILY_RT | number(p,s) | 9 | 2 | NOTNULL |
| 119 | GVT_HRLY_RT_NO_LOC | number(p,s) | 18 | 6 | NOTNULL |
| 120 | GVT_DLY_RT_NO_LOC | number(p,s) | 9 | 2 | NOTNULL |
| 121 | GVT_BW_RT_NO_LOC | number(p,s) | 9 | 2 | NOTNULL |
| 122 | GVT_MNLY_RT_NO_LOC | number(p,s) | 18 | 3 | NOTNULL |
| 123 | GVT_ANNL_RT_NO_LOC | number(p,s) | 18 | 3 | NOTNULL |
| 124 | GVT_XFER_FROM_AGCY | varchar2 | 2 | 0 | NOTNULL |
| 125 | GVT_XFER_TO_AGCY | varchar2 | 2 | 0 | NOTNULL |
| 126 | GVT_RETIRE_PLAN | varchar2 | 2 | 0 | NOTNULL |
| 127 | GVT_ANN_IND | varchar2 | 1 | 0 | NOTNULL |
| 128 | GVT_FEGLI | varchar2 | 2 | 0 | NOTNULL |
| 129 | GVT_FEGLI_LIVING | varchar2 | 1 | 0 | NOTNULL |
| 130 | GVT_LIVING_AMT | number(p,s) | 38 | 0 | NOTNULL |
| 131 | GVT_ANNUITY_OFFSET | number(p,s) | 10 | 2 | NOTNULL |
| 132 | GVT_CSRS_FROZN_SVC | varchar2 | 4 | 0 | NOTNULL |
| 133 | GVT_PREV_RET_COVRG | varchar2 | 1 | 0 | NOTNULL |
| 134 | GVT_FERS_COVERAGE | varchar2 | 1 | 0 | NOTNULL |
| 135 | GVT_TYPE_OF_APPT | varchar2 | 2 | 0 | NOTNULL |
| 136 | GVT_POI | varchar2 | 4 | 0 | NOTNULL |
| 137 | GVT_POSN_OCCUPIED | varchar2 | 1 | 0 | NOTNULL |
| 138 | GVT_CONT_EMPLID | varchar2 | 11 | 0 | NOTNULL |
| 139 | GVT_ROUTE_NEXT | varchar2 | 11 | 0 | NOTNULL |
| 140 | GVT_CHANGE_FLAG | varchar2 | 1 | 0 | NOTNULL |
| 141 | GVT_TSP_UPD_IND | varchar2 | 1 | 0 | NOTNULL |
| 142 | GVT_PI_UPD_IND | varchar2 | 1 | 0 | NOTNULL |
| 143 | GVT_SF52_NBR | varchar2 | 10 | 0 | NOTNULL |
| 144 | GVT_S113G_CEILING | varchar2 | 1 | 0 | NOTNULL |
| 145 | GVT_LEO_POSITION | varchar2 | 1 | 0 | NOTNULL |
| 146 | GVT_ANNUIT_COM_DT | date | 19 | 0 | NULL |
| 147 | GVT_BASIC_LIFE_RED | varchar2 | 2 | 0 | NOTNULL |
| 148 | GVT_DED_PRORT_DT | date | 19 | 0 | NULL |
| 149 | GVT_FEGLI_BASC_PCT | number(p,s) | 7 | 6 | NOTNULL |
| 150 | GVT_FEGLI_OPT_PCT | number(p,s) | 7 | 6 | NOTNULL |
| 151 | GVT_FEHB_PCT | number(p,s) | 7 | 6 | NOTNULL |
| 152 | GVT_RETRO_FLAG | varchar2 | 1 | 0 | NOTNULL |
| 153 | GVT_RETRO_DED_FLAG | varchar2 | 1 | 0 | NOTNULL |
| 154 | GVT_RETRO_JOB_FLAG | varchar2 | 1 | 0 | NOTNULL |
| 155 | GVT_RETRO_BSE_FLAG | varchar2 | 1 | 0 | NOTNULL |
| 156 | GVT_OTH_PAY_CHG | varchar2 | 1 | 0 | NOTNULL |
| 157 | GVT_DETL_POSN_NBR | varchar2 | 8 | 0 | NOTNULL |
| 158 | ANNL_BEN_BASE_OVRD | varchar2 | 1 | 0 | NOTNULL |
| 159 | BENEFIT_PROGRAM | varchar2 | 3 | 0 | NOTNULL |
| 160 | UPDATE_PAYROLL | varchar2 | 1 | 0 | NOTNULL |
| 161 | GVT_PAY_PLAN | varchar2 | 2 | 0 | NOTNULL |
| 162 | GVT_PAY_FLAG | varchar2 | 1 | 0 | NOTNULL |
| 163 | GVT_NID_CHANGE | varchar2 | 1 | 0 | NOTNULL |
| 164 | UNION_FULL_PART | varchar2 | 1 | 0 | NOTNULL |
| 165 | UNION_POS | varchar2 | 1 | 0 | NOTNULL |
| 166 | MATRICULA_NBR | number(p,s) | 38 | 0 | NOTNULL |
| 167 | SOC_SEC_RISK_CODE | varchar2 | 3 | 0 | NOTNULL |
| 168 | UNION_FEE_AMOUNT | number(p,s) | 8 | 2 | NOTNULL |
| 169 | UNION_FEE_START_DT | date | 19 | 0 | NULL |
| 170 | UNION_FEE_END_DT | date | 19 | 0 | NULL |
| 171 | EXEMPT_JOB_LBR | varchar2 | 1 | 0 | NOTNULL |
| 172 | EXEMPT_HOURS_MONTH | number(p,s) | 38 | 0 | NOTNULL |
| 173 | WRKS_CNCL_FUNCTION | varchar2 | 1 | 0 | NOTNULL |
| 174 | INTERCTR_WRKS_CNCL | varchar2 | 1 | 0 | NOTNULL |
| 175 | CURRENCY_CD1 | varchar2 | 3 | 0 | NOTNULL |
| 176 | PAY_UNION_FEE | varchar2 | 1 | 0 | NOTNULL |
| 177 | UNION_CD | varchar2 | 3 | 0 | NOTNULL |
| 178 | BARG_UNIT | varchar2 | 4 | 0 | NOTNULL |
| 179 | UNION_SENIORITY_DT | date | 19 | 0 | NULL |
| 180 | ENTRY_DATE | date | 19 | 0 | NULL |
| 181 | LABOR_AGREEMENT | varchar2 | 6 | 0 | NOTNULL |
| 182 | EMPL_CTG | varchar2 | 6 | 0 | NOTNULL |
| 183 | EMPL_CTG_L1 | varchar2 | 6 | 0 | NOTNULL |
| 184 | EMPL_CTG_L2 | varchar2 | 6 | 0 | NOTNULL |
| 185 | SETID_LBR_AGRMNT | varchar2 | 5 | 0 | NOTNULL |
| 186 | WPP_STOP_FLAG | varchar2 | 1 | 0 | NOTNULL |
| 187 | LABOR_FACILITY_ID | varchar2 | 10 | 0 | NOTNULL |
| 188 | LBR_FAC_ENTRY_DT | date | 19 | 0 | NULL |
| 189 | LAYOFF_EXEMPT_FLAG | varchar2 | 1 | 0 | NOTNULL |
| 190 | LAYOFF_EXEMPT_RSN | varchar2 | 11 | 0 | NOTNULL |
| 191 | GP_PAYGROUP | varchar2 | 10 | 0 | NOTNULL |
| 192 | GP_DFLT_ELIG_GRP | varchar2 | 1 | 0 | NOTNULL |
| 193 | GP_ELIG_GRP | varchar2 | 10 | 0 | NOTNULL |
| 194 | GP_DFLT_CURRTTYP | varchar2 | 1 | 0 | NOTNULL |
| 195 | CUR_RT_TYPE | varchar2 | 5 | 0 | NOTNULL |
| 196 | GP_DFLT_EXRTDT | varchar2 | 1 | 0 | NOTNULL |
| 197 | GP_ASOF_DT_EXG_RT | varchar2 | 1 | 0 | NOTNULL |
| 198 | ADDS_TO_FTE_ACTUAL | varchar2 | 1 | 0 | NOTNULL |
| 199 | CLASS_INDC | varchar2 | 1 | 0 | NOTNULL |
| 200 | ENCUMB_OVERRIDE | varchar2 | 1 | 0 | NOTNULL |
| 201 | FICA_STATUS_EE | varchar2 | 1 | 0 | NOTNULL |
| 202 | FTE | number(p,s) | 7 | 6 | NOTNULL |
| 203 | PRORATE_CNT_AMT | varchar2 | 1 | 0 | NOTNULL |
| 204 | PAY_SYSTEM_FLG | varchar2 | 2 | 0 | NOTNULL |
| 205 | BORDER_WALKER | varchar2 | 1 | 0 | NOTNULL |
| 206 | LUMP_SUM_PAY | varchar2 | 1 | 0 | NOTNULL |
| 207 | CONTRACT_NUM | varchar2 | 25 | 0 | NOTNULL |
| 208 | JOB_INDICATOR | varchar2 | 1 | 0 | NOTNULL |
| 209 | WRKS_CNCL_ROLE_CHE | varchar2 | 30 | 0 | NOTNULL |
| 210 | BENEFIT_SYSTEM | varchar2 | 2 | 0 | NOTNULL |
| 211 | WORK_DAY_HOURS | number(p,s) | 6 | 2 | NOTNULL |
| 212 | SUPERVISOR_ID | varchar2 | 11 | 0 | NOTNULL |
| 213 | REPORTS_TO | varchar2 | 8 | 0 | NOTNULL |
| 214 | ESTABID | varchar2 | 12 | 0 | NOTNULL |
| 215 | HE_NOA_EXT | varchar2 | 1 | 0 | NOTNULL |
| 216 | HE_AL_CARRYOVER | number(p,s) | 6 | 2 | NOTNULL |
| 217 | HE_AL_ACCRUAL | number(p,s) | 5 | 2 | NOTNULL |
| 218 | HE_AL_RED_CRED | number(p,s) | 5 | 2 | NOTNULL |
| 219 | HE_AL_TOTAL | number(p,s) | 5 | 2 | NOTNULL |
| 220 | HE_AL_BALANCE | number(p,s) | 6 | 2 | NOTNULL |
| 221 | HE_SL_CARRYOVER | number(p,s) | 6 | 2 | NOTNULL |
| 222 | HE_SL_ACCRUAL | number(p,s) | 5 | 2 | NOTNULL |
| 223 | HE_SL_RED_CRED | number(p,s) | 5 | 2 | NOTNULL |
| 224 | HE_SL_TOTAL | number(p,s) | 6 | 2 | NOTNULL |
| 225 | HE_SL_BALANCE | number(p,s) | 6 | 2 | NOTNULL |
| 226 | HE_RES_LASTYR | number(p,s) | 6 | 2 | NOTNULL |
| 227 | HE_RES_TWOYRS | number(p,s) | 6 | 2 | NOTNULL |
| 228 | HE_RES_THREEYRS | number(p,s) | 6 | 2 | NOTNULL |
| 229 | HE_RES_BALANCE | number(p,s) | 6 | 2 | NOTNULL |
| 230 | HE_LUMP_HRS | number(p,s) | 38 | 0 | NOTNULL |
| 231 | HE_AWOP_SEP | number(p,s) | 6 | 2 | NOTNULL |
| 232 | HE_AWOP_WIGI | number(p,s) | 6 | 2 | NOTNULL |
| 233 | HE_REG_MILITARY | number(p,s) | 38 | 0 | NOTNULL |
| 234 | HE_SPC_MILITARY | number(p,s) | 38 | 0 | NOTNULL |
| 235 | HE_FROZEN_SL | number(p,s) | 6 | 2 | NOTNULL |
| 236 | HE_TSPA_PR_YR | number(p,s) | 6 | 2 | NOTNULL |
| 237 | HE_TSPA_SUB_YR | number(p,s) | 7 | 2 | NOTNULL |
| 238 | HE_UNOFF_AL | number(p,s) | 38 | 0 | NOTNULL |
| 239 | HE_UNOFF_SL | number(p,s) | 38 | 0 | NOTNULL |
| 240 | HE_TLTR_NO | number(p,s) | 38 | 0 | NOTNULL |
| 241 | HE_UDED_PAY_CD | varchar2 | 1 | 0 | NOTNULL |
| 242 | HE_TSP_CANC_CD | varchar2 | 1 | 0 | NOTNULL |
| 243 | HE_PP_UDED_AMT | number(p,s) | 9 | 2 | NOTNULL |
| 244 | HE_EMP_UDED_AMT | number(p,s) | 9 | 2 | NOTNULL |
| 245 | HE_GVT_UDED_AMT | number(p,s) | 9 | 2 | NOTNULL |
| 246 | HE_NO_TSP_PAYPER | number(p,s) | 38 | 0 | NOTNULL |

---

## 3. Targets

### 3.1. EHRP_RECS_TRACKING_TBL

- **Database Type**: Oracle
- **Owner**: 
- **Field Count**: 10

| # | Field Name | Datatype | Precision | Scale | Nullable | Key Type |
|---|-----------|----------|-----------|-------|----------|----------|
| 1 | EMPLID | varchar2 | 8 | 0 | NULL | NOT A KEY |
| 2 | EMPL_RCD | number(p,s) | 2 | 0 | NULL | NOT A KEY |
| 3 | EFFDT | date | 19 | 0 | NULL | NOT A KEY |
| 4 | EFFSEQ | number(p,s) | 3 | 0 | NULL | NOT A KEY |
| 5 | EVENT_SUBMITTED_DT | date | 19 | 0 | NULL | NOT A KEY |
| 6 | GVT_WIP_STATUS | varchar2 | 3 | 0 | NULL | NOT A KEY |
| 7 | NOA_CD | varchar2 | 3 | 0 | NULL | NOT A KEY |
| 8 | NOA_SUFFIX_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 9 | BIIS_EVENT_ID | number(p,s) | 10 | 0 | NOTNULL | NOT A KEY |
| 10 | LOAD_DATE | date | 19 | 0 | NULL | NOT A KEY |

### 3.2. NWK_ACTION_PRIMARY_TBL

- **Database Type**: Oracle
- **Owner**: 
- **Field Count**: 260

| # | Field Name | Datatype | Precision | Scale | Nullable | Key Type |
|---|-----------|----------|-----------|-------|----------|----------|
| 1 | EVENT_ID | number(p,s) | 10 | 0 | NOTNULL | NOT A KEY |
| 2 | AGCY_ASSIGN_CD | varchar2 | 4 | 0 | NULL | NOT A KEY |
| 3 | AGCY_SUBELEMENT_CD | varchar2 | 15 | 0 | NULL | NOT A KEY |
| 4 | AGCY_SUBELEMENT_PRIOR_CD | varchar2 | 15 | 0 | NULL | NOT A KEY |
| 5 | ANN_LV_45DAY_CEIL_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 6 | ANN_LV_CATGRY_CD | number(p,s) | 1 | 0 | NULL | NOT A KEY |
| 7 | ANN_LV_CRDT_REDUCTN_HRS | number(p,s) | 5 | 2 | NULL | NOT A KEY |
| 8 | ANN_LV_CUR_BAL_HRS | number(p,s) | 6 | 0 | NULL | NOT A KEY |
| 9 | ANN_LV_LUMP_SUM_PAID_HRS | number(p,s) | 4 | 0 | NULL | NOT A KEY |
| 10 | ANN_LV_PRIOR_YEAR_BAL_HRS | number(p,s) | 6 | 2 | NULL | NOT A KEY |
| 11 | ANN_LV_RESTORED_BAL_LV_HRS | number(p,s) | 6 | 2 | NULL | NOT A KEY |
| 12 | ANN_LV_RESTORED_BAL1_HRS | number(p,s) | 6 | 2 | NULL | NOT A KEY |
| 13 | ANN_LV_RESTORED_BAL2_HRS | number(p,s) | 6 | 2 | NULL | NOT A KEY |
| 14 | ANN_LV_RESTORED_BAL3_HRS | number(p,s) | 6 | 2 | NULL | NOT A KEY |
| 15 | ANN_LV_TRANFR_IN_BAL_HRS | number(p,s) | 4 | 0 | NULL | NOT A KEY |
| 16 | ANN_LV_YTD_ACCRD_HRS | number(p,s) | 5 | 2 | NULL | NOT A KEY |
| 17 | ANN_LV_YTD_USED_HRS | number(p,s) | 5 | 2 | NULL | NOT A KEY |
| 18 | ANN_SALARY_RATE_AMT | number(p,s) | 14 | 4 | NULL | NOT A KEY |
| 19 | ANNUITANT_IND_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 20 | APPOINTING_OFFICE_CHANGE_CD | varchar2 | 3 | 0 | NULL | NOT A KEY |
| 21 | APPT_LMT_NTE_90DAY_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 22 | APPT_LMT_NTE_90DAY_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 23 | APPT_LMT_NTE_HRS | number(p,s) | 10 | 0 | NULL | NOT A KEY |
| 24 | APPT_NTE_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 25 | APPT_STATUS_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 26 | APPT_TYPE_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 27 | AUO_PAY_PCT | number(p,s) | 15 | 4 | NULL | NOT A KEY |
| 28 | AUO_PAY_PRIOR_PCT | number(p,s) | 15 | 4 | NULL | NOT A KEY |
| 29 | AUTHENTICATION_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 30 | AVAILABILITY_PAY_PCT | number(p,s) | 15 | 4 | NULL | NOT A KEY |
| 31 | AWOP_WGI_START_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 32 | AWOP_YTD_HRS | number(p,s) | 6 | 2 | NULL | NOT A KEY |
| 33 | BARGAINING_UNIT_CD | varchar2 | 15 | 0 | NULL | NOT A KEY |
| 34 | BASE_HRS | number(p,s) | 5 | 2 | NULL | NOT A KEY |
| 35 | BIRTH_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 36 | CAN_CD | varchar2 | 15 | 0 | NULL | NOT A KEY |
| 37 | CAN_NEW_CD | varchar2 | 15 | 0 | NULL | NOT A KEY |
| 38 | CAREER_START_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 39 | CASH_AWARD_AMT | number(p,s) | 15 | 4 | NULL | NOT A KEY |
| 40 | CASH_AWARD_BNFT_AMT | number(p,s) | 10 | 2 | NULL | NOT A KEY |
| 41 | CEIL_REPORTING_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 42 | CHRTY_AREA_CD | varchar2 | 10 | 0 | NULL | NOT A KEY |
| 43 | CHRTY_DED_AMT | number(p,s) | 14 | 4 | NULL | NOT A KEY |
| 44 | CITY_TAX_CD | varchar2 | 10 | 0 | NULL | NOT A KEY |
| 45 | CITY_TAX_DED_AMT | number(p,s) | 5 | 2 | NULL | NOT A KEY |
| 46 | CITY_TAX_DED_PCT | number(p,s) | 6 | 3 | NULL | NOT A KEY |
| 47 | CITY_TAX_MARTL_STATUS_CD | varchar2 | 17 | 0 | NULL | NOT A KEY |
| 48 | CITY_TAX_RESID_CD | varchar2 | 17 | 0 | NULL | NOT A KEY |
| 49 | CITY_TAX_TOT_EXEMPT_COUNT | number(p,s) | 15 | 4 | NULL | NOT A KEY |
| 50 | CNTY_TAX_CD | varchar2 | 10 | 0 | NULL | NOT A KEY |
| 51 | CNTY_TAX_DED_PCT | number(p,s) | 6 | 3 | NULL | NOT A KEY |
| 52 | CNTY_TAX_MARTL_STATUS_CD | varchar2 | 17 | 0 | NULL | NOT A KEY |
| 53 | CNTY_TAX_RESID_CD | varchar2 | 17 | 0 | NULL | NOT A KEY |
| 54 | CNTY_TAX_TOT_EXEMPT_COUNT | number(p,s) | 15 | 4 | NULL | NOT A KEY |
| 55 | COMPETITIVE_LEVEL_CD | varchar2 | 4 | 0 | NULL | NOT A KEY |
| 56 | COMPUTER_POSITION_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 57 | CORR_CANCL_EFF_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 58 | CORR_CANCL_LEGAL_AUTH_CD | varchar2 | 3 | 0 | NULL | NOT A KEY |
| 59 | CORR_CANCL_LEGAL_AUTH_TXT | varchar2 | 250 | 0 | NULL | NOT A KEY |
| 60 | CORR_CANCL_LEGAL_AUTH2_CD | varchar2 | 3 | 0 | NULL | NOT A KEY |
| 61 | CORR_CANCL_LEGAL_AUTH2_TXT | varchar2 | 250 | 0 | NULL | NOT A KEY |
| 62 | CORR_CANCL_NOA_CD | varchar2 | 3 | 0 | NULL | NOT A KEY |
| 63 | CORR_CANCL_NOA_SUFFIX_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 64 | CRDTBL_MIL_SRVC_PERIOD | varchar2 | 8 | 0 | NULL | NOT A KEY |
| 65 | DETAIL_TIMEKEEPER_NUM | varchar2 | 10 | 0 | NULL | NOT A KEY |
| 66 | DUTY_STATION_CD | varchar2 | 15 | 0 | NULL | NOT A KEY |
| 67 | DUTY_STATION_PRIOR_CD | varchar2 | 15 | 0 | NULL | NOT A KEY |
| 68 | EDUCATION_ALLOWC_AMT | number(p,s) | 7 | 2 | NULL | NOT A KEY |
| 69 | EDUCATION_LEVEL_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 70 | EMP_EOD_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 71 | EMP_FIRST_NAME | varchar2 | 40 | 0 | NULL | NOT A KEY |
| 72 | EMP_GRADE_START_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 73 | EMP_LAST_NAME | varchar2 | 40 | 0 | NULL | NOT A KEY |
| 74 | EMP_MID_INIT | varchar2 | 40 | 0 | NULL | NOT A KEY |
| 75 | EMP_PREV_FIRST_NAME | varchar2 | 40 | 0 | NULL | NOT A KEY |
| 76 | EMP_PREV_LAST_NAME | varchar2 | 40 | 0 | NULL | NOT A KEY |
| 77 | EMP_PREV_MID_INIT | varchar2 | 40 | 0 | NULL | NOT A KEY |
| 78 | EMP_RESID_CITY_ST_NAME | varchar2 | 60 | 0 | NULL | NOT A KEY |
| 79 | EMP_RESID_GEOGPHCL_LOC_CD | varchar2 | 10 | 0 | NULL | NOT A KEY |
| 80 | EMP_RESID_POSTAL_CD | varchar2 | 12 | 0 | NULL | NOT A KEY |
| 81 | EMP_RESID_STREET_NAME | varchar2 | 40 | 0 | NULL | NOT A KEY |
| 82 | EVENT_EFF_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 83 | EVENT_SUBMITTED_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 84 | FED_TAX_EIC_CD | varchar2 | 17 | 0 | NULL | NOT A KEY |
| 85 | FED_TAX_EXEMPT_CD | varchar2 | 17 | 0 | NULL | NOT A KEY |
| 86 | FED_TAX_MARTL_STATUS_CD | varchar2 | 17 | 0 | NULL | NOT A KEY |
| 87 | FED_TAX_MEDICARE_NON_RESID_CD | varchar2 | 17 | 0 | NULL | NOT A KEY |
| 88 | FED_TAX_OPT_DED_AMT | number(p,s) | 15 | 4 | NULL | NOT A KEY |
| 89 | FED_TAX_TOT_EXEMPT_COUNT | number(p,s) | 15 | 4 | NULL | NOT A KEY |
| 90 | FEGLI_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 91 | FEGLI_EFF_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 92 | FEGLI_LIVING_BNFT_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 93 | FEGLI_LIVING_BNFT_REMAIN_AMT | number(p,s) | 14 | 4 | NULL | NOT A KEY |
| 94 | FEHB_DED_AMT | number(p,s) | 15 | 4 | NULL | NOT A KEY |
| 95 | FEHB_GOVT_CONTB_AMT | number(p,s) | 15 | 4 | NULL | NOT A KEY |
| 96 | FEHB_PLAN_CD | varchar2 | 4 | 0 | NULL | NOT A KEY |
| 97 | FEHB_PREV_PLAN_CD | varchar2 | 4 | 0 | NULL | NOT A KEY |
| 98 | FERS_CONV_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 99 | FERS_COV_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 100 | FERS_DISABILITY_SSA_BNFT_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 101 | FERS_SSA_BNFT_AMT | number(p,s) | 4 | 0 | NULL | NOT A KEY |
| 102 | FILLING_POSITION_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 103 | FLSA_CATGRY_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 104 | FRINGE_BNFT_AMT | number(p,s) | 5 | 0 | NULL | NOT A KEY |
| 105 | FROZEN_SERVICE_PERIOD | varchar2 | 4 | 0 | NULL | NOT A KEY |
| 106 | FUNCTNL_CLASSFCTN_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 107 | FUNCTNL_CLASSFCTN_PRIOR_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 108 | GRADE_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 109 | GRADE_PRIOR_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 110 | HANDICAP_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 111 | HRLY_RATE_AMT | number(p,s) | 14 | 4 | NULL | NOT A KEY |
| 112 | INDIAN_PREFERENCE_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 113 | INSTRUCTIONAL_PROGRAM_CD | varchar2 | 6 | 0 | NULL | NOT A KEY |
| 114 | LEGAL_AUTH_CD | varchar2 | 3 | 0 | NULL | NOT A KEY |
| 115 | LEGAL_AUTH_TXT | varchar2 | 250 | 0 | NULL | NOT A KEY |
| 116 | LEGAL_AUTH2_CD | varchar2 | 3 | 0 | NULL | NOT A KEY |
| 117 | LEGAL_AUTH2_TXT | varchar2 | 250 | 0 | NULL | NOT A KEY |
| 118 | LOCALITY_PAY_AMT | number(p,s) | 10 | 0 | NULL | NOT A KEY |
| 119 | LOCALITY_PAY_PCT | number(p,s) | 15 | 4 | NULL | NOT A KEY |
| 120 | LOCALITY_PAY_PRIOR_AMT | number(p,s) | 10 | 0 | NULL | NOT A KEY |
| 121 | LV_REDUCTN_HRS | number(p,s) | 5 | 2 | NULL | NOT A KEY |
| 122 | LV_SCD_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 123 | LWOP_AWOP_WGI_HRS | number(p,s) | 6 | 2 | NULL | NOT A KEY |
| 124 | MEDICAL_OFFICER_IND_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 125 | MEDICAL_OFFICER_IND_PRIOR_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 126 | MSA_CD | varchar2 | 4 | 0 | NULL | NOT A KEY |
| 127 | NATL_UNION_DUES_BWKLY_DED_AMT | number(p,s) | 14 | 4 | NULL | NOT A KEY |
| 128 | NATL_UNION_DUES_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 129 | NATL_UNION_DUES_HRLY_RATE_AMT | number(p,s) | 15 | 4 | NULL | NOT A KEY |
| 130 | NOA_CD | varchar2 | 3 | 0 | NULL | NOT A KEY |
| 131 | NOA_SUFFIX_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 132 | OATH_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 133 | OCCUPATION_CD | varchar2 | 10 | 0 | NULL | NOT A KEY |
| 134 | OCCUPATION_PRIOR_CD | varchar2 | 10 | 0 | NULL | NOT A KEY |
| 135 | OCCUPATIONAL_CATGRY_CD | varchar2 | 10 | 0 | NULL | NOT A KEY |
| 136 | ORGTNL_COMPONENT_CD | varchar2 | 15 | 0 | NULL | NOT A KEY |
| 137 | ORGTNL_COMPONENT_PRIOR_CD | varchar2 | 15 | 0 | NULL | NOT A KEY |
| 138 | OT_RATE_AMT | number(p,s) | 5 | 2 | NULL | NOT A KEY |
| 139 | PAY_BASIS_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 140 | PAY_BASIS_PRIOR_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 141 | PAY_PLAN_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 142 | PAY_PLAN_PRIOR_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 143 | PCA_CATGRY_CD | varchar2 | 15 | 0 | NULL | NOT A KEY |
| 144 | PCA_CONTRACT_LENGTH_YRS | varchar2 | 15 | 0 | NULL | NOT A KEY |
| 145 | PCA_CONTRACT_NTE_AMT | number(p,s) | 14 | 4 | NULL | NOT A KEY |
| 146 | PCA_CONTRACT_NTE_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 147 | PERMANENT_TEMP_POSITION_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 148 | PERSONNEL_OFFICE_ID_CD | varchar2 | 15 | 0 | NULL | NOT A KEY |
| 149 | PERSONNEL_OFFICE_ID_DIFFRNT_CD | varchar2 | 15 | 0 | NULL | NOT A KEY |
| 150 | POSITION_CHANGE_END_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 151 | POSITION_NUM | varchar2 | 10 | 0 | NULL | NOT A KEY |
| 152 | POSITION_OCCUPIED_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 153 | POSITION_PRIOR_NUM | varchar2 | 10 | 0 | NULL | NOT A KEY |
| 154 | POSITION_SENSITIVITY_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 155 | POSITION_TITLE_NAME | varchar2 | 32 | 0 | NULL | NOT A KEY |
| 156 | POSITION_TITLE_PRIOR_NAME | varchar2 | 32 | 0 | NULL | NOT A KEY |
| 157 | PRD_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 158 | PRD_EFF_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 159 | PRD_PRIOR_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 160 | PREV_RETMT_COV_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 161 | PROB_TRIAL_PERIOD_START_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 162 | PSP_CONTRACT_EFF_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 163 | PSP_CONTRACT_LENGTH_YRS | number(p,s) | 1 | 0 | NULL | NOT A KEY |
| 164 | PSP_CONTRACT_NTE_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 165 | PSP_LENGTH_SRVC_INCREASE_AMT | number(p,s) | 7 | 0 | NULL | NOT A KEY |
| 166 | PSP_LENGTH_SRVC_CHANGE_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 167 | PSP_TOT_AMT | number(p,s) | 14 | 4 | NULL | NOT A KEY |
| 168 | QUARTERS_DED_AMT | number(p,s) | 14 | 4 | NULL | NOT A KEY |
| 169 | RACE_NATL_ORIGIN_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 170 | RATING_RECORD_PATTERN_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 171 | REEMP_ANNUITANT_HRLY_RATE_AMT | number(p,s) | 14 | 4 | NULL | NOT A KEY |
| 172 | REEMP_ANNUITANT_MONTHLY_AMT | number(p,s) | 15 | 4 | NULL | NOT A KEY |
| 173 | REGION_OPDIV_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 174 | RETMT_PLAN_CD | varchar2 | 4 | 0 | NULL | NOT A KEY |
| 175 | SCHLD_ANN_SALARY_AMT | number(p,s) | 15 | 4 | NULL | NOT A KEY |
| 176 | SCHLD_ANN_SALARY_PRIOR_AMT | number(p,s) | 14 | 4 | NULL | NOT A KEY |
| 177 | SCHLD_HRLY_RATE_AMT | number(p,s) | 14 | 4 | NULL | NOT A KEY |
| 178 | SES_POSITION_LEAVING_REASON_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 179 | SES_SABBATICAL_NTE_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 180 | SEVERANCE_PAY_AMT | number(p,s) | 7 | 2 | NULL | NOT A KEY |
| 181 | SEVERANCE_PAY_FINAL_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 182 | SEVERANCE_PAY_START_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 183 | SEVERANCE_PAY_TOT_AMT | number(p,s) | 8 | 2 | NULL | NOT A KEY |
| 184 | SEVERANCE_PAY_WEEKS_COUNT | number(p,s) | 2 | 0 | NULL | NOT A KEY |
| 185 | SEX_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 186 | SICK_LV_CRDT_REDUCTN_HRS | number(p,s) | 5 | 2 | NULL | NOT A KEY |
| 187 | SICK_LV_CUR_BAL_HRS | number(p,s) | 6 | 0 | NULL | NOT A KEY |
| 188 | SICK_LV_FERS_ELECT_BAL_HRS | number(p,s) | 8 | 2 | NULL | NOT A KEY |
| 189 | SICK_LV_PRIOR_YEAR_BAL_HRS | number(p,s) | 6 | 2 | NULL | NOT A KEY |
| 190 | SICK_LV_TRANFR_IN_BAL_HRS | number(p,s) | 4 | 0 | NULL | NOT A KEY |
| 191 | SICK_LV_YTD_ACCRD_HRS | number(p,s) | 5 | 2 | NULL | NOT A KEY |
| 192 | SICK_LV_YTD_USED_HRS | number(p,s) | 6 | 2 | NULL | NOT A KEY |
| 193 | SPECIAL_PAY_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 194 | SPECIAL_PAY_REASON_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 195 | SPECIAL_PROGRAM_CD | number(p,s) | 2 | 0 | NULL | NOT A KEY |
| 196 | SSN | varchar2 | 20 | 0 | NULL | NOT A KEY |
| 197 | ST_TAX_CD | varchar2 | 10 | 0 | NULL | NOT A KEY |
| 198 | ST_TAX_MARTL_STATUS_CD | varchar2 | 17 | 0 | NULL | NOT A KEY |
| 199 | ST_TAX_NON_RESID_CD | varchar2 | 17 | 0 | NULL | NOT A KEY |
| 200 | ST_TAX_OPT_DED_AMT | number(p,s) | 15 | 4 | NULL | NOT A KEY |
| 201 | ST_TAX_TOT_EXEMPT_COUNT | number(p,s) | 15 | 4 | NULL | NOT A KEY |
| 202 | STEP_CD | varchar2 | 15 | 0 | NULL | NOT A KEY |
| 203 | STEP_PRIOR_CD | varchar2 | 15 | 0 | NULL | NOT A KEY |
| 204 | SUPERVSRY_MGRL_PROB_START_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 205 | SUPERVSRY_STATUS_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 206 | SUSPENSION_END_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 207 | TEMP_PROMTN_EXP_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 208 | TENURE_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 209 | TERMINAL_SITE_CD | number(p,s) | 2 | 0 | NULL | NOT A KEY |
| 210 | TERMINAL_SITE_DIFFRNT_CD | number(p,s) | 2 | 0 | NULL | NOT A KEY |
| 211 | TIMEKEEPER_NUM | varchar2 | 10 | 0 | NULL | NOT A KEY |
| 212 | TSTG_DESIGNTD_POSITION_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 213 | TSTG_DESIGNTD_POSITION_EFF_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 214 | UNION_CD | varchar2 | 15 | 0 | NULL | NOT A KEY |
| 215 | UNION_DED_AMT | number(p,s) | 14 | 4 | NULL | NOT A KEY |
| 216 | UNION_DED_TYPE_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 217 | US_CITIZENSHIP_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 218 | VETERANS_PREFERENCE_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 219 | VETERANS_STATUS_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 220 | VLNTRY_SEP_INCENTIVE_PYMT_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 221 | WAGE_BOARD_SHIFT2_AMT | number(p,s) | 4 | 2 | NULL | NOT A KEY |
| 222 | WAGE_BOARD_SHIFT3_AMT | number(p,s) | 4 | 2 | NULL | NOT A KEY |
| 223 | WGI_START_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 224 | WGI_STATUS_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 225 | WORK_SCHEDULE_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 226 | WORK_SCHEDULE_CHANGE_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 227 | WORK_SCHEDULE_PRIOR_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 228 | YEAR_DEGREE_ATTAINED_DTE | number(p,s) | 4 | 0 | NULL | NOT A KEY |
| 229 | AS_OF_DAY | varchar2 | 10 | 0 | NULL | NOT A KEY |
| 230 | LOAD_ID | varchar2 | 10 | 0 | NOTNULL | NOT A KEY |
| 231 | HB_PRETAX_CODE | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 232 | HB_PRETAX_PP_DED | number(p,s) | 7 | 2 | NULL | NOT A KEY |
| 233 | HB_PRETAX_TOTAL_PREPAID | number(p,s) | 7 | 2 | NULL | NOT A KEY |
| 234 | EMP_ID | varchar2 | 8 | 0 | NULL | NOT A KEY |
| 235 | LOAD_DATE | date | 19 | 0 | NULL | NOT A KEY |
| 236 | NEW_SSN | varchar2 | 9 | 0 | NULL | NOT A KEY |
| 237 | OLD_SSN | varchar2 | 9 | 0 | NULL | NOT A KEY |
| 238 | EFFSEQ | number(p,s) | 3 | 0 | NULL | NOT A KEY |
| 239 | EMPL_REC_NO | number(p,s) | 2 | 0 | NULL | NOT A KEY |
| 240 | EHRP_TYPE_ACTION | varchar2 | 3 | 0 | NULL | NOT A KEY |
| 241 | EHRP_ACTION_REASON | varchar2 | 3 | 0 | NULL | NOT A KEY |
| 242 | GVT_WIP_STATUS | varchar2 | 3 | 0 | NULL | NOT A KEY |
| 243 | GVT_STATUS_TYPE | varchar2 | 3 | 0 | NULL | NOT A KEY |
| 244 | EHRP_POSITION_NUMBER | varchar2 | 8 | 0 | NULL | NOT A KEY |
| 245 | EHRP_POSITION_PRIOR_NUMBER | varchar2 | 8 | 0 | NULL | NOT A KEY |
| 246 | CORR_CANCL_EFFSEQ | number(p,s) | 3 | 0 | NULL | NOT A KEY |
| 247 | TITLE42_IND | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 248 | TITLE38_IND | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 249 | OPDIV | varchar2 | 8 | 0 | NULL | NOT A KEY |
| 250 | OPDIV_PRIOR | varchar2 | 8 | 0 | NULL | NOT A KEY |
| 251 | SETID | varchar2 | 5 | 0 | NULL | NOT A KEY |
| 252 | CURR_APPT_AUTH1_CD | varchar2 | 3 | 0 | NULL | NOT A KEY |
| 253 | CURR_APPT_AUTH2_CD | varchar2 | 3 | 0 | NULL | NOT A KEY |
| 254 | LEO_POSITION_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 255 | TEMP_GVT_EFFDT | date | 19 | 0 | NULL | NOT A KEY |
| 256 | REPORTS_TO | varchar2 | 8 | 0 | NULL | NOT A KEY |
| 257 | POSITION_ENTRY_DT | date | 19 | 0 | NULL | NOT A KEY |
| 258 | WGI_DUE_DT | date | 19 | 0 | NULL | NOT A KEY |
| 259 | EHRP_CHANGED_WIP_STATUS_DT | date | 19 | 0 | NULL | NOT A KEY |
| 260 | BIIS_CHANGED_WIP_STATUS_DT | date | 19 | 0 | NULL | NOT A KEY |

### 3.3. NWK_ACTION_SECONDARY_TBL

- **Database Type**: Oracle
- **Owner**: 
- **Field Count**: 209

| # | Field Name | Datatype | Precision | Scale | Nullable | Key Type |
|---|-----------|----------|-----------|-------|----------|----------|
| 1 | EVENT_ID | number(p,s) | 10 | 0 | NOTNULL | NOT A KEY |
| 2 | ADTNL_OPT_LIFE_INS_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 3 | ALLOT_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 4 | ALLOT_DED_AMT | number(p,s) | 8 | 2 | NULL | NOT A KEY |
| 5 | ALLOT_EFF_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 6 | ALLOT_EMP_ACCT_NUM | varchar2 | 17 | 0 | NULL | NOT A KEY |
| 7 | ALLOT_EMP_ACCT_TYPE_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 8 | ALLOT_FINCL_INSTN_CITY_ST_NAME | varchar2 | 60 | 0 | NULL | NOT A KEY |
| 9 | ALLOT_FINCL_INSTN_NAME | varchar2 | 43 | 0 | NULL | NOT A KEY |
| 10 | ALLOT_FINCL_INSTN_NUM | varchar2 | 10 | 0 | NULL | NOT A KEY |
| 11 | ALLOT_FINCL_INSTN_POSTAL_CD | varchar2 | 12 | 0 | NULL | NOT A KEY |
| 12 | ALLOT_FINCL_INSTN_STREET_NAME | varchar2 | 40 | 0 | NULL | NOT A KEY |
| 13 | BOND_COBEN_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 14 | BOND_COBEN_NAME | varchar2 | 120 | 0 | NULL | NOT A KEY |
| 15 | BOND_COBEN_SSN | varchar2 | 20 | 0 | NULL | NOT A KEY |
| 16 | BOND_DENOM_CD | varchar2 | 15 | 0 | NULL | NOT A KEY |
| 17 | BOND_INSCRIPTION_NUM | varchar2 | 15 | 0 | NULL | NOT A KEY |
| 18 | BOND_NOTE_TOT_DED_AMT | number(p,s) | 7 | 2 | NULL | NOT A KEY |
| 19 | BOND_OWNER_CITY_NAME | varchar2 | 30 | 0 | NULL | NOT A KEY |
| 20 | BOND_OWNER_NAME | varchar2 | 120 | 0 | NULL | NOT A KEY |
| 21 | BOND_OWNER_POSTAL_CD | varchar2 | 12 | 0 | NULL | NOT A KEY |
| 22 | BOND_OWNER_SSN | varchar2 | 20 | 0 | NULL | NOT A KEY |
| 23 | BOND_OWNER_ST_NAME | varchar2 | 30 | 0 | NULL | NOT A KEY |
| 24 | BOND_OWNER_STREET_NAME | varchar2 | 40 | 0 | NULL | NOT A KEY |
| 25 | BOND_REFUND_IND | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 26 | BUYOUT_AMT | number(p,s) | 7 | 2 | NULL | NOT A KEY |
| 27 | BUYOUT_EFF_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 28 | CLAIM_TYPE_CD | number(p,s) | 1 | 0 | NULL | NOT A KEY |
| 29 | COLA_PCT | number(p,s) | 8 | 2 | NULL | NOT A KEY |
| 30 | COURT_ORDER_APPLCTN_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 31 | CSA_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 32 | CSA_DED_AMT | number(p,s) | 8 | 2 | NULL | NOT A KEY |
| 33 | CSA_DED_PCT | number(p,s) | 8 | 2 | NULL | NOT A KEY |
| 34 | CSA_RECIPIENT_NAME | varchar2 | 120 | 0 | NULL | NOT A KEY |
| 35 | CSRS_ADMIN_FEE_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 36 | ENVIRN_DIF_PCT | number(p,s) | 8 | 2 | NULL | NOT A KEY |
| 37 | FAMILY_LIFE_INS_CD | number(p,s) | 1 | 0 | NULL | NOT A KEY |
| 38 | FORGN_LANG_AWARD_AMT | number(p,s) | 5 | 0 | NULL | NOT A KEY |
| 39 | FORGN_LANG_AWARD_SEP_AMT | number(p,s) | 5 | 0 | NULL | NOT A KEY |
| 40 | FORGN_POST_ALLOWC_TOT_AMT | number(p,s) | 7 | 2 | NULL | NOT A KEY |
| 41 | FORGN_POST_DIF_PAY_PCT | number(p,s) | 6 | 3 | NULL | NOT A KEY |
| 42 | FURLOUGH_EXP_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 43 | HAZARD_DUTY_DIF_PCT | number(p,s) | 8 | 2 | NULL | NOT A KEY |
| 44 | INDVDL_GROUP_AWARD | varchar2 | 4 | 0 | NULL | NOT A KEY |
| 45 | LAUNDRY_DED_AMT | number(p,s) | 7 | 2 | NULL | NOT A KEY |
| 46 | LIFE_INS_AMT | number(p,s) | 3 | 0 | NULL | NOT A KEY |
| 47 | LIFE_INS_REDUCTN_CD | number(p,s) | 1 | 0 | NULL | NOT A KEY |
| 48 | LIVING_QUARTERS_ALLOWC_TOT_AMT | number(p,s) | 7 | 2 | NULL | NOT A KEY |
| 49 | LWOP_EXP_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 50 | LWP_EXP_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 51 | MERIT_PAY_AGGREGATE_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 52 | MERIT_PAY_CERTIFICATE_IND | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 53 | MERIT_PAY_CRITICAL_ELEMENT_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 54 | MERIT_PAY_GRADE_CD | number(p,s) | 2 | 0 | NULL | NOT A KEY |
| 55 | MERIT_PAY_HRLY_RATE_AMT | number(p,s) | 4 | 2 | NULL | NOT A KEY |
| 56 | MERIT_PAY_INCREASE_AMT | number(p,s) | 8 | 2 | NULL | NOT A KEY |
| 57 | MERIT_PAY_PAID_OUT_IND | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 58 | MERIT_PAY_PERF_PLAN_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 59 | MERIT_PAY_PERF_PLAN_END_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 60 | MERIT_PAY_PERF_SCORE_NUM | number(p,s) | 3 | 0 | NULL | NOT A KEY |
| 61 | MERIT_PAY_POOL_DESIGNATOR_CD | varchar2 | 5 | 0 | NULL | NOT A KEY |
| 62 | MERIT_PAY_PRD_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 63 | MERIT_PAY_SALARY_AMT | number(p,s) | 8 | 2 | NULL | NOT A KEY |
| 64 | MERIT_PAY_SCALE_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 65 | MERIT_PAY_STEP_CD | number(p,s) | 2 | 0 | NULL | NOT A KEY |
| 66 | MIL_LV_CUR_FY_HRS | number(p,s) | 5 | 0 | NULL | NOT A KEY |
| 67 | MIL_LV_EMERG_CUR_FY_HRS | number(p,s) | 3 | 0 | NULL | NOT A KEY |
| 68 | MIL_RETIRED_PAY_RECPNT_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 69 | MIL_RETIRED_PAY_WAIVER_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 70 | MIL_RETMT_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 71 | MIL_RETMT_OFFSET_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 72 | MIL_RETMT_TOT_DED_AMT | number(p,s) | 7 | 2 | NULL | NOT A KEY |
| 73 | MIL_SRVC_ACHVD_RANK_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 74 | MIL_SRVC_BRANCH_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 75 | MIL_SRVC_BRANCH_COMPONENT_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 76 | NON_BAYLOR_BASE_HRLY_RATE_AMT | number(p,s) | 5 | 2 | NULL | NOT A KEY |
| 77 | NON_FORGN_POST_DIF_PAY_PCT | number(p,s) | 6 | 3 | NULL | NOT A KEY |
| 78 | NON_GOVT_RETMT_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 79 | OTHER_PAY_AMT | number(p,s) | 5 | 0 | NULL | NOT A KEY |
| 80 | OWCP_CUR_RECEIPT_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 81 | PAY_TABLE_NUM | varchar2 | 10 | 0 | NULL | NOT A KEY |
| 82 | PERF_END1_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 83 | PERF_END2_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 84 | PERF_RATING_RECORD_LEVEL_CD | varchar2 | 15 | 0 | NULL | NOT A KEY |
| 85 | POST56_MIL_SRVC_DEPST_PAID_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 86 | PREMIUM_PAY_PCT | number(p,s) | 8 | 2 | NULL | NOT A KEY |
| 87 | RECRUITMENT_BONUS_AMT | number(p,s) | 6 | 0 | NULL | NOT A KEY |
| 88 | RECRUITMENT_EXP_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 89 | REG_SCHLD_STANDBY_DUTY_PAY_PCT | number(p,s) | 8 | 2 | NULL | NOT A KEY |
| 90 | RELOCATION_BONUS_AMT | number(p,s) | 6 | 0 | NULL | NOT A KEY |
| 91 | RELOCATION_EXP_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 92 | RETENTION_BONUS_BWKLY_AMT | number(p,s) | 5 | 0 | NULL | NOT A KEY |
| 93 | RETENTION_BONUS_TOT_AMT | number(p,s) | 6 | 0 | NULL | NOT A KEY |
| 94 | RETMT_REFUND_RECEIVED_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 95 | RETMT_SCD_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 96 | RETMT_SURVIVOR_ELECT_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 97 | RETMT_TAX_WITH_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 98 | RETND1_EFF_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 99 | RETND1_EXP_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 100 | RETND1_GRADE_CD | varchar2 | 15 | 0 | NULL | NOT A KEY |
| 101 | RETND1_MEDICAL_OFFICER_IND_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 102 | RETND1_OCCUPATION_CD | varchar2 | 4 | 0 | NULL | NOT A KEY |
| 103 | RETND1_ORGTNL_COMPONENT_CD | varchar2 | 11 | 0 | NULL | NOT A KEY |
| 104 | RETND1_PAY_PLAN_CD | varchar2 | 15 | 0 | NULL | NOT A KEY |
| 105 | RETND1_STEP_CD | varchar2 | 15 | 0 | NULL | NOT A KEY |
| 106 | RETND2_EFF_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 107 | RETND2_EXP_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 108 | RETND2_GRADE_CD | varchar2 | 15 | 0 | NULL | NOT A KEY |
| 109 | RETND2_MEDICAL_OFFICER_IND_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 110 | RETND2_OCCUPATION_CD | varchar2 | 4 | 0 | NULL | NOT A KEY |
| 111 | RETND2_ORGTNL_COMPONENT_CD | varchar2 | 11 | 0 | NULL | NOT A KEY |
| 112 | RETND2_PAY_PLAN_CD | varchar2 | 15 | 0 | NULL | NOT A KEY |
| 113 | RETND2_STEP_CD | varchar2 | 15 | 0 | NULL | NOT A KEY |
| 114 | RIF_RELEASE_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 115 | RIF_SCD_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 116 | RIF_VETERANS_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 117 | SAVE_RATE_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 118 | SBRS_ADTNL_BWKLY_DED_AMT | number(p,s) | 8 | 2 | NULL | NOT A KEY |
| 119 | SBRS_ADTNL_BWKLY_REDUCTN_AMT | number(p,s) | 8 | 2 | NULL | NOT A KEY |
| 120 | SBRS_BWKLY_DED_AMT | number(p,s) | 8 | 2 | NULL | NOT A KEY |
| 121 | SBRS_BWKLY_REDUCTN_AMT | number(p,s) | 8 | 2 | NULL | NOT A KEY |
| 122 | SBRS_CONTRACT_NUM | varchar2 | 8 | 0 | NULL | NOT A KEY |
| 123 | SBRS_EMP_DED_PCT | number(p,s) | 3 | 3 | NULL | NOT A KEY |
| 124 | SBRS_EMP_OTHER_BWKLY_DED_AMT | number(p,s) | 8 | 2 | NULL | NOT A KEY |
| 125 | SBRS_EMP_OTHER_DED_PCT | number(p,s) | 3 | 3 | NULL | NOT A KEY |
| 126 | SBRS_EMP_REDUCTN_PCT | number(p,s) | 3 | 3 | NULL | NOT A KEY |
| 127 | SBRS_EMPLYR_OTHER_DED_PCT | number(p,s) | 3 | 3 | NULL | NOT A KEY |
| 128 | SBRS_EMPLYR_TIAA_CREF_DED_PCT | number(p,s) | 3 | 3 | NULL | NOT A KEY |
| 129 | SBRS_INSTN_CITY_NAME | varchar2 | 23 | 0 | NULL | NOT A KEY |
| 130 | SBRS_INSTN_NAME | varchar2 | 30 | 0 | NULL | NOT A KEY |
| 131 | SBRS_INSTN_POSTAL_CD | varchar2 | 12 | 0 | NULL | NOT A KEY |
| 132 | SBRS_INSTN_ST_NAME | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 133 | SBRS_INSTN_STREET1_NAME | varchar2 | 25 | 0 | NULL | NOT A KEY |
| 134 | SBRS_INSTN_STREET2_NAME | varchar2 | 25 | 0 | NULL | NOT A KEY |
| 135 | SBRS_SRA_BWKLY_REDUCTN_AMT | number(p,s) | 8 | 2 | NULL | NOT A KEY |
| 136 | SBRS_SRA_REDUCTN_PCT | number(p,s) | 3 | 3 | NULL | NOT A KEY |
| 137 | SEP_ACCT_TYPE_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 138 | SEP_EMP_ACCT_NUM | varchar2 | 17 | 0 | NULL | NOT A KEY |
| 139 | SEP_EMP_CITY_NAME | varchar2 | 30 | 0 | NULL | NOT A KEY |
| 140 | SEP_EMP_POSTAL_CD | varchar2 | 12 | 0 | NULL | NOT A KEY |
| 141 | SEP_EMP_ST_NAME | varchar2 | 30 | 0 | NULL | NOT A KEY |
| 142 | SEP_EMP_STREET_NAME | varchar2 | 40 | 0 | NULL | NOT A KEY |
| 143 | SEP_FED_TAX_MARTL_STATUS_CD | varchar2 | 17 | 0 | NULL | NOT A KEY |
| 144 | SEP_FED_TAX_TOT_EXEMPT_COUNT | number(p,s) | 8 | 2 | NULL | NOT A KEY |
| 145 | SEP_FEHB_PLAN_CD | varchar2 | 4 | 0 | NULL | NOT A KEY |
| 146 | SEP_FINCL_INSTN_NUM | varchar2 | 10 | 0 | NULL | NOT A KEY |
| 147 | SEP_MAINT_ALLOWC_AMT | number(p,s) | 7 | 2 | NULL | NOT A KEY |
| 148 | SEP_REASON_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 149 | STAFFING_DIF_BWKLY_AMT | number(p,s) | 5 | 0 | NULL | NOT A KEY |
| 150 | STAFFING_DIF_TOT_AMT | number(p,s) | 5 | 0 | NULL | NOT A KEY |
| 151 | STANDBY_PAY_PCT | number(p,s) | 6 | 3 | NULL | NOT A KEY |
| 152 | STD_OPT_LIFE_INS_CD | number(p,s) | 1 | 0 | NULL | NOT A KEY |
| 153 | SUBSISTENCE_DED_AMT | number(p,s) | 7 | 2 | NULL | NOT A KEY |
| 154 | SUPERVSRY_DIF_BWKLY_AMT | number(p,s) | 5 | 0 | NULL | NOT A KEY |
| 155 | SUPERVSRY_DIF_TOT_AMT | number(p,s) | 5 | 0 | NULL | NOT A KEY |
| 156 | T38_PREM_HRLY_RATE_AMT | number(p,s) | 5 | 2 | NULL | NOT A KEY |
| 157 | TIAA_CERTIFICATE_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 158 | TIME_OFF_AWARD_AMT | number(p,s) | 7 | 2 | NULL | NOT A KEY |
| 159 | TIME_OFF_EFF_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 160 | TIME_OFF_FORFEITED_HRS | number(p,s) | 5 | 2 | NULL | NOT A KEY |
| 161 | TIME_OFF_FORFEITED_TOT_HRS | number(p,s) | 5 | 2 | NULL | NOT A KEY |
| 162 | TIME_OFF_GRANTED_HRS | number(p,s) | 5 | 2 | NULL | NOT A KEY |
| 163 | TIME_OFF_GRANTED_TOT_HRS | number(p,s) | 5 | 2 | NULL | NOT A KEY |
| 164 | TIME_OFF_USED_HRS | number(p,s) | 5 | 2 | NULL | NOT A KEY |
| 165 | TIME_OFF_USED_TOT_HRS | number(p,s) | 5 | 2 | NULL | NOT A KEY |
| 166 | TSP_ADJMT_AMT | number(p,s) | 7 | 2 | NULL | NOT A KEY |
| 167 | TSP_ADJMT_START_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 168 | TSP_ADJMT_STOP_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 169 | TSP_EMP_DED_AMT | number(p,s) | 7 | 2 | NULL | NOT A KEY |
| 170 | TSP_EMP_DED_RATE_PCT | number(p,s) | 8 | 2 | NULL | NOT A KEY |
| 171 | TSP_EMP_PP_UND_DED_AMT | number(p,s) | 9 | 2 | NULL | NOT A KEY |
| 172 | TSP_EMP_PP_UND_DED_PYMT_COUNT | number(p,s) | 3 | 0 | NULL | NOT A KEY |
| 173 | TSP_EMP_PREV_GOVT_CONTB_AMT | number(p,s) | 7 | 2 | NULL | NOT A KEY |
| 174 | TSP_EMP_UND_DED_OUTSTNDG_AMT | number(p,s) | 9 | 2 | NULL | NOT A KEY |
| 175 | TSP_GOVT_UND_DED_OUTSTNDG_AMT | number(p,s) | 9 | 2 | NULL | NOT A KEY |
| 176 | TSP_LOAN_PYMT_COUNT | number(p,s) | 3 | 0 | NULL | NOT A KEY |
| 177 | TSP_PAY_SUBJ_YTD_AMT | number(p,s) | 8 | 2 | NULL | NOT A KEY |
| 178 | TSP_PP_ADJMT_DED_AMT | number(p,s) | 7 | 2 | NULL | NOT A KEY |
| 179 | TSP_SCD_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 180 | TSP_STATUS_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 181 | TSP_STATUS_CHANGE_EFF_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 182 | TSP_UND_DED_DELAY_OPTION_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 183 | TSP_UND_DED_LTR_NUM | number(p,s) | 6 | 0 | NULL | NOT A KEY |
| 184 | TSP_UND_DED_OPTION_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 185 | TSP_UND_DED_STOP_OPTION_CD | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 186 | TSP_VESTING_CD | varchar2 | 2 | 0 | NULL | NOT A KEY |
| 187 | TSPC_EMP_DIST_PCT | number(p,s) | 8 | 2 | NULL | NOT A KEY |
| 188 | TSPC_GOVT_DIST_PCT | number(p,s) | 5 | 2 | NULL | NOT A KEY |
| 189 | TSPF_EMP_DIST_PCT | number(p,s) | 15 | 4 | NULL | NOT A KEY |
| 190 | TSPF_GOVT_DIST_PCT | number(p,s) | 5 | 2 | NULL | NOT A KEY |
| 191 | TSPG_EMP_DIST_PCT | number(p,s) | 8 | 2 | NULL | NOT A KEY |
| 192 | TSPG_GOVT_DIST_PCT | number(p,s) | 5 | 2 | NULL | NOT A KEY |
| 193 | UNIF_ALLOWC_ANN_AMT | number(p,s) | 7 | 2 | NULL | NOT A KEY |
| 194 | UNIF_ALLOWC_HRLY_RATE_AMT | number(p,s) | 7 | 2 | NULL | NOT A KEY |
| 195 | UNIF_ALLOWC_PP_AMT | number(p,s) | 7 | 2 | NULL | NOT A KEY |
| 196 | TSPA_HARDSHIP_STOP | varchar2 | 8 | 0 | NULL | NOT A KEY |
| 197 | TSPA_HARDSHIP_NOA | varchar2 | 4 | 0 | NULL | NOT A KEY |
| 198 | TEA_CHOICE_OPTION | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 199 | TEA_TRANSPORT_MODE | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 200 | TEA_PAYMENT_OPTION | varchar2 | 1 | 0 | NULL | NOT A KEY |
| 201 | TEA_EMPLOYER_CONTR_AMT | number(p,s) | 7 | 2 | NULL | NOT A KEY |
| 202 | TEA_EMPLOYEE_DEDUCT | number(p,s) | 7 | 2 | NULL | NOT A KEY |
| 203 | TEA_CASH_OUT | number(p,s) | 7 | 2 | NULL | NOT A KEY |
| 204 | TEA_PRIOR_YR_HHS | number(p,s) | 7 | 2 | NULL | NOT A KEY |
| 205 | TEA_COM_CHOICE_START_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 206 | LWOP_START_DTE | date | 19 | 0 | NULL | NOT A KEY |
| 207 | RETENTION_BONUS_END_DATE | date | 19 | 0 | NULL | NOT A KEY |
| 208 | LEO_SCD_DT | date | 19 | 0 | NULL | NOT A KEY |
| 209 | PAYGROUP | varchar2 | 3 | 0 | NULL | NOT A KEY |

---

## 4. Transformations

### 4.1. exp_GET_EFFDT_YEAR (Type: Expression)

**Field Count**: 3

**Type**: Expression - Business logic transformation

#### Local Variables (Processing Logic)

| Variable | Datatype | Expression | Precision | Scale |
|----------|----------|------------|-----------|-------|
| v_curr_year | decimal | `GET_DATE_PART(EFFDT, 'YYYY')` | 10 | 0 |

#### Input/Output Pass-Through Ports

| Port Name | Datatype | Expression | Precision | Scale |
|-----------|----------|------------|-----------|-------|
| EFFDT | date/time | `EFFDT` | 29 | 9 |

#### Output Ports (Computed Business Logic)

| Output Port | Datatype | Expression | Default Value | Precision | Scale |
|-------------|----------|------------|---------------|-----------|-------|
| o_CURRENT_YEAR | decimal | `v_curr_year` | ERROR('transformation error') | 10 | 0 |

---

### 4.2. lkp_OLD_SEQUENCE_NUMBER (Type: Lookup Procedure)

**Field Count**: 3

**Type**: Lookup Procedure - Database lookup with caching

- **Lookup Table**: `SEQUENCE_NUM_TBL`
- **Lookup Condition**: `EHRP_YEAR = o_CURRENT_YEAR`
- **Connection**: `$Source`
- **Caching Enabled**: YES
- **Multiple Match Policy**: Use Any Value

**Input/Output Ports**:

| Port Name | Datatype | Precision | Scale |
|-----------|----------|-----------|-------|
| o_CURRENT_YEAR | decimal | 10 | 0 |

**Output / Lookup-Output Ports (Return Fields)**:

| Port Name | Datatype | Port Type | Precision | Scale |
|-----------|----------|-----------|-----------|-------|
| EHRP_SEQ_NUMBER | decimal | LOOKUP/OUTPUT | 4 | 0 |
| o_CURRENT_YEAR | decimal | INPUT/OUTPUT | 10 | 0 |

**Lookup-Only Ports (cached but not returned)**:

| Port Name | Datatype | Precision | Scale |
|-----------|----------|-----------|-------|
| EHRP_YEAR | decimal | 4 | 0 |

**Cache Configuration**:

- Lookup Data Cache Size: `Auto`
- Lookup Index Cache Size: `Auto`
- Dynamic Lookup Cache: `NO`
- Lookup cache directory name: `$PMCacheDir`
- Lookup cache persistent: `NO`
- Pre-build lookup cache: `Auto`
- Sorted Input: `NO`
- Case Sensitive String Comparison: `NO`

---

### 4.3. SQ_PS_GVT_JOB (Type: Source Qualifier)

**Description**: make sure to put this back:  WHERE  NWK_NEW_EHRP_ACTIONS_TBL.EMPLID = PS_GVT_JOB.EMPLID AND NWK_NEW_EHRP_ACTIONS_TBL.EMPL_RCD = PS_GVT_JOB.EMPL_RCD AND NWK_NEW_EHRP_ACTIONS_TBL.EFFDT = PS_GVT_JOB.EFFDT AND NWK_NEW_EHRP_ACTIONS_TBL.EFFSEQ  = PS_GVT_JOB.EFFSEQ ORDER BY PS_GVT_JOB.EFFDT

**Field Count**: 250

**Type**: Source Qualifier - Reads and joins source tables

**SQL Override**:
```sql
SELECT PS_GVT_JOB.EMPLID, PS_GVT_JOB.EMPL_RCD, PS_GVT_JOB.EFFDT, PS_GVT_JOB.EFFSEQ, PS_GVT_JOB.DEPTID, PS_GVT_JOB.JOBCODE, PS_GVT_JOB.POSITION_NBR, PS_GVT_JOB.ACTION, PS_GVT_JOB.ACTION_DT, PS_GVT_JOB.ACTION_REASON, PS_GVT_JOB.LOCATION, PS_GVT_JOB.POSITION_ENTRY_DT, PS_GVT_JOB.REG_TEMP, PS_GVT_JOB.COMPANY, PS_GVT_JOB.PAYGROUP, PS_GVT_JOB.STD_HOURS, PS_GVT_JOB.STD_HRS_FREQUENCY, PS_GVT_JOB.SAL_ADMIN_PLAN, PS_GVT_JOB.GRADE, PS_GVT_JOB.GRADE_ENTRY_DT, PS_GVT_JOB.STEP_ENTRY_DT, PS_GVT_JOB.ACCT_CD, PS_GVT_JOB.COMPRATE, PS_GVT_JOB.HOURLY_RT, PS_GVT_JOB.ANNL_BENEF_BASE_RT, PS_GVT_JOB.SETID_DEPT, PS_GVT_JOB.FLSA_STATUS, PS_GVT_JOB.GVT_EFFDT, PS_GVT_JOB.GVT_WIP_STATUS, PS_GVT_JOB.GVT_STATUS_TYPE, PS_GVT_JOB.GVT_NOA_CODE, PS_GVT_JOB.GVT_LEG_AUTH_1, PS_GVT_JOB.GVT_PAR_AUTH_D1, PS_GVT_JOB.GVT_PAR_AUTH_D1_2, PS_GVT_JOB.GVT_LEG_AUTH_2, PS_GVT_JOB.GVT_PAR_AUTH_D2, PS_GVT_JOB.GVT_PAR_AUTH_D2_2, PS_GVT_JOB.GVT_PAR_NTE_DATE, PS_GVT_JOB.GVT_WORK_SCHED, PS_GVT_JOB.GVT_SUB_AGENCY, PS_GVT_JOB.GVT_PAY_RATE_DETER, PS_GVT_JOB.GVT_STEP, PS_GVT_JOB.GVT_RTND_PAY_PLAN, PS_GVT_JOB.GVT_RTND_GRADE, PS_GVT_JOB.GVT_RTND_STEP, PS_GVT_JOB.GVT_PAY_BASIS, PS_GVT_JOB.GVT_COMPRATE, PS_GVT_JOB.GVT_LOCALITY_ADJ, PS_GVT_JOB.GVT_HRLY_RT_NO_LOC, PS_GVT_JOB.GVT_XFER_FROM_AGCY, PS_GVT_JOB.GVT_XFER_TO_AGCY, PS_GVT_JOB.GVT_RETIRE_PLAN, PS_GVT_JOB.GVT_ANN_IND, PS_GVT_JOB.GVT_FEGLI, PS_GVT_JOB.GVT_FEGLI_LIVING, PS_GVT_JOB.GVT_LIVING_AMT, PS_GVT_JOB.GVT_ANNUITY_OFFSET, PS_GVT_JOB.GVT_CSRS_FROZN_SVC, PS_GVT_JOB.GVT_PREV_RET_COVRG, PS_GVT_JOB.GVT_FERS_COVERAGE, PS_GVT_JOB.GVT_TYPE_OF_APPT, PS_GVT_JOB.GVT_POI, PS_GVT_JOB.GVT_POSN_OCCUPIED, PS_GVT_JOB.GVT_LEO_POSITION, PS_GVT_JOB.GVT_PAY_PLAN, PS_GVT_JOB.UNION_CD, PS_GVT_JOB.BARG_UNIT, PS_GVT_JOB.REPORTS_TO, PS_GVT_JOB.HE_NOA_EXT, PS_GVT_JOB.HE_AL_CARRYOVER, PS_GVT_JOB.HE_AL_ACCRUAL, PS_GVT_JOB.HE_AL_RED_CRED, PS_GVT_JOB.HE_AL_TOTAL, PS_GVT_JOB.HE_AL_BALANCE, PS_GVT_JOB.HE_SL_CARRYOVER, PS_GVT_JOB.HE_SL_ACCRUAL, PS_GVT_JOB.HE_SL_RED_CRED, PS_GVT_JOB.HE_SL_TOTAL, PS_GVT_JOB.HE_SL_BALANCE, PS_GVT_JOB.HE_RES_LASTYR, PS_GVT_JOB.HE_RES_TWOYRS, PS_GVT_JOB.HE_RES_THREEYRS, PS_GVT_JOB.HE_RES_BALANCE, PS_GVT_JOB.HE_LUMP_HRS, PS_GVT_JOB.HE_AWOP_SEP, PS_GVT_JOB.HE_AWOP_WIGI, PS_GVT_JOB.HE_REG_MILITARY, PS_GVT_JOB.HE_SPC_MILITARY, PS_GVT_JOB.HE_FROZEN_SL, PS_GVT_JOB.HE_TSPA_SUB_YR, PS_GVT_JOB.HE_TLTR_NO, PS_GVT_JOB.HE_UDED_PAY_CD, PS_GVT_JOB.HE_TSP_CANC_CD, PS_GVT_JOB.HE_PP_UDED_AMT, PS_GVT_JOB.HE_EMP_UDED_AMT, PS_GVT_JOB.HE_GVT_UDED_AMT, PS_GVT_JOB.HE_NO_TSP_PAYPER  FROM  PS_GVT_JOB, NWK_NEW_EHRP_ACTIONS_TBL  WHERE  NWK_NEW_EHRP_ACTIONS_TBL.EMPLID = PS_GVT_JOB.EMPLID AND NWK_NEW_EHRP_ACTIONS_TBL.EMPL_RCD = PS_GVT_JOB.EMPL_RCD AND NWK_NEW_EHRP_ACTIONS_TBL.EFFDT = PS_GVT_JOB.EFFDT AND NWK_NEW_EHRP_ACTIONS_TBL.EFFSEQ  = PS_GVT_JOB.EFFSEQ ORDER BY PS_GVT_JOB.EFFDT
```

**User-Defined Join**: `NWK_NEW_EHRP_ACTIONS_TBL.EMPLID = PS_GVT_JOB.EMPLID AND NWK_NEW_EHRP_ACTIONS_TBL.EMPL_RCD = PS_GVT_JOB.EMPL_RCD AND NWK_NEW_EHRP_ACTIONS_TBL.EFFDT = PS_GVT_JOB.EFFDT AND NWK_NEW_EHRP_ACTIONS_TBL.EFFSEQ  = PS_GVT_JOB.EFFSEQ`

**Ports**:

| Port Name | Datatype | Port Type | Precision | Scale |
|-----------|----------|-----------|-----------|-------|
| EMPLID | string | INPUT/OUTPUT | 11 | 0 |
| EMPL_RCD | decimal | INPUT/OUTPUT | 38 | 0 |
| EFFDT | date/time | INPUT/OUTPUT | 29 | 9 |
| EFFSEQ | decimal | INPUT/OUTPUT | 38 | 0 |
| DEPTID | string | INPUT/OUTPUT | 10 | 0 |
| JOBCODE | string | INPUT/OUTPUT | 6 | 0 |
| POSITION_NBR | string | INPUT/OUTPUT | 8 | 0 |
| POSITION_OVERRIDE | string | INPUT/OUTPUT | 1 | 0 |
| POSN_CHANGE_RECORD | string | INPUT/OUTPUT | 1 | 0 |
| EMPL_STATUS | string | INPUT/OUTPUT | 1 | 0 |
| ACTION | string | INPUT/OUTPUT | 3 | 0 |
| ACTION_DT | date/time | INPUT/OUTPUT | 29 | 9 |
| ACTION_REASON | string | INPUT/OUTPUT | 3 | 0 |
| LOCATION | string | INPUT/OUTPUT | 10 | 0 |
| TAX_LOCATION_CD | string | INPUT/OUTPUT | 10 | 0 |
| JOB_ENTRY_DT | date/time | INPUT/OUTPUT | 29 | 9 |
| DEPT_ENTRY_DT | date/time | INPUT/OUTPUT | 29 | 9 |
| POSITION_ENTRY_DT | date/time | INPUT/OUTPUT | 29 | 9 |
| SHIFT | string | INPUT/OUTPUT | 1 | 0 |
| REG_TEMP | string | INPUT/OUTPUT | 1 | 0 |
| FULL_PART_TIME | string | INPUT/OUTPUT | 1 | 0 |
| COMPANY | string | INPUT/OUTPUT | 3 | 0 |
| PAYGROUP | string | INPUT/OUTPUT | 3 | 0 |
| BAS_GROUP_ID | string | INPUT/OUTPUT | 3 | 0 |
| ELIG_CONFIG1 | string | INPUT/OUTPUT | 10 | 0 |
| ELIG_CONFIG2 | string | INPUT/OUTPUT | 10 | 0 |
| ELIG_CONFIG3 | string | INPUT/OUTPUT | 10 | 0 |
| ELIG_CONFIG4 | string | INPUT/OUTPUT | 10 | 0 |
| ELIG_CONFIG5 | string | INPUT/OUTPUT | 10 | 0 |
| ELIG_CONFIG6 | string | INPUT/OUTPUT | 10 | 0 |
| ELIG_CONFIG7 | string | INPUT/OUTPUT | 10 | 0 |
| ELIG_CONFIG8 | string | INPUT/OUTPUT | 10 | 0 |
| ELIG_CONFIG9 | string | INPUT/OUTPUT | 10 | 0 |
| BEN_STATUS | string | INPUT/OUTPUT | 4 | 0 |
| BAS_ACTION | string | INPUT/OUTPUT | 3 | 0 |
| COBRA_ACTION | string | INPUT/OUTPUT | 3 | 0 |
| EMPL_TYPE | string | INPUT/OUTPUT | 1 | 0 |
| HOLIDAY_SCHEDULE | string | INPUT/OUTPUT | 6 | 0 |
| STD_HOURS | decimal | INPUT/OUTPUT | 6 | 2 |
| STD_HRS_FREQUENCY | string | INPUT/OUTPUT | 5 | 0 |
| OFFICER_CD | string | INPUT/OUTPUT | 1 | 0 |
| EMPL_CLASS | string | INPUT/OUTPUT | 3 | 0 |
| SAL_ADMIN_PLAN | string | INPUT/OUTPUT | 4 | 0 |
| GRADE | string | INPUT/OUTPUT | 3 | 0 |
| GRADE_ENTRY_DT | date/time | INPUT/OUTPUT | 29 | 9 |
| STEP | decimal | INPUT/OUTPUT | 38 | 0 |
| STEP_ENTRY_DT | date/time | INPUT/OUTPUT | 29 | 9 |
| GL_PAY_TYPE | string | INPUT/OUTPUT | 6 | 0 |
| ACCT_CD | string | INPUT/OUTPUT | 25 | 0 |
| EARNS_DIST_TYPE | string | INPUT/OUTPUT | 1 | 0 |
| COMP_FREQUENCY | string | INPUT/OUTPUT | 5 | 0 |
| COMPRATE | decimal | INPUT/OUTPUT | 18 | 6 |
| CHANGE_AMT | decimal | INPUT/OUTPUT | 18 | 6 |
| CHANGE_PCT | decimal | INPUT/OUTPUT | 6 | 3 |
| ANNUAL_RT | decimal | INPUT/OUTPUT | 18 | 3 |
| MONTHLY_RT | decimal | INPUT/OUTPUT | 18 | 3 |
| DAILY_RT | decimal | INPUT/OUTPUT | 18 | 3 |
| HOURLY_RT | decimal | INPUT/OUTPUT | 18 | 6 |
| ANNL_BENEF_BASE_RT | decimal | INPUT/OUTPUT | 18 | 3 |
| SHIFT_RT | decimal | INPUT/OUTPUT | 18 | 6 |
| SHIFT_FACTOR | decimal | INPUT/OUTPUT | 4 | 3 |
| CURRENCY_CD | string | INPUT/OUTPUT | 3 | 0 |
| BUSINESS_UNIT | string | INPUT/OUTPUT | 5 | 0 |
| SETID_DEPT | string | INPUT/OUTPUT | 5 | 0 |
| SETID_JOBCODE | string | INPUT/OUTPUT | 5 | 0 |
| SETID_LOCATION | string | INPUT/OUTPUT | 5 | 0 |
| SETID_SALARY | string | INPUT/OUTPUT | 5 | 0 |
| REG_REGION | string | INPUT/OUTPUT | 5 | 0 |
| DIRECTLY_TIPPED | string | INPUT/OUTPUT | 1 | 0 |
| FLSA_STATUS | string | INPUT/OUTPUT | 1 | 0 |
| EEO_CLASS | string | INPUT/OUTPUT | 1 | 0 |
| FUNCTION_CD | string | INPUT/OUTPUT | 2 | 0 |
| TARIFF_GER | string | INPUT/OUTPUT | 2 | 0 |
| TARIFF_AREA_GER | string | INPUT/OUTPUT | 3 | 0 |
| PERFORM_GROUP_GER | string | INPUT/OUTPUT | 2 | 0 |
| LABOR_TYPE_GER | string | INPUT/OUTPUT | 1 | 0 |
| SPK_COMM_ID_GER | string | INPUT/OUTPUT | 9 | 0 |
| HOURLY_RT_FRA | string | INPUT/OUTPUT | 3 | 0 |
| ACCDNT_CD_FRA | string | INPUT/OUTPUT | 1 | 0 |
| VALUE_1_FRA | string | INPUT/OUTPUT | 5 | 0 |
| VALUE_2_FRA | string | INPUT/OUTPUT | 5 | 0 |
| VALUE_3_FRA | string | INPUT/OUTPUT | 5 | 0 |
| VALUE_4_FRA | string | INPUT/OUTPUT | 5 | 0 |
| VALUE_5_FRA | string | INPUT/OUTPUT | 5 | 0 |
| CTG_RATE | decimal | INPUT/OUTPUT | 38 | 0 |
| PAID_HOURS | decimal | INPUT/OUTPUT | 6 | 2 |
| PAID_FTE | decimal | INPUT/OUTPUT | 7 | 6 |
| PAID_HRS_FREQUENCY | string | INPUT/OUTPUT | 5 | 0 |
| GVT_EFFDT | date/time | INPUT/OUTPUT | 29 | 9 |
| GVT_EFFDT_PROPOSED | date/time | INPUT/OUTPUT | 29 | 9 |
| GVT_TRANS_NBR | decimal | INPUT/OUTPUT | 38 | 0 |
| GVT_TRANS_NBR_SEQ | decimal | INPUT/OUTPUT | 38 | 0 |
| GVT_WIP_STATUS | string | INPUT/OUTPUT | 3 | 0 |
| GVT_STATUS_TYPE | string | INPUT/OUTPUT | 3 | 0 |
| GVT_NOA_CODE | string | INPUT/OUTPUT | 3 | 0 |
| GVT_LEG_AUTH_1 | string | INPUT/OUTPUT | 3 | 0 |
| GVT_PAR_AUTH_D1 | string | INPUT/OUTPUT | 25 | 0 |
| GVT_PAR_AUTH_D1_2 | string | INPUT/OUTPUT | 25 | 0 |
| GVT_LEG_AUTH_2 | string | INPUT/OUTPUT | 3 | 0 |
| GVT_PAR_AUTH_D2 | string | INPUT/OUTPUT | 25 | 0 |
| GVT_PAR_AUTH_D2_2 | string | INPUT/OUTPUT | 25 | 0 |
| GVT_PAR_NTE_DATE | date/time | INPUT/OUTPUT | 29 | 9 |
| GVT_WORK_SCHED | string | INPUT/OUTPUT | 1 | 0 |
| GVT_SUB_AGENCY | string | INPUT/OUTPUT | 2 | 0 |
| GVT_ELIG_FEHB | string | INPUT/OUTPUT | 3 | 0 |
| GVT_FEHB_DT | date/time | INPUT/OUTPUT | 29 | 9 |
| GVT_PAY_RATE_DETER | string | INPUT/OUTPUT | 1 | 0 |
| GVT_STEP | string | INPUT/OUTPUT | 2 | 0 |
| GVT_RTND_PAY_PLAN | string | INPUT/OUTPUT | 2 | 0 |
| GVT_RTND_SAL_PLAN | string | INPUT/OUTPUT | 4 | 0 |
| GVT_RTND_GRADE | string | INPUT/OUTPUT | 3 | 0 |
| GVT_RTND_STEP | decimal | INPUT/OUTPUT | 38 | 0 |
| GVT_RTND_GVT_STEP | string | INPUT/OUTPUT | 2 | 0 |
| GVT_PAY_BASIS | string | INPUT/OUTPUT | 2 | 0 |
| GVT_COMPRATE | decimal | INPUT/OUTPUT | 18 | 6 |
| GVT_LOCALITY_ADJ | decimal | INPUT/OUTPUT | 8 | 2 |
| GVT_BIWEEKLY_RT | decimal | INPUT/OUTPUT | 9 | 2 |
| GVT_DAILY_RT | decimal | INPUT/OUTPUT | 9 | 2 |
| GVT_HRLY_RT_NO_LOC | decimal | INPUT/OUTPUT | 18 | 6 |
| GVT_DLY_RT_NO_LOC | decimal | INPUT/OUTPUT | 9 | 2 |
| GVT_BW_RT_NO_LOC | decimal | INPUT/OUTPUT | 9 | 2 |
| GVT_MNLY_RT_NO_LOC | decimal | INPUT/OUTPUT | 18 | 3 |
| GVT_ANNL_RT_NO_LOC | decimal | INPUT/OUTPUT | 18 | 3 |
| GVT_XFER_FROM_AGCY | string | INPUT/OUTPUT | 2 | 0 |
| GVT_XFER_TO_AGCY | string | INPUT/OUTPUT | 2 | 0 |
| GVT_RETIRE_PLAN | string | INPUT/OUTPUT | 2 | 0 |
| GVT_ANN_IND | string | INPUT/OUTPUT | 1 | 0 |
| GVT_FEGLI | string | INPUT/OUTPUT | 2 | 0 |
| GVT_FEGLI_LIVING | string | INPUT/OUTPUT | 1 | 0 |
| GVT_LIVING_AMT | decimal | INPUT/OUTPUT | 38 | 0 |
| GVT_ANNUITY_OFFSET | decimal | INPUT/OUTPUT | 10 | 2 |
| GVT_CSRS_FROZN_SVC | string | INPUT/OUTPUT | 4 | 0 |
| GVT_PREV_RET_COVRG | string | INPUT/OUTPUT | 1 | 0 |
| GVT_FERS_COVERAGE | string | INPUT/OUTPUT | 1 | 0 |
| GVT_TYPE_OF_APPT | string | INPUT/OUTPUT | 2 | 0 |
| GVT_POI | string | INPUT/OUTPUT | 4 | 0 |
| GVT_POSN_OCCUPIED | string | INPUT/OUTPUT | 1 | 0 |
| GVT_CONT_EMPLID | string | INPUT/OUTPUT | 11 | 0 |
| GVT_ROUTE_NEXT | string | INPUT/OUTPUT | 11 | 0 |
| GVT_CHANGE_FLAG | string | INPUT/OUTPUT | 1 | 0 |
| GVT_TSP_UPD_IND | string | INPUT/OUTPUT | 1 | 0 |
| GVT_PI_UPD_IND | string | INPUT/OUTPUT | 1 | 0 |
| GVT_SF52_NBR | string | INPUT/OUTPUT | 10 | 0 |
| GVT_S113G_CEILING | string | INPUT/OUTPUT | 1 | 0 |
| GVT_LEO_POSITION | string | INPUT/OUTPUT | 1 | 0 |
| GVT_ANNUIT_COM_DT | date/time | INPUT/OUTPUT | 29 | 9 |
| GVT_BASIC_LIFE_RED | string | INPUT/OUTPUT | 2 | 0 |
| GVT_DED_PRORT_DT | date/time | INPUT/OUTPUT | 29 | 9 |
| GVT_FEGLI_BASC_PCT | decimal | INPUT/OUTPUT | 7 | 6 |
| GVT_FEGLI_OPT_PCT | decimal | INPUT/OUTPUT | 7 | 6 |
| GVT_FEHB_PCT | decimal | INPUT/OUTPUT | 7 | 6 |
| GVT_RETRO_FLAG | string | INPUT/OUTPUT | 1 | 0 |
| GVT_RETRO_DED_FLAG | string | INPUT/OUTPUT | 1 | 0 |
| GVT_RETRO_JOB_FLAG | string | INPUT/OUTPUT | 1 | 0 |
| GVT_RETRO_BSE_FLAG | string | INPUT/OUTPUT | 1 | 0 |
| GVT_OTH_PAY_CHG | string | INPUT/OUTPUT | 1 | 0 |
| GVT_DETL_POSN_NBR | string | INPUT/OUTPUT | 8 | 0 |
| ANNL_BEN_BASE_OVRD | string | INPUT/OUTPUT | 1 | 0 |
| BENEFIT_PROGRAM | string | INPUT/OUTPUT | 3 | 0 |
| UPDATE_PAYROLL | string | INPUT/OUTPUT | 1 | 0 |
| GVT_PAY_PLAN | string | INPUT/OUTPUT | 2 | 0 |
| GVT_PAY_FLAG | string | INPUT/OUTPUT | 1 | 0 |
| GVT_NID_CHANGE | string | INPUT/OUTPUT | 1 | 0 |
| UNION_FULL_PART | string | INPUT/OUTPUT | 1 | 0 |
| UNION_POS | string | INPUT/OUTPUT | 1 | 0 |
| MATRICULA_NBR | decimal | INPUT/OUTPUT | 38 | 0 |
| SOC_SEC_RISK_CODE | string | INPUT/OUTPUT | 3 | 0 |
| UNION_FEE_AMOUNT | decimal | INPUT/OUTPUT | 8 | 2 |
| UNION_FEE_START_DT | date/time | INPUT/OUTPUT | 29 | 9 |
| UNION_FEE_END_DT | date/time | INPUT/OUTPUT | 29 | 9 |
| EXEMPT_JOB_LBR | string | INPUT/OUTPUT | 1 | 0 |
| EXEMPT_HOURS_MONTH | decimal | INPUT/OUTPUT | 38 | 0 |
| WRKS_CNCL_FUNCTION | string | INPUT/OUTPUT | 1 | 0 |
| INTERCTR_WRKS_CNCL | string | INPUT/OUTPUT | 1 | 0 |
| CURRENCY_CD1 | string | INPUT/OUTPUT | 3 | 0 |
| PAY_UNION_FEE | string | INPUT/OUTPUT | 1 | 0 |
| UNION_CD | string | INPUT/OUTPUT | 3 | 0 |
| BARG_UNIT | string | INPUT/OUTPUT | 4 | 0 |
| UNION_SENIORITY_DT | date/time | INPUT/OUTPUT | 29 | 9 |
| ENTRY_DATE | date/time | INPUT/OUTPUT | 29 | 9 |
| LABOR_AGREEMENT | string | INPUT/OUTPUT | 6 | 0 |
| EMPL_CTG | string | INPUT/OUTPUT | 6 | 0 |
| EMPL_CTG_L1 | string | INPUT/OUTPUT | 6 | 0 |
| EMPL_CTG_L2 | string | INPUT/OUTPUT | 6 | 0 |
| SETID_LBR_AGRMNT | string | INPUT/OUTPUT | 5 | 0 |
| WPP_STOP_FLAG | string | INPUT/OUTPUT | 1 | 0 |
| LABOR_FACILITY_ID | string | INPUT/OUTPUT | 10 | 0 |
| LBR_FAC_ENTRY_DT | date/time | INPUT/OUTPUT | 29 | 9 |
| LAYOFF_EXEMPT_FLAG | string | INPUT/OUTPUT | 1 | 0 |
| LAYOFF_EXEMPT_RSN | string | INPUT/OUTPUT | 11 | 0 |
| GP_PAYGROUP | string | INPUT/OUTPUT | 10 | 0 |
| GP_DFLT_ELIG_GRP | string | INPUT/OUTPUT | 1 | 0 |
| GP_ELIG_GRP | string | INPUT/OUTPUT | 10 | 0 |
| GP_DFLT_CURRTTYP | string | INPUT/OUTPUT | 1 | 0 |
| CUR_RT_TYPE | string | INPUT/OUTPUT | 5 | 0 |
| GP_DFLT_EXRTDT | string | INPUT/OUTPUT | 1 | 0 |
| GP_ASOF_DT_EXG_RT | string | INPUT/OUTPUT | 1 | 0 |
| ADDS_TO_FTE_ACTUAL | string | INPUT/OUTPUT | 1 | 0 |
| CLASS_INDC | string | INPUT/OUTPUT | 1 | 0 |
| ENCUMB_OVERRIDE | string | INPUT/OUTPUT | 1 | 0 |
| FICA_STATUS_EE | string | INPUT/OUTPUT | 1 | 0 |
| FTE | decimal | INPUT/OUTPUT | 7 | 6 |
| PRORATE_CNT_AMT | string | INPUT/OUTPUT | 1 | 0 |
| PAY_SYSTEM_FLG | string | INPUT/OUTPUT | 2 | 0 |
| BORDER_WALKER | string | INPUT/OUTPUT | 1 | 0 |
| LUMP_SUM_PAY | string | INPUT/OUTPUT | 1 | 0 |
| CONTRACT_NUM | string | INPUT/OUTPUT | 25 | 0 |
| JOB_INDICATOR | string | INPUT/OUTPUT | 1 | 0 |
| WRKS_CNCL_ROLE_CHE | string | INPUT/OUTPUT | 30 | 0 |
| BENEFIT_SYSTEM | string | INPUT/OUTPUT | 2 | 0 |
| WORK_DAY_HOURS | decimal | INPUT/OUTPUT | 6 | 2 |
| SUPERVISOR_ID | string | INPUT/OUTPUT | 11 | 0 |
| REPORTS_TO | string | INPUT/OUTPUT | 8 | 0 |
| ESTABID | string | INPUT/OUTPUT | 12 | 0 |
| HE_NOA_EXT | string | INPUT/OUTPUT | 1 | 0 |
| HE_AL_CARRYOVER | decimal | INPUT/OUTPUT | 6 | 2 |
| HE_AL_ACCRUAL | decimal | INPUT/OUTPUT | 5 | 2 |
| HE_AL_RED_CRED | decimal | INPUT/OUTPUT | 5 | 2 |
| HE_AL_TOTAL | decimal | INPUT/OUTPUT | 5 | 2 |
| HE_AL_BALANCE | decimal | INPUT/OUTPUT | 6 | 2 |
| HE_SL_CARRYOVER | decimal | INPUT/OUTPUT | 6 | 2 |
| HE_SL_ACCRUAL | decimal | INPUT/OUTPUT | 5 | 2 |
| HE_SL_RED_CRED | decimal | INPUT/OUTPUT | 5 | 2 |
| HE_SL_TOTAL | decimal | INPUT/OUTPUT | 6 | 2 |
| HE_SL_BALANCE | decimal | INPUT/OUTPUT | 6 | 2 |
| HE_RES_LASTYR | decimal | INPUT/OUTPUT | 6 | 2 |
| HE_RES_TWOYRS | decimal | INPUT/OUTPUT | 6 | 2 |
| HE_RES_THREEYRS | decimal | INPUT/OUTPUT | 6 | 2 |
| HE_RES_BALANCE | decimal | INPUT/OUTPUT | 6 | 2 |
| HE_LUMP_HRS | decimal | INPUT/OUTPUT | 38 | 0 |
| HE_AWOP_SEP | decimal | INPUT/OUTPUT | 6 | 2 |
| HE_AWOP_WIGI | decimal | INPUT/OUTPUT | 6 | 2 |
| HE_REG_MILITARY | decimal | INPUT/OUTPUT | 38 | 0 |
| HE_SPC_MILITARY | decimal | INPUT/OUTPUT | 38 | 0 |
| HE_FROZEN_SL | decimal | INPUT/OUTPUT | 6 | 2 |
| HE_TSPA_PR_YR | decimal | INPUT/OUTPUT | 6 | 2 |
| HE_TSPA_SUB_YR | decimal | INPUT/OUTPUT | 7 | 2 |
| HE_UNOFF_AL | decimal | INPUT/OUTPUT | 38 | 0 |
| HE_UNOFF_SL | decimal | INPUT/OUTPUT | 38 | 0 |
| HE_TLTR_NO | decimal | INPUT/OUTPUT | 38 | 0 |
| HE_UDED_PAY_CD | string | INPUT/OUTPUT | 1 | 0 |
| HE_TSP_CANC_CD | string | INPUT/OUTPUT | 1 | 0 |
| HE_PP_UDED_AMT | decimal | INPUT/OUTPUT | 9 | 2 |
| HE_EMP_UDED_AMT | decimal | INPUT/OUTPUT | 9 | 2 |
| HE_GVT_UDED_AMT | decimal | INPUT/OUTPUT | 9 | 2 |
| HE_NO_TSP_PAYPER | decimal | INPUT/OUTPUT | 38 | 0 |
| EMPLID1 | string | INPUT/OUTPUT | 11 | 0 |
| EMPL_RCD1 | decimal | INPUT/OUTPUT | 38 | 0 |
| EFFDT1 | date/time | INPUT/OUTPUT | 29 | 9 |
| EFFSEQ1 | decimal | INPUT/OUTPUT | 38 | 0 |

**Key Attributes**:

- Sql Query: `SELECT PS_GVT_JOB.EMPLID, PS_GVT_JOB.EMPL_RCD, PS_GVT_JOB.EFFDT, PS_GVT_JOB.EFFSEQ, PS_GVT_JOB.DEPTID, PS_GVT_JOB.JOBCODE, PS_GVT_JOB.POSITION_NBR, PS_GVT_JOB.ACTION, PS_GVT_JOB.ACTION_DT, PS_GVT_JOB.ACTION_REASON, PS_GVT_JOB.LOCATION, PS_GVT_JOB.POSITION_ENTRY_DT, PS_GVT_JOB.REG_TEMP, PS_GVT_JOB.COMPANY, PS_GVT_JOB.PAYGROUP, PS_GVT_JOB.STD_HOURS, PS_GVT_JOB.STD_HRS_FREQUENCY, PS_GVT_JOB.SAL_ADMIN_PLAN, PS_GVT_JOB.GRADE, PS_GVT_JOB.GRADE_ENTRY_DT, PS_GVT_JOB.STEP_ENTRY_DT, PS_GVT_JOB.ACCT_CD, PS_GVT_JOB.COMPRATE, PS_GVT_JOB.HOURLY_RT, PS_GVT_JOB.ANNL_BENEF_BASE_RT, PS_GVT_JOB.SETID_DEPT, PS_GVT_JOB.FLSA_STATUS, PS_GVT_JOB.GVT_EFFDT, PS_GVT_JOB.GVT_WIP_STATUS, PS_GVT_JOB.GVT_STATUS_TYPE, PS_GVT_JOB.GVT_NOA_CODE, PS_GVT_JOB.GVT_LEG_AUTH_1, PS_GVT_JOB.GVT_PAR_AUTH_D1, PS_GVT_JOB.GVT_PAR_AUTH_D1_2, PS_GVT_JOB.GVT_LEG_AUTH_2, PS_GVT_JOB.GVT_PAR_AUTH_D2, PS_GVT_JOB.GVT_PAR_AUTH_D2_2, PS_GVT_JOB.GVT_PAR_NTE_DATE, PS_GVT_JOB.GVT_WORK_SCHED, PS_GVT_JOB.GVT_SUB_AGENCY, PS_GVT_JOB.GVT_PAY_RATE_DETER, PS_GVT_JOB.GVT_STEP, PS_GVT_JOB.GVT_RTND_PAY_PLAN, PS_GVT_JOB.GVT_RTND_GRADE, PS_GVT_JOB.GVT_RTND_STEP, PS_GVT_JOB.GVT_PAY_BASIS, PS_GVT_JOB.GVT_COMPRATE, PS_GVT_JOB.GVT_LOCALITY_ADJ, PS_GVT_JOB.GVT_HRLY_RT_NO_LOC, PS_GVT_JOB.GVT_XFER_FROM_AGCY, PS_GVT_JOB.GVT_XFER_TO_AGCY, PS_GVT_JOB.GVT_RETIRE_PLAN, PS_GVT_JOB.GVT_ANN_IND, PS_GVT_JOB.GVT_FEGLI, PS_GVT_JOB.GVT_FEGLI_LIVING, PS_GVT_JOB.GVT_LIVING_AMT, PS_GVT_JOB.GVT_ANNUITY_OFFSET, PS_GVT_JOB.GVT_CSRS_FROZN_SVC, PS_GVT_JOB.GVT_PREV_RET_COVRG, PS_GVT_JOB.GVT_FERS_COVERAGE, PS_GVT_JOB.GVT_TYPE_OF_APPT, PS_GVT_JOB.GVT_POI, PS_GVT_JOB.GVT_POSN_OCCUPIED, PS_GVT_JOB.GVT_LEO_POSITION, PS_GVT_JOB.GVT_PAY_PLAN, PS_GVT_JOB.UNION_CD, PS_GVT_JOB.BARG_UNIT, PS_GVT_JOB.REPORTS_TO, PS_GVT_JOB.HE_NOA_EXT, PS_GVT_JOB.HE_AL_CARRYOVER, PS_GVT_JOB.HE_AL_ACCRUAL, PS_GVT_JOB.HE_AL_RED_CRED, PS_GVT_JOB.HE_AL_TOTAL, PS_GVT_JOB.HE_AL_BALANCE, PS_GVT_JOB.HE_SL_CARRYOVER, PS_GVT_JOB.HE_SL_ACCRUAL, PS_GVT_JOB.HE_SL_RED_CRED, PS_GVT_JOB.HE_SL_TOTAL, PS_GVT_JOB.HE_SL_BALANCE, PS_GVT_JOB.HE_RES_LASTYR, PS_GVT_JOB.HE_RES_TWOYRS, PS_GVT_JOB.HE_RES_THREEYRS, PS_GVT_JOB.HE_RES_BALANCE, PS_GVT_JOB.HE_LUMP_HRS, PS_GVT_JOB.HE_AWOP_SEP, PS_GVT_JOB.HE_AWOP_WIGI, PS_GVT_JOB.HE_REG_MILITARY, PS_GVT_JOB.HE_SPC_MILITARY, PS_GVT_JOB.HE_FROZEN_SL, PS_GVT_JOB.HE_TSPA_SUB_YR, PS_GVT_JOB.HE_TLTR_NO, PS_GVT_JOB.HE_UDED_PAY_CD, PS_GVT_JOB.HE_TSP_CANC_CD, PS_GVT_JOB.HE_PP_UDED_AMT, PS_GVT_JOB.HE_EMP_UDED_AMT, PS_GVT_JOB.HE_GVT_UDED_AMT, PS_GVT_JOB.HE_NO_TSP_PAYPER  FROM  PS_GVT_JOB, NWK_NEW_EHRP_ACTIONS_TBL  WHERE  NWK_NEW_EHRP_ACTIONS_TBL.EMPLID = PS_GVT_JOB.EMPLID AND NWK_NEW_EHRP_ACTIONS_TBL.EMPL_RCD = PS_GVT_JOB.EMPL_RCD AND NWK_NEW_EHRP_ACTIONS_TBL.EFFDT = PS_GVT_JOB.EFFDT AND NWK_NEW_EHRP_ACTIONS_TBL.EFFSEQ  = PS_GVT_JOB.EFFSEQ ORDER BY PS_GVT_JOB.EFFDT`
- User Defined Join: `NWK_NEW_EHRP_ACTIONS_TBL.EMPLID = PS_GVT_JOB.EMPLID AND NWK_NEW_EHRP_ACTIONS_TBL.EMPL_RCD = PS_GVT_JOB.EMPL_RCD AND NWK_NEW_EHRP_ACTIONS_TBL.EFFDT = PS_GVT_JOB.EFFDT AND NWK_NEW_EHRP_ACTIONS_TBL.EFFSEQ  = PS_GVT_JOB.EFFSEQ`
- Number Of Sorted Ports: `0`
- Tracing Level: `Normal`
- Select Distinct: `NO`
- Is Partitionable: `NO`
- Output is deterministic: `NO`
- Output is repeatable: `Never`

---

### 4.4. lkp_PS_GVT_EMPLOYMENT (Type: Lookup Procedure)

**Field Count**: 85

**Type**: Lookup Procedure - Database lookup with caching

- **Lookup Table**: `PS_GVT_EMPLOYMENT`
- **Lookup Condition**: `EMPLID = EMPLID_IN AND EMPL_RCD = EMPL_RCD_IN AND EFFDT = EFFDT_IN AND EFFSEQ = EFFSEQ_IN`
- **Connection**: `$Source`
- **Caching Enabled**: YES
- **Multiple Match Policy**: Use Any Value

**Input Ports**:

| Port Name | Datatype | Precision | Scale |
|-----------|----------|-----------|-------|
| EMPLID_IN | string | 11 | 0 |
| EMPL_RCD_IN | decimal | 38 | 0 |
| EFFDT_IN | date/time | 29 | 9 |
| EFFSEQ_IN | decimal | 38 | 0 |

**Output / Lookup-Output Ports (Return Fields)**:

| Port Name | Datatype | Port Type | Precision | Scale |
|-----------|----------|-----------|-----------|-------|
| EMPLID | string | LOOKUP/OUTPUT | 11 | 0 |
| EMPL_RCD | decimal | LOOKUP/OUTPUT | 38 | 0 |
| EFFDT | date/time | LOOKUP/OUTPUT | 29 | 9 |
| EFFSEQ | decimal | LOOKUP/OUTPUT | 38 | 0 |
| BENEFIT_RCD_NBR | decimal | LOOKUP/OUTPUT | 38 | 0 |
| HOME_HOST_CLASS | string | LOOKUP/OUTPUT | 1 | 0 |
| HIRE_DT | date/time | LOOKUP/OUTPUT | 29 | 9 |
| REHIRE_DT | date/time | LOOKUP/OUTPUT | 29 | 9 |
| CMPNY_SENIORITY_DT | date/time | LOOKUP/OUTPUT | 29 | 9 |
| SERVICE_DT | date/time | LOOKUP/OUTPUT | 29 | 9 |
| PROF_EXPERIENCE_DT | date/time | LOOKUP/OUTPUT | 29 | 9 |
| LAST_VERIFICATN_DT | date/time | LOOKUP/OUTPUT | 29 | 9 |
| EXPECTED_RETURN_DT | date/time | LOOKUP/OUTPUT | 29 | 9 |
| TERMINATION_DT | date/time | LOOKUP/OUTPUT | 29 | 9 |
| LAST_DATE_WORKED | date/time | LOOKUP/OUTPUT | 29 | 9 |
| LAST_INCREASE_DT | date/time | LOOKUP/OUTPUT | 29 | 9 |
| OWN_5PERCENT_CO | string | LOOKUP/OUTPUT | 1 | 0 |
| BUSINESS_TITLE | string | LOOKUP/OUTPUT | 30 | 0 |
| REPORTS_TO | string | LOOKUP/OUTPUT | 8 | 0 |
| PROBATION_DT | date/time | LOOKUP/OUTPUT | 29 | 9 |
| SECURITY_CLEARANCE | string | LOOKUP/OUTPUT | 1 | 0 |
| DED_TAKEN | string | LOOKUP/OUTPUT | 1 | 0 |
| DED_SUBSET_ID | string | LOOKUP/OUTPUT | 3 | 0 |
| SETID | string | LOOKUP/OUTPUT | 5 | 0 |
| GVT_SCD_RETIRE | date/time | LOOKUP/OUTPUT | 29 | 9 |
| GVT_SCD_TSP | date/time | LOOKUP/OUTPUT | 29 | 9 |
| GVT_SCD_LEO | date/time | LOOKUP/OUTPUT | 29 | 9 |
| GVT_SCD_SEVPAY | date/time | LOOKUP/OUTPUT | 29 | 9 |
| GVT_SEVPAY_PRV_WKS | decimal | LOOKUP/OUTPUT | 38 | 0 |
| GVT_MAND_RET_DT | date/time | LOOKUP/OUTPUT | 29 | 9 |
| GVT_WGI_STATUS | string | LOOKUP/OUTPUT | 1 | 0 |
| GVT_INTRM_DAYS_WGI | decimal | LOOKUP/OUTPUT | 38 | 0 |
| GVT_NONPAY_NOA | string | LOOKUP/OUTPUT | 3 | 0 |
| GVT_NONPAY_HRS_WGI | decimal | LOOKUP/OUTPUT | 6 | 2 |
| GVT_NONPAY_HRS_SCD | decimal | LOOKUP/OUTPUT | 6 | 2 |
| GVT_NONPAY_HRS_TNR | decimal | LOOKUP/OUTPUT | 6 | 2 |
| GVT_NONPAY_HRS_PRB | decimal | LOOKUP/OUTPUT | 6 | 2 |
| GVT_TEMP_PRO_EXPIR | date/time | LOOKUP/OUTPUT | 29 | 9 |
| GVT_TEMP_PSN_EXPIR | date/time | LOOKUP/OUTPUT | 29 | 9 |
| GVT_DETAIL_EXPIRES | date/time | LOOKUP/OUTPUT | 29 | 9 |
| GVT_SABBATIC_EXPIR | date/time | LOOKUP/OUTPUT | 29 | 9 |
| GVT_RTND_GRADE_BEG | date/time | LOOKUP/OUTPUT | 29 | 9 |
| GVT_RTND_GRADE_EXP | date/time | LOOKUP/OUTPUT | 29 | 9 |
| GVT_NOA_CODE | string | LOOKUP/OUTPUT | 3 | 0 |
| GVT_CURR_APT_AUTH1 | string | LOOKUP/OUTPUT | 3 | 0 |
| GVT_CURR_APT_AUTH2 | string | LOOKUP/OUTPUT | 3 | 0 |
| GVT_APPT_EXPIR_DT | date/time | LOOKUP/OUTPUT | 29 | 9 |
| GVT_CNV_BEGIN_DATE | date/time | LOOKUP/OUTPUT | 29 | 9 |
| GVT_CAREER_CNV_DUE | date/time | LOOKUP/OUTPUT | 29 | 9 |
| GVT_CAREER_COND_DT | date/time | LOOKUP/OUTPUT | 29 | 9 |
| GVT_APPT_LIMIT_HRS | decimal | LOOKUP/OUTPUT | 38 | 0 |
| GVT_APPT_LIMIT_DYS | decimal | LOOKUP/OUTPUT | 38 | 0 |
| GVT_APPT_LIMIT_AMT | decimal | LOOKUP/OUTPUT | 38 | 0 |
| GVT_SUPV_PROB_DT | date/time | LOOKUP/OUTPUT | 29 | 9 |
| GVT_SES_PROB_DT | date/time | LOOKUP/OUTPUT | 29 | 9 |
| GVT_SEC_CLR_STATUS | string | LOOKUP/OUTPUT | 1 | 0 |
| GVT_CLRNCE_STAT_DT | date/time | LOOKUP/OUTPUT | 29 | 9 |
| GVT_ERN_PGM_PERM | string | LOOKUP/OUTPUT | 2 | 0 |
| GVT_OCC_SERS_PERM | string | LOOKUP/OUTPUT | 4 | 0 |
| GVT_GRADE_PERM | string | LOOKUP/OUTPUT | 3 | 0 |
| GVT_COMP_AREA_PERM | string | LOOKUP/OUTPUT | 2 | 0 |
| GVT_COMP_LVL_PERM | string | LOOKUP/OUTPUT | 4 | 0 |
| GVT_CHANGE_FLAG | string | LOOKUP/OUTPUT | 1 | 0 |
| GVT_SPEP | string | LOOKUP/OUTPUT | 2 | 0 |
| GVT_WGI_DUE_DATE | date/time | LOOKUP/OUTPUT | 29 | 9 |
| GVT_DT_LEI | date/time | LOOKUP/OUTPUT | 29 | 9 |
| GVT_FIN_DISCLOSURE | string | LOOKUP/OUTPUT | 1 | 0 |
| GVT_FIN_DISCL_DATE | date/time | LOOKUP/OUTPUT | 29 | 9 |
| GVT_TENURE | string | LOOKUP/OUTPUT | 1 | 0 |
| GVT_DETL_BARG_UNIT | string | LOOKUP/OUTPUT | 4 | 0 |
| GVT_DETL_UNION_CD | string | LOOKUP/OUTPUT | 3 | 0 |
| NEXT_REVIEW_DT | date/time | LOOKUP/OUTPUT | 29 | 9 |
| GVT_WELFARE_WK_CD | string | LOOKUP/OUTPUT | 2 | 0 |
| TENURE_ACCR_FLG | string | LOOKUP/OUTPUT | 1 | 0 |
| FTE_TENURE | decimal | LOOKUP/OUTPUT | 3 | 2 |
| EG_GROUP | string | LOOKUP/OUTPUT | 6 | 0 |
| FTE_FLX_SRVC | decimal | LOOKUP/OUTPUT | 3 | 2 |
| CONTRACT_LENGTH | string | LOOKUP/OUTPUT | 1 | 0 |
| APPOINT_END_DT | date/time | LOOKUP/OUTPUT | 29 | 9 |
| NEE_PROVIDER_ID | string | LOOKUP/OUTPUT | 10 | 0 |
| POSITION_PHONE | string | LOOKUP/OUTPUT | 24 | 0 |

**Cache Configuration**:

- Lookup Data Cache Size: `Auto`
- Lookup Index Cache Size: `Auto`
- Dynamic Lookup Cache: `NO`
- Lookup cache directory name: `$PMCacheDir`
- Lookup cache persistent: `NO`
- Pre-build lookup cache: `Auto`
- Sorted Input: `NO`
- Case Sensitive String Comparison: `NO`

---

### 4.5. lkp_PS_GVT_PERS_NID (Type: Lookup Procedure)

**Field Count**: 13

**Type**: Lookup Procedure - Database lookup with caching

- **Lookup Table**: `PS_GVT_PERS_NID`
- **Lookup Condition**: `EMPLID = EMPLID_IN AND EMPL_RCD = EMPL_RCD_IN AND EFFDT = EFFDT_IN AND EFFSEQ = EFFSEQ_IN`
- **Connection**: `$Source`
- **Caching Enabled**: YES
- **Multiple Match Policy**: Use Any Value

**Input Ports**:

| Port Name | Datatype | Precision | Scale |
|-----------|----------|-----------|-------|
| EMPLID_IN | string | 11 | 0 |
| EMPL_RCD_IN | decimal | 38 | 0 |
| EFFDT_IN | date/time | 29 | 9 |
| EFFSEQ_IN | decimal | 38 | 0 |

**Output / Lookup-Output Ports (Return Fields)**:

| Port Name | Datatype | Port Type | Precision | Scale |
|-----------|----------|-----------|-----------|-------|
| EMPLID | string | LOOKUP/OUTPUT | 11 | 0 |
| EMPL_RCD | decimal | LOOKUP/OUTPUT | 38 | 0 |
| EFFDT | date/time | LOOKUP/OUTPUT | 29 | 9 |
| EFFSEQ | decimal | LOOKUP/OUTPUT | 38 | 0 |
| COUNTRY | string | LOOKUP/OUTPUT | 3 | 0 |
| NATIONAL_ID_TYPE | string | LOOKUP/OUTPUT | 6 | 0 |
| NATIONAL_ID | string | LOOKUP/OUTPUT | 20 | 0 |
| SSN_KEY_FRA | string | LOOKUP/OUTPUT | 2 | 0 |
| PRIMARY_NID | string | LOOKUP/OUTPUT | 1 | 0 |

**Cache Configuration**:

- Lookup Data Cache Size: `Auto`
- Lookup Index Cache Size: `Auto`
- Dynamic Lookup Cache: `NO`
- Lookup cache directory name: `$PMCacheDir`
- Lookup cache persistent: `NO`
- Pre-build lookup cache: `Auto`
- Sorted Input: `NO`
- Case Sensitive String Comparison: `NO`

---

### 4.6. exp_MAIN2BIIS (Type: Expression)

**Field Count**: 186

**Type**: Expression - Business logic transformation

#### Local Variables (Processing Logic)

| Variable | Datatype | Expression | Precision | Scale |
|----------|----------|------------|-----------|-------|
| v_CURRENT_YEAR | integer | `GET_DATE_PART(EFFDT, 'YYYY')` | 10 | 0 |
| v_EVENT_ID | integer | `IIF(v_CURRENT_YEAR =v_PREVIOUS_YEAR ,v_EVENT_ID + 1, EHRP_SEQ_NUMBER+1)` | 10 | 0 |
| v_PREVIOUS_YEAR | integer | `v_CURRENT_YEAR` | 10 | 0 |
| v_MONTH_NUMBER | integer | `GET_DATE_PART(SYSDATE, 'MM')` | 10 | 0 |
| v_DAY_NUMBER | integer | `GET_DATE_PART(SYSDATE, 'DD')` | 10 | 0 |
| v_MONTH_CHAR | string | `IIF(v_MONTH_NUMBER < 10, '0'  \|\| TO_CHAR(v_MONTH_NUMBER), TO_CHAR(v_MONTH_NUMBER))` | 2 | 0 |
| v_DAY_CHAR | string | `IIF(v_DAY_NUMBER < 10, '0'  \|\| TO_CHAR(v_DAY_NUMBER), TO_CHAR(v_DAY_NUMBER))` | 2 | 0 |

#### Input-Only Ports

| Port Name | Datatype | Precision | Scale |
|-----------|----------|-----------|-------|
| EHRP_SEQ_NUMBER | decimal | 4 | 0 |
| GVT_SUB_AGENCY | string | 2 | 0 |
| GVT_XFER_FROM_AGCY | string | 2 | 0 |
| HE_AL_RED_CRED | decimal | 5 | 2 |
| HE_AL_BALANCE | decimal | 6 | 2 |
| HE_LUMP_HRS | decimal | 38 | 0 |
| HE_AL_CARRYOVER | decimal | 6 | 2 |
| HE_RES_LASTYR | decimal | 6 | 2 |
| HE_RES_BALANCE | decimal | 6 | 2 |
| HE_RES_TWOYRS | decimal | 6 | 2 |
| HE_RES_THREEYRS | decimal | 6 | 2 |
| HE_AL_ACCRUAL | decimal | 5 | 2 |
| HE_AL_TOTAL | decimal | 5 | 2 |
| STD_HOURS | decimal | 6 | 2 |
| STD_HRS_FREQUENCY | string | 5 | 0 |
| GVT_PAR_AUTH_D1 | string | 25 | 0 |
| GVT_PAR_AUTH_D1_2 | string | 25 | 0 |
| GVT_PAR_AUTH_D2 | string | 25 | 0 |
| GVT_PAR_AUTH_D2_2 | string | 25 | 0 |
| COMPRATE | decimal | 18 | 6 |
| HE_SL_RED_CRED | decimal | 5 | 2 |
| HE_SL_BALANCE | decimal | 6 | 2 |
| HE_FROZEN_SL | decimal | 6 | 2 |
| HE_SL_CARRYOVER | decimal | 6 | 2 |
| HE_SL_ACCRUAL | decimal | 5 | 2 |
| HE_SL_TOTAL | decimal | 6 | 2 |
| CITY | string | 30 | 0 |
| STATE | string | 6 | 0 |
| EARNINGS_END_DT | date/time | 29 | 9 |
| ERNCD | string | 3 | 0 |
| GOAL_AMT | decimal | 10 | 2 |
| GVT_TANG_BEN_AMT | decimal | 38 | 0 |
| OTH_HRS | decimal | 6 | 2 |
| CITIZENSHIP_STATUS_IN | string | 1 | 0 |
| GVT_DATE_WRK | date/time | 29 | 9 |
| BUSINESS_TITLE | string | 30 | 0 |
| GVT_APPT_LIMIT_HRS | decimal | 38 | 0 |
| JPM_INTEGER_2 | decimal | 38 | 0 |
| OTH_PAY | decimal | 10 | 2 |
| PROBATION_DT | date/time | 29 | 9 |

#### Input/Output Pass-Through Ports

| Port Name | Datatype | Expression | Precision | Scale |
|-----------|----------|------------|-----------|-------|
| COMPANY | string | `COMPANY` | 3 | 0 |
| GVT_XFER_TO_AGCY | string | `GVT_XFER_TO_AGCY` | 2 | 0 |
| ANNL_BENEF_BASE_RT | decimal | `ANNL_BENEF_BASE_RT` | 18 | 3 |
| GVT_ANN_IND | string | `GVT_ANN_IND` | 1 | 0 |
| GVT_TYPE_OF_APPT | string | `GVT_TYPE_OF_APPT` | 2 | 0 |
| HE_AWOP_SEP | decimal | `HE_AWOP_SEP` | 6 | 2 |
| BARG_UNIT | string | `BARG_UNIT` | 4 | 0 |
| ACCT_CD | string | `ACCT_CD` | 25 | 0 |
| LOCATION | string | `LOCATION` | 10 | 0 |
| GRADE_ENTRY_DT | date/time | `GRADE_ENTRY_DT` | 29 | 9 |
| EFFDT | date/time | `EFFDT` | 29 | 9 |
| GVT_FEGLI | string | `GVT_FEGLI` | 2 | 0 |
| GVT_FEGLI_LIVING | string | `GVT_FEGLI_LIVING` | 1 | 0 |
| GVT_LIVING_AMT | decimal | `GVT_LIVING_AMT` | 38 | 0 |
| GVT_FERS_COVERAGE | string | `GVT_FERS_COVERAGE` | 1 | 0 |
| FLSA_STATUS | string | `FLSA_STATUS` | 1 | 0 |
| GVT_CSRS_FROZN_SVC | string | `GVT_CSRS_FROZN_SVC` | 4 | 0 |
| GRADE | string | `GRADE` | 3 | 0 |
| HOURLY_RT | decimal | `HOURLY_RT` | 18 | 6 |
| GVT_LEG_AUTH_1 | string | `GVT_LEG_AUTH_1` | 3 | 0 |
| GVT_LEG_AUTH_2 | string | `GVT_LEG_AUTH_2` | 3 | 0 |
| GVT_LOCALITY_ADJ | decimal | `GVT_LOCALITY_ADJ` | 8 | 2 |
| HE_AWOP_WIGI | decimal | `HE_AWOP_WIGI` | 6 | 2 |
| GVT_NOA_CODE | string | `GVT_NOA_CODE` | 3 | 0 |
| HE_NOA_EXT | string | `HE_NOA_EXT` | 1 | 0 |
| DEPTID | string | `DEPTID` | 10 | 0 |
| GVT_PAY_BASIS | string | `GVT_PAY_BASIS` | 2 | 0 |
| GVT_PAY_PLAN | string | `GVT_PAY_PLAN` | 2 | 0 |
| GVT_POI | string | `GVT_POI` | 4 | 0 |
| JOBCODE | string | `JOBCODE` | 6 | 0 |
| POSITION_NBR | string | `POSITION_NBR` | 8 | 0 |
| GVT_POSN_OCCUPIED | string | `GVT_POSN_OCCUPIED` | 1 | 0 |
| GVT_PAY_RATE_DETER | string | `GVT_PAY_RATE_DETER` | 1 | 0 |
| GVT_PREV_RET_COVRG | string | `GVT_PREV_RET_COVRG` | 1 | 0 |
| GVT_ANNUITY_OFFSET | decimal | `GVT_ANNUITY_OFFSET` | 10 | 2 |
| GVT_RETIRE_PLAN | string | `GVT_RETIRE_PLAN` | 2 | 0 |
| GVT_COMPRATE | decimal | `GVT_COMPRATE` | 18 | 6 |
| GVT_HRLY_RT_NO_LOC | decimal | `GVT_HRLY_RT_NO_LOC` | 18 | 6 |
| GVT_STEP | string | `GVT_STEP` | 2 | 0 |
| UNION_CD | string | `UNION_CD` | 3 | 0 |
| STEP_ENTRY_DT | date/time | `STEP_ENTRY_DT` | 29 | 9 |
| GVT_WORK_SCHED | string | `GVT_WORK_SCHED` | 1 | 0 |
| EMPLID | string | `EMPLID` | 11 | 0 |
| EFFSEQ | decimal | `EFFSEQ` | 38 | 0 |
| EMPL_RCD | decimal | `EMPL_RCD` | 38 | 0 |
| ACTION | string | `ACTION` | 3 | 0 |
| ACTION_REASON | string | `ACTION_REASON` | 3 | 0 |
| GVT_WIP_STATUS | string | `GVT_WIP_STATUS` | 3 | 0 |
| GVT_STATUS_TYPE | string | `GVT_STATUS_TYPE` | 3 | 0 |
| HE_REG_MILITARY | decimal | `HE_REG_MILITARY` | 38 | 0 |
| HE_SPC_MILITARY | decimal | `HE_SPC_MILITARY` | 38 | 0 |
| SAL_ADMIN_PLAN | string | `SAL_ADMIN_PLAN` | 4 | 0 |
| GVT_RTND_PAY_PLAN | string | `GVT_RTND_PAY_PLAN` | 2 | 0 |
| GVT_RTND_GRADE | string | `GVT_RTND_GRADE` | 3 | 0 |
| GVT_RTND_STEP | decimal | `GVT_RTND_STEP` | 38 | 0 |
| HE_PP_UDED_AMT | decimal | `HE_PP_UDED_AMT` | 9 | 2 |
| HE_NO_TSP_PAYPER | decimal | `HE_NO_TSP_PAYPER` | 38 | 0 |
| HE_TSPA_SUB_YR | decimal | `HE_TSPA_SUB_YR` | 7 | 2 |
| HE_EMP_UDED_AMT | decimal | `HE_EMP_UDED_AMT` | 9 | 2 |
| HE_GVT_UDED_AMT | decimal | `HE_GVT_UDED_AMT` | 9 | 2 |
| HE_TLTR_NO | decimal | `HE_TLTR_NO` | 38 | 0 |
| HE_UDED_PAY_CD | string | `HE_UDED_PAY_CD` | 1 | 0 |
| HE_TSP_CANC_CD | string | `HE_TSP_CANC_CD` | 1 | 0 |
| REG_TEMP | string | `REG_TEMP` | 1 | 0 |
| ACTION_DT | date/time | `ACTION_DT` | 29 | 9 |
| GVT_SCD_RETIRE | date/time | `GVT_SCD_RETIRE` | 29 | 9 |
| GVT_SCD_TSP | date/time | `GVT_SCD_TSP` | 29 | 9 |
| GVT_RTND_GRADE_BEG | date/time | `GVT_RTND_GRADE_BEG` | 29 | 9 |
| GVT_RTND_GRADE_EXP | date/time | `GVT_RTND_GRADE_EXP` | 29 | 9 |
| GVT_SABBATIC_EXPIR | date/time | `GVT_SABBATIC_EXPIR` | 29 | 9 |
| GVT_CURR_APT_AUTH1 | string | `GVT_CURR_APT_AUTH1` | 3 | 0 |
| GVT_CURR_APT_AUTH2 | string | `GVT_CURR_APT_AUTH2` | 3 | 0 |
| CMPNY_SENIORITY_DT | date/time | `CMPNY_SENIORITY_DT` | 29 | 9 |
| SETID_DEPT | string | `SETID_DEPT` | 5 | 0 |
| GVT_PAR_NTE_DATE | date/time | `GVT_PAR_NTE_DATE` | 29 | 9 |
| GVT_LEO_POSITION | string | `GVT_LEO_POSITION` | 1 | 0 |
| TEMP_GVT_EFFDT | date/time | `TEMP_GVT_EFFDT` | 29 | 9 |
| PAYGROUP | string | `PAYGROUP` | 3 | 0 |
| REPORTS_TO | string | `REPORTS_TO` | 8 | 0 |
| POSITION_ENTRY_DT | date/time | `POSITION_ENTRY_DT` | 29 | 9 |
| GVT_SCD_LEO | date/time | `GVT_SCD_LEO` | 29 | 9 |
| GVT_WGI_DUE_DATE | date/time | `GVT_WGI_DUE_DATE` | 29 | 9 |

#### Output Ports (Computed Business Logic)

| Output Port | Datatype | Expression | Default Value | Precision | Scale |
|-------------|----------|------------|---------------|-----------|-------|
| o_EVENT_ID | integer | `v_EVENT_ID` | ERROR('transformation error') | 10 | 0 |
| o_AGCY_ASSIGN_CD | string | `COMPANY  \|\| GVT_SUB_AGENCY` | ERROR('transformation error') | 10 | 0 |
| o_AGCY_SUBELEMENT_PRIOR_CD | string | `IIF(IS_SPACES( GVT_XFER_FROM_AGCY) or  ISNULL(GVT_XFER_FROM_AGCY), 			NULL,  	GVT_XFER_FROM_AGCY \|\| '00' )` | ERROR('transformation error') | 4 | 0 |
| o_HE_AL_RED_CRED | decimal | `IIF(HE_AL_RED_CRED =0, NULL, HE_AL_RED_CRED)` | ERROR('transformation error') | 5 | 2 |
| o_HE_AL_BALANCE | decimal | `IIF(HE_AL_BALANCE=0, NULL, HE_AL_BALANCE)` | ERROR('transformation error') | 6 | 2 |
| o_HE_LUMP_HRS | decimal | `IIF(HE_LUMP_HRS=0, NULL, HE_LUMP_HRS)` | ERROR('transformation error') | 38 | 0 |
| o_HE_AL_CARRYOVER | decimal | `IIF(HE_AL_CARRYOVER=0, NULL, HE_AL_CARRYOVER)` | ERROR('transformation error') | 6 | 2 |
| o_HE_RES_LASTYR | decimal | `IIF(HE_RES_LASTYR=0, NULL, HE_RES_LASTYR)` | ERROR('transformation error') | 6 | 2 |
| o_HE_RES_BALANCE | decimal | `IIF(HE_RES_BALANCE=0, NULL, HE_RES_BALANCE)` | ERROR('transformation error') | 6 | 0 |
| o_HE_RES_TWOYRS | decimal | `IIF(HE_RES_TWOYRS=0, NULL, HE_RES_TWOYRS)` | ERROR('transformation error') | 6 | 2 |
| o_HE_RES_THREEYRS | decimal | `IIF(HE_RES_THREEYRS=0, NULL, HE_RES_THREEYRS)` | ERROR('transformation error') | 6 | 2 |
| o_HE_AL_ACCRUAL | decimal | `IIF(HE_AL_ACCRUAL=0, NULL, HE_AL_ACCRUAL)` | ERROR('transformation error') | 5 | 2 |
| o_HE_AL_TOTAL | decimal | `IIF(HE_AL_TOTAL=0, NULL, HE_AL_TOTAL)` | ERROR('transformation error') | 5 | 2 |
| o_HE_AWOP_SEP | decimal | `IIF(HE_AWOP_SEP=0, NULL, HE_AWOP_SEP)` | ERROR('transformation error') | 6 | 2 |
| o_BASE_HOURS | decimal | `IIF(GVT_WORK_SCHED='I',  STD_HOURS,STD_HOURS*2)` | ERROR('transformation error') | 7 | 2 |
| o_LEG_AUTH_TXT_1 | string | `UPPER(GVT_PAR_AUTH_D1  \|\| RTRIM(GVT_PAR_AUTH_D1_2))` | ERROR('transformation error') | 60 | 0 |
| o_LEG_AUTH_TXT_2 | string | `UPPER(GVT_PAR_AUTH_D2  \|\| RTRIM(GVT_PAR_AUTH_D2_2))` | ERROR('transformation error') | 60 | 0 |
| o_HE_AWOP_WIGI | decimal | `IIF(HE_AWOP_WIGI=0, NULL, HE_AWOP_WIGI)` | ERROR('transformation error') | 6 | 2 |
| o_PERM_TEMP_POSITION_CD | string | `IIF(STD_HOURS < 40,3,IIF(STD_HOURS>=40,1,0))` | ERROR('transformation error') | 10 | 0 |
| o_GVT_ANNUITY_OFFSET | decimal | `IIF(GVT_ANNUITY_OFFSET=0,NULL,GVT_ANNUITY_OFFSET)` | ERROR('transformation error') | 10 | 2 |
| o_HE_SL_RED_CRED | decimal | `IIF(HE_SL_RED_CRED=0,NULL, HE_SL_RED_CRED)` | ERROR('transformation error') | 5 | 2 |
| o_HE_SL_BALANCE | decimal | `IIF(HE_SL_BALANCE=0, NULL, HE_SL_BALANCE)` | ERROR('transformation error') | 6 | 2 |
| o_HE_FROZEN_SL | decimal | `IIF(HE_FROZEN_SL=0, NULL, HE_FROZEN_SL)` | ERROR('transformation error') | 6 | 2 |
| o_HE_SL_CARRYOVER | decimal | `IIF(HE_SL_CARRYOVER=0, NULL, HE_SL_CARRYOVER)` | ERROR('transformation error') | 6 | 2 |
| o_HE_SL_ACCRUAL | decimal | `IIF(HE_SL_ACCRUAL=0, NULL, HE_SL_ACCRUAL)` | ERROR('transformation error') | 5 | 2 |
| o_HE_SL_TOTAL | decimal | `IIF(HE_SL_TOTAL=0, NULL, HE_SL_TOTAL)` | ERROR('transformation error') | 6 | 2 |
| o_UNION_CD | string | `IIF(IS_SPACES(UNION_CD), NULL, UNION_CD)` | ERROR('transformation error') | 3 | 0 |
| o_EMP_RESID_CITY_STATE_NAME | string | `CITY  \|\|  ',  ' \|\| STATE` | ERROR('transformation error') | 60 | 0 |
| o_EFFSEQ | string | `EFFSEQ` | ERROR('transformation error') | 10 | 0 |
| o_LOAD_ID | string | `'NK' \|\| GET_DATE_PART(SYSDATE, 'YYYY') \|\|  v_MONTH_CHAR  \|\| v_DAY_CHAR ` | ERROR('transformation error') | 10 | 0 |
| o_LOAD_DATE | date/time | `trunc(sysdate) ` | ERROR('transformation error') | 29 | 9 |
| o_LINE_SEQ | string | `'01N'` | ERROR('transformation error') | 3 | 0 |
| o_CASH_AWARD_AMT | decimal | `DECODE(TRUE,                IN (GVT_NOA_CODE,'840', '841', '842' , '843', '844','845', '848', '849', '873', '874', '875',                           '876','877','878','879') AND NOT (IN(ERNCD, 'RCR', 'RLC')),                              GOAL_AMT,                IN(GVT_NOA_CODE,  '817', '889'),                            GOAL_AMT,                     NULL)` | ERROR('transformation error') | 19 | 4 |
| o_CASH_AWARD_BNFT_AMT | decimal | `IIF(IN (GVT_NOA_CODE,'840', '841', '842' , '843', '844','845', '848', '849', '873', '874', '875', '876','877','878','879') AND NOT (IN(ERNCD, 'RCR', 'RLC')) ,GVT_TANG_BEN_AMT,NULL)` | ERROR('transformation error') | 19 | 4 |
| o_RECRUITMENT_BONUS_AMT | decimal | `IIF(GVT_NOA_CODE = '815' OR (GVT_NOA_CODE = '948'  AND HE_NOA_EXT = '0'), GOAL_AMT,NULL)` | ERROR('transformation error') | 19 | 4 |
| o_RELOCATION_BONUS_AMT | decimal | `IIF(IN(GVT_NOA_CODE, '816'), GOAL_AMT,NULL)` | ERROR('transformation error') | 19 | 4 |
| o_TIME_OFF_AWARD_AMT | decimal | `IIF(IN(GVT_NOA_CODE, '846', '847'), OTH_HRS,NULL)` | ERROR('transformation error') | 9 | 2 |
| o_TIME_OFF_GRANTED_HRS | decimal | `IIF(IN(GVT_NOA_CODE, '846', '847'), OTH_HRS,NULL)` | ERROR('transformation error') | 9 | 2 |
| o_CITIZENSHIP_STATUS | string | `IIF(CITIZENSHIP_STATUS_IN = '1'  or CITIZENSHIP_STATUS_IN = '2', '1', '8')` | ERROR('transformation error') | 1 | 0 |
| o_TSP_VESTING_CD | string | `IIF(IN(GVT_RETIRE_PLAN, 'K', 'M'),IIF(IN(GVT_TYPE_OF_APPT, '55', '34', '36', '46', '44'), 2, 3), 0)` | ERROR('transformation error') | 1 | 0 |
| o_PERMANENT_TEMP_POSITION_CD | string | `IIF(REG_TEMP = 'R' AND GVT_WORK_SCHED = 'F', '1', IIF(IN(REG_TEMP, 'R', 'T' ) AND IN(GVT_WORK_SCHED, 'P', 'I'), '3', IIF(REG_TEMP = 'T' AND GVT_WORK_SCHED = 'F', '2', NULL)))` | ERROR('transformation error') | 1 | 0 |
| o_EVENT_SUBMITTED_DTE | date/time | `IIF(ISNULL(GVT_DATE_WRK), ACTION_DT, GVT_DATE_WRK)` | ERROR('transformation error') | 29 | 9 |
| o_RECRUITMENT_EXP_DTE | date/time | `IIF(GVT_NOA_CODE = '815' OR (GVT_NOA_CODE = '948'  AND HE_NOA_EXT = '0'), EARNINGS_END_DT, NULL)` | ERROR('transformation error') | 29 | 9 |
| o_RELOCATION_EXP_DTE | date/time | `IIF(GVT_NOA_CODE = '816' OR (GVT_NOA_CODE = '948'  AND HE_NOA_EXT = '1'), EARNINGS_END_DT, NULL)` | ERROR('transformation error') | 29 | 9 |
| o_GVT_CURR_APT_AUTH1 | string | `IIF(IS_SPACES(GVT_CURR_APT_AUTH1), NULL, GVT_CURR_APT_AUTH1)` | ERROR('transformation error') | 3 | 0 |
| o_GVT_CURR_APT_AUTH2 | string | `IIF(IS_SPACES(GVT_CURR_APT_AUTH2), NULL, GVT_CURR_APT_AUTH2)` | ERROR('transformation error') | 3 | 0 |
| FEGLI_LIVING_BNFT_CD | string | `DECODE(GVT_NOA_CODE, '805', 'F',  '806', 'P', NULL) ` | ERROR('transformation error') | 1 | 0 |
| LWOP_START_DATE | date/time | `IIF(IN(GVT_NOA_CODE, '460', '473'), EFFDT, NULL)` | ERROR('transformation error') | 29 | 9 |
| BUYOUT_EFFDT | date/time | `IIF(GVT_NOA_CODE = '825', EFFDT, NULL)` | ERROR('transformation error') | 29 | 9 |
| o_BUSINESS_TITLE | string | `UPPER(BUSINESS_TITLE)` | ERROR('transformation error') | 30 | 0 |
| o_BUYOUT_AMT | decimal | `IIF(GVT_NOA_CODE= '825' and ACTION='BON', GOAL_AMT,NULL)` | ERROR('transformation error') | 14 | 4 |
| o_APPT_LIMIT_NTE_HRS | decimal | `IIF(GVT_APPT_LIMIT_HRS=0,NULL, GVT_APPT_LIMIT_HRS)` | ERROR('transformation error') | 38 | 0 |
| YEAR_DEGREE | decimal | `IIF(JPM_INTEGER_2=0, NULL, JPM_INTEGER_2)` | ERROR('transformation error') | 38 | 0 |
| o_SEVERANCE_PAY_AMT | decimal | `IIF(IN (GVT_NOA_CODE,'304', '312', '356' , '357') ,OTH_PAY,NULL)` | ERROR('transformation error') | 8 | 2 |
| o_SEVERANCE_TOTAL_AMT | decimal | `IIF(IN (GVT_NOA_CODE,'304', '312', '356' , '357') ,GOAL_AMT,NULL)` | ERROR('transformation error') | 7 | 2 |
| o_SEVERANCE_PAY_START_DTE | date/time | `IIF(IN (GVT_NOA_CODE,'304', '312', '356' , '357') ,EFFDT,NULL)` | ERROR('transformation error') | 29 | 9 |
| o_PROBATION_DT | date/time | `IIF(ISNULL(PROBATION_DT), NULL, ADD_TO_DATE(PROBATION_DT, 'MM', -12))` | ERROR('transformation error') | 29 | 9 |

---

### 4.7. lkp_PS_GVT_AWD_DATA (Type: Lookup Procedure)

**Field Count**: 30

**Type**: Lookup Procedure - Database lookup with caching

- **Lookup Table**: `PS_GVT_AWD_DATA`
- **Lookup Condition**: `EMPLID = EMPLID_IN AND EMPL_RCD = EMPL_RCD_IN AND EFFDT = EFFDT_IN AND EFFSEQ = EFFSEQ_IN`
- **Connection**: `$Source`
- **Caching Enabled**: YES
- **Multiple Match Policy**: Use Any Value

**Input Ports**:

| Port Name | Datatype | Precision | Scale |
|-----------|----------|-----------|-------|
| EMPLID_IN | string | 11 | 0 |
| EMPL_RCD_IN | decimal | 38 | 0 |
| EFFDT_IN | date/time | 29 | 9 |
| EFFSEQ_IN | decimal | 38 | 0 |

**Output / Lookup-Output Ports (Return Fields)**:

| Port Name | Datatype | Port Type | Precision | Scale |
|-----------|----------|-----------|-----------|-------|
| EMPLID | string | LOOKUP/OUTPUT | 11 | 0 |
| EMPL_RCD | decimal | LOOKUP/OUTPUT | 38 | 0 |
| EFFDT | date/time | LOOKUP/OUTPUT | 29 | 9 |
| EFFSEQ | decimal | LOOKUP/OUTPUT | 38 | 0 |
| GVT_AWD_CLASS | string | LOOKUP/OUTPUT | 1 | 0 |
| OTH_PAY | decimal | LOOKUP/OUTPUT | 10 | 2 |
| OTH_HRS | decimal | LOOKUP/OUTPUT | 6 | 2 |
| GOAL_AMT | decimal | LOOKUP/OUTPUT | 10 | 2 |
| EARNINGS_END_DT | date/time | LOOKUP/OUTPUT | 29 | 9 |
| GVT_USE_BY_DATE | date/time | LOOKUP/OUTPUT | 29 | 9 |
| GVT_AWARD_GROUP | string | LOOKUP/OUTPUT | 4 | 0 |
| GVT_TANG_BEN_AMT | decimal | LOOKUP/OUTPUT | 38 | 0 |
| GVT_INTANG_BEN_AMT | decimal | LOOKUP/OUTPUT | 38 | 0 |
| GVT_SUGGESTION_NBR | string | LOOKUP/OUTPUT | 10 | 0 |
| GVT_OBLIG_EXPIR_DT | date/time | LOOKUP/OUTPUT | 29 | 9 |
| GL_PAY_TYPE | string | LOOKUP/OUTPUT | 6 | 0 |
| ACCT_CD | string | LOOKUP/OUTPUT | 25 | 0 |
| ERNCD | string | LOOKUP/OUTPUT | 3 | 0 |
| GVT_SEPCHK_IND | string | LOOKUP/OUTPUT | 1 | 0 |
| SEPCHK | decimal | LOOKUP/OUTPUT | 38 | 0 |
| DED_TAKEN | string | LOOKUP/OUTPUT | 1 | 0 |
| DED_TAKEN_GENL | string | LOOKUP/OUTPUT | 1 | 0 |
| DISABLE_DIR_DEP | string | LOOKUP/OUTPUT | 1 | 0 |
| GROSSUP | string | LOOKUP/OUTPUT | 1 | 0 |
| ADDL_SEQ | decimal | LOOKUP/OUTPUT | 38 | 0 |
| COMMENTS | text | LOOKUP/OUTPUT | 4000 | 0 |

**Cache Configuration**:

- Lookup Data Cache Size: `Auto`
- Lookup Index Cache Size: `Auto`
- Dynamic Lookup Cache: `NO`
- Lookup cache directory name: `$PMCacheDir`
- Lookup cache persistent: `NO`
- Pre-build lookup cache: `Auto`
- Sorted Input: `NO`
- Case Sensitive String Comparison: `NO`

---

### 4.8. lkp_PS_GVT_EE_DATA_TRK (Type: Lookup Procedure)

**Field Count**: 18

**Type**: Lookup Procedure - Database lookup with caching

- **Lookup Table**: `PS_GVT_EE_DATA_TRK`
- **Lookup Condition**: `EMPLID = EMPLID_IN AND EMPL_RCD = EMPL_RCD_IN AND EFFDT = EFFDT_IN AND EFFSEQ = EFFSEQ_IN`
- **Connection**: `$Source`
- **Caching Enabled**: YES
- **Multiple Match Policy**: Use Any Value

**Lookup SQL Override**:
```sql
SELECT DISTINCT A.GVT_DATE_WRK as GVT_DATE_WRK, A.EMPLID as EMPLID, A.EMPL_RCD as EMPL_RCD, A.EFFDT as EFFDT, A.EFFSEQ as EFFSEQ, A.GVT_WIP_STATUS as GVT_WIP_STATUS  FROM PS_GVT_EE_DATA_TRK A
```

**Input Ports**:

| Port Name | Datatype | Precision | Scale |
|-----------|----------|-----------|-------|
| EMPLID_IN | string | 11 | 0 |
| EMPL_RCD_IN | decimal | 38 | 0 |
| EFFDT_IN | date/time | 29 | 9 |
| EFFSEQ_IN | decimal | 38 | 0 |
| GVT_WIP_STATUS_IN | string | 3 | 0 |

**Output / Lookup-Output Ports (Return Fields)**:

| Port Name | Datatype | Port Type | Precision | Scale |
|-----------|----------|-----------|-----------|-------|
| GVT_DATE_WRK | date/time | LOOKUP/OUTPUT | 29 | 9 |

**Lookup-Only Ports (cached but not returned)**:

| Port Name | Datatype | Precision | Scale |
|-----------|----------|-----------|-------|
| EMPLID | string | 11 | 0 |
| EMPL_RCD | decimal | 38 | 0 |
| EFFDT | date/time | 29 | 9 |
| EFFSEQ | decimal | 38 | 0 |
| GVT_TRK_SEQUENCE | decimal | 38 | 0 |
| GVT_WIP_STATUS | string | 3 | 0 |
| OPRID | string | 30 | 0 |
| GVT_OPRID_OVERRIDE | string | 1 | 0 |
| GVT_TRK_EMPLID | string | 11 | 0 |
| GVT_TRK_EMPL_RCD | decimal | 38 | 0 |
| GVT_EE_TRK_DT | date/time | 29 | 9 |
| COMMENTS | text | 4000 | 0 |

**Cache Configuration**:

- Lookup Data Cache Size: `Auto`
- Lookup Index Cache Size: `Auto`
- Dynamic Lookup Cache: `NO`
- Lookup cache directory name: `$PMCacheDir`
- Lookup cache persistent: `NO`
- Pre-build lookup cache: `Auto`
- Sorted Input: `NO`
- Case Sensitive String Comparison: `NO`

---

### 4.9. lkp_PS_HE_FILL_POS (Type: Lookup Procedure)

**Field Count**: 9

**Type**: Lookup Procedure - Database lookup with caching

- **Lookup Table**: `PS_HE_FILL_POS`
- **Lookup Condition**: `EMPLID = EMPLID_IN AND EMPL_RCD = EMPL_RCD_IN AND EFFDT = EFFDT_IN AND EFFSEQ = EFFSEQ_IN`
- **Connection**: `$Source`
- **Caching Enabled**: YES
- **Multiple Match Policy**: Use Any Value

**Input Ports**:

| Port Name | Datatype | Precision | Scale |
|-----------|----------|-----------|-------|
| EMPLID_IN | string | 11 | 0 |
| EMPL_RCD_IN | decimal | 38 | 0 |
| EFFDT_IN | date/time | 29 | 9 |
| EFFSEQ_IN | decimal | 38 | 0 |

**Output / Lookup-Output Ports (Return Fields)**:

| Port Name | Datatype | Port Type | Precision | Scale |
|-----------|----------|-----------|-----------|-------|
| HE_FILL_POSITION | string | LOOKUP/OUTPUT | 1 | 0 |

**Lookup-Only Ports (cached but not returned)**:

| Port Name | Datatype | Precision | Scale |
|-----------|----------|-----------|-------|
| EMPLID | string | 11 | 0 |
| EMPL_RCD | decimal | 38 | 0 |
| EFFDT | date/time | 29 | 9 |
| EFFSEQ | decimal | 38 | 0 |

**Cache Configuration**:

- Lookup Data Cache Size: `Auto`
- Lookup Index Cache Size: `Auto`
- Dynamic Lookup Cache: `NO`
- Lookup cache directory name: `$PMCacheDir`
- Lookup cache persistent: `NO`
- Pre-build lookup cache: `Auto`
- Sorted Input: `NO`
- Case Sensitive String Comparison: `NO`

---

### 4.10. lkp_PS_GVT_CITIZENSHIP (Type: Lookup Procedure)

**Field Count**: 11

**Type**: Lookup Procedure - Database lookup with caching

- **Lookup Table**: `PS_GVT_CITIZENSHIP`
- **Lookup Condition**: `EMPLID = EMPLID_IN AND EMPL_RCD = EMPL_RCD_IN AND EFFDT = EFFDT_IN AND EFFSEQ = EFFSEQ_IN`
- **Connection**: `$Source`
- **Caching Enabled**: YES
- **Multiple Match Policy**: Use Any Value

**Input Ports**:

| Port Name | Datatype | Precision | Scale |
|-----------|----------|-----------|-------|
| EMPLID_IN | string | 11 | 0 |
| EMPL_RCD_IN | decimal | 38 | 0 |
| EFFDT_IN | date/time | 29 | 9 |
| EFFSEQ_IN | decimal | 38 | 0 |

**Output / Lookup-Output Ports (Return Fields)**:

| Port Name | Datatype | Port Type | Precision | Scale |
|-----------|----------|-----------|-----------|-------|
| CITIZENSHIP_STATUS | string | LOOKUP/OUTPUT | 1 | 0 |

**Lookup-Only Ports (cached but not returned)**:

| Port Name | Datatype | Precision | Scale |
|-----------|----------|-----------|-------|
| EMPLID | string | 11 | 0 |
| EMPL_RCD | decimal | 38 | 0 |
| EFFDT | date/time | 29 | 9 |
| EFFSEQ | decimal | 38 | 0 |
| DEPENDENT_ID | string | 2 | 0 |
| COUNTRY | string | 3 | 0 |

**Cache Configuration**:

- Lookup Data Cache Size: `Auto`
- Lookup Index Cache Size: `Auto`
- Dynamic Lookup Cache: `NO`
- Lookup cache directory name: `$PMCacheDir`
- Lookup cache persistent: `NO`
- Pre-build lookup cache: `Auto`
- Sorted Input: `NO`
- Case Sensitive String Comparison: `NO`

---

### 4.11. lkp_PS_GVT_PERS_DATA (Type: Lookup Procedure)

**Field Count**: 125

**Type**: Lookup Procedure - Database lookup with caching

- **Lookup Table**: `PS_GVT_PERS_DATA`
- **Lookup Condition**: `EMPLID = EMPLID_IN AND EMPL_RCD = EMPL_RCD_IN AND EFFDT = EFFDT_IN AND EFFSEQ = EFFSEQ_IN`
- **Connection**: `$Source`
- **Caching Enabled**: YES
- **Multiple Match Policy**: Use Any Value

**Lookup SQL Override**:
```sql
SELECT  	p.LAST_NAME as LAST_NAME,  	p.FIRST_NAME as FIRST_NAME,  	p.MIDDLE_NAME as MIDDLE_NAME,  	p.ADDRESS1 as ADDRESS1,  	p.CITY as CITY,  	p.STATE as STATE,  	p.POSTAL as POSTAL,  	p.GEO_CODE as GEO_CODE,  	p.SEX as SEX,  	p.BIRTHDATE as BIRTHDATE,  	p.MILITARY_STATUS as MILITARY_STATUS,  	p.GVT_CRED_MIL_SVCE as GVT_CRED_MIL_SVCE,  	p.GVT_MILITARY_COMP as GVT_MILITARY_COMP,  	p.GVT_MIL_RESRVE_CAT as GVT_MIL_RESRVE_CAT,  	p.GVT_VET_PREF_APPT as GVT_VET_PREF_APPT,  	p.ETHNIC_GROUP as ETHNIC_GROUP,  	p.GVT_DISABILITY_CD as GVT_DISABILITY_CD,  	p.EMPLID as EMPLID,  	p.EMPL_RCD as EMPL_RCD,  	p.EFFDT as EFFDT,  	p.EFFSEQ as EFFSEQ  FROM  	PS_GVT_PERS_DATA p, NWK_NEW_EHRP_ACTIONS_TBL n WHERE      p.EMPLID = n.EMPLID and     p.EMPL_RCD = n.EMPL_RCD and    p.EFFDT = n.EFFDT and    p.EFFSEQ = n.EFFSEQ
```

**Input Ports**:

| Port Name | Datatype | Precision | Scale |
|-----------|----------|-----------|-------|
| EMPLID_IN | string | 11 | 0 |
| EMPL_RCD_IN | decimal | 38 | 0 |
| EFFDT_IN | date/time | 29 | 9 |
| EFFSEQ_IN | decimal | 38 | 0 |

**Output / Lookup-Output Ports (Return Fields)**:

| Port Name | Datatype | Port Type | Precision | Scale |
|-----------|----------|-----------|-----------|-------|
| LAST_NAME | string | LOOKUP/OUTPUT | 30 | 0 |
| FIRST_NAME | string | LOOKUP/OUTPUT | 30 | 0 |
| MIDDLE_NAME | string | LOOKUP/OUTPUT | 30 | 0 |
| ADDRESS1 | string | LOOKUP/OUTPUT | 55 | 0 |
| CITY | string | LOOKUP/OUTPUT | 30 | 0 |
| STATE | string | LOOKUP/OUTPUT | 6 | 0 |
| POSTAL | string | LOOKUP/OUTPUT | 12 | 0 |
| GEO_CODE | string | LOOKUP/OUTPUT | 11 | 0 |
| SEX | string | LOOKUP/OUTPUT | 1 | 0 |
| BIRTHDATE | date/time | LOOKUP/OUTPUT | 29 | 9 |
| MILITARY_STATUS | string | LOOKUP/OUTPUT | 1 | 0 |
| GVT_CRED_MIL_SVCE | string | LOOKUP/OUTPUT | 6 | 0 |
| GVT_MILITARY_COMP | string | LOOKUP/OUTPUT | 1 | 0 |
| GVT_MIL_RESRVE_CAT | string | LOOKUP/OUTPUT | 1 | 0 |
| GVT_VET_PREF_APPT | string | LOOKUP/OUTPUT | 1 | 0 |
| ETHNIC_GROUP | string | LOOKUP/OUTPUT | 1 | 0 |
| GVT_DISABILITY_CD | string | LOOKUP/OUTPUT | 2 | 0 |

**Lookup-Only Ports (cached but not returned)**:

| Port Name | Datatype | Precision | Scale |
|-----------|----------|-----------|-------|
| EMPLID | string | 11 | 0 |
| EMPL_RCD | decimal | 38 | 0 |
| EFFDT | date/time | 29 | 9 |
| EFFSEQ | decimal | 38 | 0 |
| COUNTRY_NM_FORMAT | string | 3 | 0 |
| NAME | string | 50 | 0 |
| NAME_INITIALS | string | 6 | 0 |
| NAME_PREFIX | string | 4 | 0 |
| NAME_SUFFIX | string | 15 | 0 |
| NAME_ROYAL_PREFIX | string | 15 | 0 |
| NAME_ROYAL_SUFFIX | string | 15 | 0 |
| NAME_TITLE | string | 30 | 0 |
| LAST_NAME_SRCH | string | 30 | 0 |
| FIRST_NAME_SRCH | string | 30 | 0 |
| SECOND_LAST_NAME | string | 30 | 0 |
| SECOND_LAST_SRCH | string | 30 | 0 |
| NAME_AC | string | 50 | 0 |
| PREF_FIRST_NAME | string | 30 | 0 |
| PARTNER_LAST_NAME | string | 30 | 0 |
| PARTNER_ROY_PREFIX | string | 15 | 0 |
| LAST_NAME_PREF_NLD | string | 1 | 0 |
| NAME_DISPLAY | string | 50 | 0 |
| NAME_FORMAL | string | 60 | 0 |
| COUNTRY | string | 3 | 0 |
| ADDRESS2 | string | 55 | 0 |
| ADDRESS3 | string | 55 | 0 |
| ADDRESS4 | string | 55 | 0 |
| NUM1 | string | 6 | 0 |
| NUM2 | string | 6 | 0 |
| HOUSE_TYPE | string | 2 | 0 |
| ADDR_FIELD1 | string | 2 | 0 |
| ADDR_FIELD2 | string | 4 | 0 |
| ADDR_FIELD3 | string | 4 | 0 |
| COUNTY | string | 30 | 0 |
| IN_CITY_LIMIT | string | 1 | 0 |
| COUNTRY_OTHER | string | 3 | 0 |
| ADDRESS1_OTHER | string | 55 | 0 |
| ADDRESS2_OTHER | string | 55 | 0 |
| ADDRESS3_OTHER | string | 55 | 0 |
| ADDRESS4_OTHER | string | 55 | 0 |
| CITY_OTHER | string | 30 | 0 |
| COUNTY_OTHER | string | 30 | 0 |
| STATE_OTHER | string | 6 | 0 |
| POSTAL_OTHER | string | 12 | 0 |
| NUM1_OTHER | string | 6 | 0 |
| NUM2_OTHER | string | 6 | 0 |
| HOUSE_TYPE_OTHER | string | 2 | 0 |
| ADDR_FIELD1_OTHER | string | 2 | 0 |
| ADDR_FIELD2_OTHER | string | 4 | 0 |
| ADDR_FIELD3_OTHER | string | 4 | 0 |
| IN_CITY_LMT_OTHER | string | 1 | 0 |
| GEO_CODE_OTHER | string | 11 | 0 |
| COUNTRY_CODE | string | 3 | 0 |
| PHONE | string | 24 | 0 |
| PER_ORG | string | 3 | 0 |
| ORIG_HIRE_DT | date/time | 29 | 9 |
| MAR_STATUS | string | 1 | 0 |
| MAR_STATUS_DT | date/time | 29 | 9 |
| BIRTHPLACE | string | 30 | 0 |
| BIRTHCOUNTRY | string | 3 | 0 |
| BIRTHSTATE | string | 6 | 0 |
| DT_OF_DEATH | date/time | 29 | 9 |
| HIGHEST_EDUC_LVL | string | 2 | 0 |
| FT_STUDENT | string | 1 | 0 |
| US_WORK_ELIGIBILTY | string | 1 | 0 |
| CITIZEN_PROOF1 | string | 10 | 0 |
| CITIZEN_PROOF2 | string | 10 | 0 |
| SMOKER | string | 1 | 0 |
| MEDICARE_ENTLD_DT | date/time | 29 | 9 |
| SMOKER_DT | date/time | 29 | 9 |
| ADDRESS1_AC | string | 55 | 0 |
| ADDRESS2_AC | string | 55 | 0 |
| ADDRESS3_AC | string | 55 | 0 |
| CITY_AC | string | 30 | 0 |
| LANG_CD | string | 3 | 0 |
| YEARS_OF_EXP | decimal | 4 | 1 |
| GVT_MIL_GRADE | string | 3 | 0 |
| GVT_MIL_SEP_RET | string | 1 | 0 |
| GVT_MIL_SVCE_END | date/time | 29 | 9 |
| GVT_MIL_SVCE_START | date/time | 29 | 9 |
| GVT_MIL_VERIFY | string | 1 | 0 |
| GVT_PAR_NBR_LAST | decimal | 38 | 0 |
| GVT_UNIF_SVC_CTR | string | 1 | 0 |
| GVT_VET_PREF_RIF | string | 1 | 0 |
| GVT_CHANGE_FLAG | string | 1 | 0 |
| GVT_DRAFT_STATUS | string | 1 | 0 |
| GVT_YR_ATTAINED | date/time | 29 | 9 |
| DISABLED_VET | string | 1 | 0 |
| DISABLED | string | 1 | 0 |
| GRADE | string | 3 | 0 |
| SAL_ADMIN_PLAN | string | 4 | 0 |
| GVT_CURR_AGCY_EMPL | string | 1 | 0 |
| GVT_CURR_FED_EMPL | string | 1 | 0 |
| GVT_HIGH_PAY_PLAN | string | 2 | 0 |
| GVT_HIGH_GRADE | string | 3 | 0 |
| GVT_PREV_AGCY_EMPL | string | 1 | 0 |
| GVT_PREV_FED_EMPL | string | 1 | 0 |
| GVT_SEP_INCENTIVE | string | 1 | 0 |
| GVT_SEP_INCENT_DT | date/time | 29 | 9 |
| GVT_TENURE | string | 1 | 0 |
| GVT_PAY_PLAN | string | 2 | 0 |
| BARG_UNIT | string | 4 | 0 |
| ALTER_EMPLID | string | 11 | 0 |
| HE_ERI | string | 6 | 0 |

**Cache Configuration**:

- Lookup Data Cache Size: `Auto`
- Lookup Index Cache Size: `Auto`
- Dynamic Lookup Cache: `NO`
- Lookup cache directory name: `$PMCacheDir`
- Lookup cache persistent: `NO`
- Pre-build lookup cache: `Auto`
- Sorted Input: `NO`
- Case Sensitive String Comparison: `NO`

---

### 4.12. exp_PERS_DATA (Type: Expression)

**Field Count**: 17

**Type**: Expression - Business logic transformation

#### Input/Output Pass-Through Ports

| Port Name | Datatype | Expression | Precision | Scale |
|-----------|----------|------------|-----------|-------|
| BIRTHDATE | date/time | `BIRTHDATE` | 29 | 9 |
| FIRST_NAME | string | `FIRST_NAME` | 30 | 0 |
| LAST_NAME | string | `LAST_NAME` | 30 | 0 |
| MIDDLE_NAME | string | `MIDDLE_NAME` | 30 | 0 |
| CITY | string | `CITY` | 30 | 0 |
| STATE | string | `STATE` | 6 | 0 |
| GEO_CODE | string | `GEO_CODE` | 11 | 0 |
| POSTAL | string | `POSTAL` | 12 | 0 |
| ADDRESS1 | string | `ADDRESS1` | 55 | 0 |
| GVT_DISABILITY_CD | string | `GVT_DISABILITY_CD` | 2 | 0 |
| ETHNIC_GROUP | string | `ETHNIC_GROUP` | 1 | 0 |
| GVT_VET_PREF_APPT | string | `GVT_VET_PREF_APPT` | 1 | 0 |
| MILITARY_STATUS | string | `MILITARY_STATUS` | 1 | 0 |
| SEX | string | `SEX` | 1 | 0 |
| GVT_MILITARY_COMP | string | `GVT_MILITARY_COMP` | 1 | 0 |
| GVT_MIL_RESRVE_CAT | string | `GVT_MIL_RESRVE_CAT` | 1 | 0 |
| GVT_CRED_MIL_SVCE | string | `GVT_CRED_MIL_SVCE` | 6 | 0 |

---

### 4.13. lkp_PS_JPM_JP_ITEMS (Type: Lookup Procedure)

**Field Count**: 124

**Type**: Lookup Procedure - Database lookup with caching

- **Lookup Table**: `PS_JPM_JP_ITEMS`
- **Lookup Condition**: `JPM_PROFILE_ID = EMPLID_IN`
- **Connection**: `BIISPRD`
- **Caching Enabled**: YES
- **Multiple Match Policy**: Use Any Value

**Lookup SQL Override**:
```sql
SELECT PS_JPM_JP_ITEMS.JPM_CAT_TYPE as JPM_CAT_TYPE, PS_JPM_JP_ITEMS.JPM_CAT_ITEM_ID as JPM_CAT_ITEM_ID, PS_JPM_JP_ITEMS.JPM_CAT_ITEM_QUAL2 as JPM_CAT_ITEM_QUAL2, PS_JPM_JP_ITEMS.EFFDT as EFFDT, PS_JPM_JP_ITEMS.EFF_STATUS as EFF_STATUS, PS_JPM_JP_ITEMS.JPM_INTEGER_2 as JPM_INTEGER_2, PS_JPM_JP_ITEMS.MAJOR_CODE as MAJOR_CODE, PS_JPM_JP_ITEMS.MAJOR_DESCR as MAJOR_DESCR, PS_JPM_JP_ITEMS.LASTUPDOPRID as LASTUPDOPRID, PS_JPM_JP_ITEMS.JPM_PROFILE_ID as JPM_PROFILE_ID FROM EHRP.PS_JPM_JP_ITEMS WHERE  jpm_cat_type = 'DEG' AND eff_status = 'A'
```

**Input Ports**:

| Port Name | Datatype | Precision | Scale |
|-----------|----------|-----------|-------|
| EMPLID_IN | string | 10 | 0 |

**Output / Lookup-Output Ports (Return Fields)**:

| Port Name | Datatype | Port Type | Precision | Scale |
|-----------|----------|-----------|-----------|-------|
| JPM_PROFILE_ID | string | LOOKUP/OUTPUT | 12 | 0 |
| JPM_CAT_TYPE | string | LOOKUP/OUTPUT | 12 | 0 |
| JPM_CAT_ITEM_ID | string | LOOKUP/OUTPUT | 12 | 0 |
| JPM_CAT_ITEM_QUAL2 | string | LOOKUP/OUTPUT | 12 | 0 |
| EFFDT | date/time | LOOKUP/OUTPUT | 29 | 9 |
| EFF_STATUS | string | LOOKUP/OUTPUT | 1 | 0 |
| JPM_INTEGER_2 | decimal | LOOKUP/OUTPUT | 38 | 0 |
| MAJOR_CODE | string | LOOKUP/OUTPUT | 10 | 0 |
| MAJOR_DESCR | string | LOOKUP/OUTPUT | 100 | 0 |
| LASTUPDOPRID | string | LOOKUP/OUTPUT | 30 | 0 |

**Lookup-Only Ports (cached but not returned)**:

| Port Name | Datatype | Precision | Scale |
|-----------|----------|-----------|-------|
| JPM_CAT_ITEM_QUAL | string | 12 | 0 |
| JPM_ITEM_KEY_ID | decimal | 12 | 0 |
| JPM_JP_ITEM_SRC | string | 4 | 0 |
| JPM_SOURCE_ID1 | string | 12 | 0 |
| JPM_SOURCE_ID2 | string | 12 | 0 |
| JPM_SOURCE_ID3 | string | 12 | 0 |
| JPM_JP_QUAL_SET | string | 12 | 0 |
| JPM_JP_QUAL_SET2 | string | 12 | 0 |
| JPM_PARENT_KEY_ID | decimal | 12 | 0 |
| JPM_WF_STATUS | string | 2 | 0 |
| COUNTRY | string | 3 | 0 |
| JPM_DATE_1 | date/time | 29 | 9 |
| JPM_DATE_2 | date/time | 29 | 9 |
| JPM_EVAL_MTHD | string | 4 | 0 |
| JPM_TEXT1325_1 | string | 1325 | 0 |
| JPM_TEXT1325_2 | string | 1325 | 0 |
| JPM_TEXT254_1 | string | 254 | 0 |
| JPM_TEXT254_2 | string | 254 | 0 |
| JPM_TEXT254_3 | string | 254 | 0 |
| JPM_TEXT254_4 | string | 254 | 0 |
| JPM_YN_1 | string | 1 | 0 |
| JPM_YN_2 | string | 1 | 0 |
| JPM_YN_3 | string | 1 | 0 |
| JPM_YN_4 | string | 1 | 0 |
| JPM_YN_5 | string | 1 | 0 |
| RATING_MODEL | string | 4 | 0 |
| JPM_INTEREST_LEVEL | string | 1 | 0 |
| JPM_MANDATORY | string | 1 | 0 |
| JPM_RATING1 | string | 1 | 0 |
| JPM_RATING2 | string | 1 | 0 |
| JPM_RATING3 | string | 1 | 0 |
| JPM_TEXT254_5 | string | 254 | 0 |
| JPM_DATE_3 | date/time | 29 | 9 |
| JPM_DATE_4 | date/time | 29 | 9 |
| JPM_DATE_5 | date/time | 29 | 9 |
| JPM_DATE_6 | date/time | 29 | 9 |
| JPM_INTEGER_1 | decimal | 38 | 0 |
| JPM_PCT_1 | decimal | 38 | 0 |
| JPM_PCT_2 | decimal | 38 | 0 |
| JPM_DECIMAL_1 | decimal | 9 | 2 |
| JPM_DECIMAL_2 | decimal | 9 | 2 |
| JPM_PROMPT_1 | string | 12 | 0 |
| JPM_PROMPT_2 | string | 12 | 0 |
| JPM_PROMPT_3 | string | 12 | 0 |
| JPM_PROMPT_4 | string | 12 | 0 |
| JPM_PROMPT_5 | string | 12 | 0 |
| JPM_PROMPT_6 | string | 12 | 0 |
| JPM_PROMPT_7 | string | 12 | 0 |
| JPM_PROMPT_8 | string | 12 | 0 |
| JPM_PROMPT_9 | string | 12 | 0 |
| JPM_PROMPT_10 | string | 12 | 0 |
| JPM_PROMPT_11 | string | 12 | 0 |
| JPM_PROMPT_12 | string | 12 | 0 |
| JPM_PROMPT_13 | string | 12 | 0 |
| JPM_PROMPT_14 | string | 12 | 0 |
| JPM_PROMPT_15 | string | 12 | 0 |
| JPM_PROMPT_16 | string | 12 | 0 |
| JPM_PROMPT_17 | string | 12 | 0 |
| JPM_PROMPT_18 | string | 12 | 0 |
| JPM_PROMPT_19 | string | 12 | 0 |
| JPM_PROMPT_20 | string | 12 | 0 |
| JPM_PERSON_ID_1 | string | 11 | 0 |
| JPM_IMPORTANCE | string | 1 | 0 |
| JPM_VERIFY_METHOD | string | 1 | 0 |
| STATE | string | 6 | 0 |
| NVQ_STATUS | string | 1 | 0 |
| SCHOOL_CODE | string | 10 | 0 |
| SCHOOL_DESCR | string | 100 | 0 |
| JPM_MINOR_CD | string | 10 | 0 |
| MINOR_DESCR | string | 100 | 0 |
| AVERAGE_GRADE | string | 5 | 0 |
| PRACTIC_GRADE_GER | string | 4 | 0 |
| THEORY_GRADE_GER | string | 4 | 0 |
| IPE_SW | string | 1 | 0 |
| EVALUATION_ID | string | 2 | 0 |
| GVT_CREDIT_HOURS | string | 3 | 0 |
| GVT_CRED_HRS_TYPE | string | 1 | 0 |
| EDUC_LVL_AUS | string | 1 | 0 |
| APS_HEDUC_CD_AUS | string | 2 | 0 |
| FACULTY_CODE | string | 10 | 0 |
| FACULTY_DESCR | string | 100 | 0 |
| SUBFACULTY_CODE | string | 10 | 0 |
| SUBFACULTY_DESCR | string | 100 | 0 |
| MAJOR_CATEGORY | string | 1 | 0 |
| FP_SUBJECT_CD | string | 3 | 0 |
| EP_APPRAISAL_ID | decimal | 38 | 0 |
| SCHOOL_TYPE | string | 3 | 0 |
| SETID_DEPT | string | 5 | 0 |
| DEPTID | string | 10 | 0 |
| BUSINESS_UNIT | string | 5 | 0 |
| SETID_LOCATION | string | 5 | 0 |
| LOCATION | string | 10 | 0 |
| JPM_ADHOC_DESCR | string | 80 | 0 |
| FP_SKIL_HIR | string | 1 | 0 |
| FP_SKIL_PRM | string | 1 | 0 |
| FP_SKIL_TEN | string | 1 | 0 |
| FP_DEGR_REQUIRED | string | 1 | 0 |
| BONUS_AMOUNT_FRA | decimal | 7 | 2 |
| BONUS_DT_FRA | date/time | 29 | 9 |
| JPM_OBSTACLE_1 | string | 2 | 0 |
| JPM_AREA_PREF_1 | string | 2 | 0 |
| JPM_AREA_PREF_2 | string | 2 | 0 |
| JPM_AREA_PREF_3 | string | 2 | 0 |
| JPM_CNTRY_PREF_1 | string | 3 | 0 |
| JPM_CNTRY_PREF_2 | string | 3 | 0 |
| JPM_CNTRY_PREF_3 | string | 3 | 0 |
| JPM_LOC_BUNIT_1 | string | 5 | 0 |
| JPM_SETID_LOC_1 | string | 5 | 0 |
| JPM_LOCATION_1 | string | 10 | 0 |
| JPM_LOC_BUNIT_2 | string | 5 | 0 |
| JPM_SETID_LOC_2 | string | 5 | 0 |
| JPM_LOCATION_2 | string | 10 | 0 |
| LASTUPDDTTM | date/time | 29 | 9 |

**Cache Configuration**:

- Lookup Data Cache Size: `Auto`
- Lookup Index Cache Size: `Auto`
- Dynamic Lookup Cache: `NO`
- Lookup cache directory name: `$PMCacheDir`
- Lookup cache persistent: `NO`
- Pre-build lookup cache: `Auto`
- Sorted Input: `NO`
- Case Sensitive String Comparison: `NO`

---

## 5. Data Flow DAG

### 5.1. High-Level Data Flow

```
+---------------------------------+    +---------------------------------+
| NWK_NEW_EHRP_ACTIONS_TBL        |    | PS_GVT_JOB                      |
| (Source)                         |    | (Source)                         |
+-----------------+---------------+    +-----------------+---------------+
                  |                                      |
                  +----------------+---------------------+
                                   |
                                   v
                    +----------------------------+
                    | SQ_PS_GVT_JOB              |
                    | (Source Qualifier)          |
                    | Joins both sources          |
                    +-------------+--------------+
                                  |
              +-------------------+-------------------+
              |                   |                   |
              v                   v                   v
    +----------------+  +----------------+  +--------------------+
    | exp_GET_EFFDT   |  | Lookup         |  | exp_MAIN2BIIS      |
    | _YEAR           |  | Transforms (8) |  | (Main Expression)  |
    +-------+--------+  +-------+--------+  +--------+-----+-----+
            |                    |                    |     |     |
            v                    |                    |     |     |
    +----------------+          |                    |     |     |
    | lkp_OLD_SEQ_   |          |                    |     |     |
    | NUMBER          +--------->                    |     |     |
    +----------------+                               |     |     |
                                                     |     |     |
              +------------------+                   |     |     |
              | lkp_PS_GVT_      |                   |     |     |
              | PERS_DATA -----> exp_PERS_DATA ----->+     |     |
              +-----------------+                    |     |     |
                                                     v     v     v
                                           +---------+ +---+---+ +----------+
                                           |EHRP_RECS| |NWK_ACT| |NWK_ACTION|
                                           |TRACKING | |PRIMARY| |SECONDARY |
                                           |_TBL     | |_TBL   | |_TBL      |
                                           +---------+ +-------+ +----------+
```

### 5.2. Lookup Transforms Feed Into Main Expression

| Lookup | Source Table | Key Fields | Purpose |
|--------|-------------|------------|---------|
| lkp_OLD_SEQUENCE_NUMBER | SEQUENCE_NUM_TBL | YEAR_IN | Get sequence number for EVENT_ID |
| lkp_PS_GVT_EMPLOYMENT | PS_GVT_EMPLOYMENT | EMPLID, EMPL_RCD, EFFDT, EFFSEQ | Employment SCD/hire dates |
| lkp_PS_GVT_PERS_NID | PS_GVT_PERS_NID | EMPLID | National ID (SSN) |
| lkp_PS_GVT_AWD_DATA | PS_GVT_AWD_DATA | EMPLID, EMPL_RCD, EFFDT, EFFSEQ | Award data |
| lkp_PS_GVT_EE_DATA_TRK | PS_GVT_EE_DATA_TRK | EMPLID | Employee data tracking |
| lkp_PS_HE_FILL_POS | PS_HE_FILL_POS | POSITION_NBR | Fill position data |
| lkp_PS_GVT_CITIZENSHIP | PS_GVT_CITIZENSHIP | EMPLID | Citizenship status |
| lkp_PS_GVT_PERS_DATA | PS_GVT_PERS_DATA | EMPLID | Personal data (name, DOB, etc.) |
| lkp_PS_JPM_JP_ITEMS | PS_JPM_JP_ITEMS | JPM_PROFILE_ID (=EMPLID) | Education/degree data |

### 5.3. Detailed Data Flow Edges (CONNECTOR elements)

**Total Connectors**: 589

| # | From Instance | From Type | To Instance | To Type | Field Mappings |
|---|---------------|-----------|-------------|---------|----------------|
| 1 | PS_GVT_JOB | Source Definition | SQ_PS_GVT_JOB | Source Qualifier | 246 |
| 2 | SQ_PS_GVT_JOB | Source Qualifier | exp_MAIN2BIIS | Expression | 97 |
| 3 | exp_MAIN2BIIS | Expression | NWK_ACTION_PRIMARY_TBL | Target Definition | 93 |
| 4 | exp_MAIN2BIIS | Expression | NWK_ACTION_SECONDARY_TBL | Target Definition | 32 |
| 5 | lkp_PS_GVT_PERS_DATA | Lookup Procedure | exp_PERS_DATA | Expression | 17 |
| 6 | lkp_PS_GVT_EMPLOYMENT | Lookup Procedure | NWK_ACTION_PRIMARY_TBL | Target Definition | 14 |
| 7 | exp_PERS_DATA | Expression | NWK_ACTION_PRIMARY_TBL | Target Definition | 13 |
| 8 | lkp_PS_GVT_EMPLOYMENT | Lookup Procedure | exp_MAIN2BIIS | Expression | 13 |
| 9 | SQ_PS_GVT_JOB | Source Qualifier | EHRP_RECS_TRACKING_TBL | Target Definition | 7 |
| 10 | lkp_PS_GVT_AWD_DATA | Lookup Procedure | exp_MAIN2BIIS | Expression | 6 |
| 11 | SQ_PS_GVT_JOB | Source Qualifier | lkp_PS_GVT_EE_DATA_TRK | Lookup Procedure | 5 |
| 12 | NWK_NEW_EHRP_ACTIONS_TBL | Source Definition | SQ_PS_GVT_JOB | Source Qualifier | 4 |
| 13 | SQ_PS_GVT_JOB | Source Qualifier | lkp_PS_GVT_EMPLOYMENT | Lookup Procedure | 4 |
| 14 | SQ_PS_GVT_JOB | Source Qualifier | lkp_PS_GVT_PERS_NID | Lookup Procedure | 4 |
| 15 | SQ_PS_GVT_JOB | Source Qualifier | lkp_PS_GVT_AWD_DATA | Lookup Procedure | 4 |
| 16 | SQ_PS_GVT_JOB | Source Qualifier | lkp_PS_HE_FILL_POS | Lookup Procedure | 4 |
| 17 | SQ_PS_GVT_JOB | Source Qualifier | lkp_PS_GVT_CITIZENSHIP | Lookup Procedure | 4 |
| 18 | SQ_PS_GVT_JOB | Source Qualifier | lkp_PS_GVT_PERS_DATA | Lookup Procedure | 4 |
| 19 | exp_MAIN2BIIS | Expression | EHRP_RECS_TRACKING_TBL | Target Definition | 3 |
| 20 | exp_PERS_DATA | Expression | NWK_ACTION_SECONDARY_TBL | Target Definition | 2 |
| 21 | lkp_PS_JPM_JP_ITEMS | Lookup Procedure | NWK_ACTION_PRIMARY_TBL | Target Definition | 2 |
| 22 | exp_PERS_DATA | Expression | exp_MAIN2BIIS | Expression | 2 |
| 23 | lkp_PS_HE_FILL_POS | Lookup Procedure | NWK_ACTION_PRIMARY_TBL | Target Definition | 1 |
| 24 | lkp_PS_GVT_PERS_NID | Lookup Procedure | NWK_ACTION_PRIMARY_TBL | Target Definition | 1 |
| 25 | SQ_PS_GVT_JOB | Source Qualifier | exp_GET_EFFDT_YEAR | Expression | 1 |
| 26 | exp_GET_EFFDT_YEAR | Expression | lkp_OLD_SEQUENCE_NUMBER | Lookup Procedure | 1 |
| 27 | lkp_OLD_SEQUENCE_NUMBER | Lookup Procedure | exp_MAIN2BIIS | Expression | 1 |
| 28 | SQ_PS_GVT_JOB | Source Qualifier | lkp_PS_JPM_JP_ITEMS | Lookup Procedure | 1 |
| 29 | lkp_PS_GVT_CITIZENSHIP | Lookup Procedure | exp_MAIN2BIIS | Expression | 1 |
| 30 | lkp_PS_GVT_EE_DATA_TRK | Lookup Procedure | exp_MAIN2BIIS | Expression | 1 |
| 31 | lkp_PS_JPM_JP_ITEMS | Lookup Procedure | exp_MAIN2BIIS | Expression | 1 |

---

## 6. Mapping, Session, and Workflow

### 6.1. Mapping

- **Name**: `m_EHRP2BIIS_UPDATE`
- **Is Valid**: YES

**Mapping Instances**:

| Instance Name | Transformation Name | Transformation Type | Instance Type | DB Connection |
|---------------|--------------------|--------------------|---------------|---------------|
| NWK_ACTION_SECONDARY_TBL | NWK_ACTION_SECONDARY_TBL | Target Definition | TARGET |  |
| NWK_ACTION_PRIMARY_TBL | NWK_ACTION_PRIMARY_TBL | Target Definition | TARGET |  |
| EHRP_RECS_TRACKING_TBL | EHRP_RECS_TRACKING_TBL | Target Definition | TARGET |  |
| exp_GET_EFFDT_YEAR | exp_GET_EFFDT_YEAR | Expression | TRANSFORMATION |  |
| lkp_OLD_SEQUENCE_NUMBER | lkp_OLD_SEQUENCE_NUMBER | Lookup Procedure | TRANSFORMATION |  |
| PS_GVT_JOB | PS_GVT_JOB | Source Definition | SOURCE | ORA_BIISPRD_SRC |
| SQ_PS_GVT_JOB | SQ_PS_GVT_JOB | Source Qualifier | TRANSFORMATION |  (Sources: PS_GVT_JOB, NWK_NEW_EHRP_ACTIONS_TBL) |
| lkp_PS_GVT_EMPLOYMENT | lkp_PS_GVT_EMPLOYMENT | Lookup Procedure | TRANSFORMATION |  |
| lkp_PS_GVT_PERS_NID | lkp_PS_GVT_PERS_NID | Lookup Procedure | TRANSFORMATION |  |
| exp_MAIN2BIIS | exp_MAIN2BIIS | Expression | TRANSFORMATION |  |
| lkp_PS_GVT_AWD_DATA | lkp_PS_GVT_AWD_DATA | Lookup Procedure | TRANSFORMATION |  |
| lkp_PS_GVT_EE_DATA_TRK | lkp_PS_GVT_EE_DATA_TRK | Lookup Procedure | TRANSFORMATION |  |
| lkp_PS_HE_FILL_POS | lkp_PS_HE_FILL_POS | Lookup Procedure | TRANSFORMATION |  |
| lkp_PS_GVT_CITIZENSHIP | lkp_PS_GVT_CITIZENSHIP | Lookup Procedure | TRANSFORMATION |  |
| lkp_PS_GVT_PERS_DATA | lkp_PS_GVT_PERS_DATA | Lookup Procedure | TRANSFORMATION |  |
| exp_PERS_DATA | exp_PERS_DATA | Expression | TRANSFORMATION |  |
| NWK_NEW_EHRP_ACTIONS_TBL | NWK_NEW_EHRP_ACTIONS_TBL | Source Definition | SOURCE | ORA_BIISPRD_SRC |
| lkp_PS_JPM_JP_ITEMS | lkp_PS_JPM_JP_ITEMS | Lookup Procedure | TRANSFORMATION |  |

### 6.2. Session

- **Name**: `s_m_EHRP2BIIS_UPDATE`
- **Mapping**: `m_EHRP2BIIS_UPDATE`

**Session Configuration**:

- Session Log File Name: `s_m_EHRP2BIIS_UPDATE.log`
- Session Log File directory: `$PMSessionLogDir\`
- Treat source rows as: `Insert`
- Commit Type: `Target`
- Commit Interval: `10000`
- Commit On End Of File: `YES`
- Rollback Transactions on Errors: `NO`
- Recovery Strategy: `Fail task and continue workflow`
- DTM buffer size: `Auto`
- Enable high precision: `NO`
- Pushdown Optimization: `None`

**Session Transformation Instances**:

| Pipeline | Instance Name | Stage | Transform Name | Transform Type |
|----------|---------------|-------|----------------|----------------|
| 1 | NWK_ACTION_SECONDARY_TBL | 1 |  |  |
| 1 | NWK_ACTION_PRIMARY_TBL | 2 |  |  |
| 1 | EHRP_RECS_TRACKING_TBL | 3 |  |  |
| 0 | PS_GVT_JOB | 0 |  |  |
| 1 | SQ_PS_GVT_JOB | 4 |  |  |
| 1 | lkp_PS_GVT_EMPLOYMENT | 4 |  |  |
| 1 | lkp_PS_GVT_PERS_NID | 4 |  |  |
| 1 | exp_MAIN2BIIS | 4 |  |  |
| 1 | lkp_PS_GVT_AWD_DATA | 4 |  |  |
| 1 | lkp_PS_GVT_EE_DATA_TRK | 4 |  |  |
| 1 | lkp_PS_HE_FILL_POS | 4 |  |  |
| 1 | lkp_PS_GVT_CITIZENSHIP | 4 |  |  |
| 1 | lkp_PS_GVT_PERS_DATA | 4 |  |  |
| 1 | exp_PERS_DATA | 4 |  |  |
| 0 | NWK_NEW_EHRP_ACTIONS_TBL | 0 |  |  |
| 1 | exp_GET_EFFDT_YEAR | 4 |  |  |
| 1 | lkp_OLD_SEQUENCE_NUMBER | 4 |  |  |
| 1 | lkp_PS_JPM_JP_ITEMS | 4 |  |  |

**Connection Assignments**:

- lkp_PS_JPM_JP_ITEMS: Connection=`INFO_NATE`, Type=``

### 6.3. Workflow

- **Name**: `wf_EHRP2BIIS_UPDATE`

**Workflow Configuration**:

- Workflow Log File Name: `wf_EHRP2BIIS_UPDATE.log`
- Workflow Log File Directory: `$PMWorkflowLogDir\`
- Save Workflow log by: `By runs`
- Service Level Name: `Default`
- Enable HA recovery: `NO`
- Automatically recover terminated tasks: `NO`
- Allow concurrent run with unique run instance name: `NO`

**Task Instances**:

| Task Name | Task Type | Enabled |
|-----------|-----------|---------|
| Start | Start | YES |
| s_m_EHRP2BIIS_UPDATE | Session | YES |

**Workflow Links**:

| From Task | To Task | Condition |
|-----------|---------|-----------|
| Start | s_m_EHRP2BIIS_UPDATE | (unconditional) |

**Workflow Execution Flow**:
```
Start --> s_m_EHRP2BIIS_UPDATE (Session)
```

**Workflow Variables**:

| Variable | Datatype | Description |
|----------|----------|-------------|
| $Start.StartTime | date/time | The time this task started |
| $Start.EndTime | date/time | The time this task completed |
| $Start.Status | integer | Status of this task's execution |
| $Start.PrevTaskStatus | integer | Status of the previous task that is not disabled |
| $Start.ErrorCode | integer | Error code for this task's execution |
| $Start.ErrorMsg | string | Error message for this task's execution |
| $s_m_EHRP2BIIS_UPDATE.StartTime | date/time | The time this task started |
| $s_m_EHRP2BIIS_UPDATE.EndTime | date/time | The time this task completed |
| $s_m_EHRP2BIIS_UPDATE.Status | integer | Status of this task's execution |
| $s_m_EHRP2BIIS_UPDATE.PrevTaskStatus | integer | Status of the previous task that is not disabled |
| $s_m_EHRP2BIIS_UPDATE.ErrorCode | integer | Error code for this task's execution |
| $s_m_EHRP2BIIS_UPDATE.ErrorMsg | string | Error message for this task's execution |
| $s_m_EHRP2BIIS_UPDATE.SrcSuccessRows | integer | Rows successfully read |
| $s_m_EHRP2BIIS_UPDATE.SrcFailedRows | integer | Rows failed to read |
| $s_m_EHRP2BIIS_UPDATE.TgtSuccessRows | integer | Rows successfully loaded |
| $s_m_EHRP2BIIS_UPDATE.TgtFailedRows | integer | Rows failed to load |
| $s_m_EHRP2BIIS_UPDATE.TotalTransErrors | integer | Total number of transformation errors |
| $s_m_EHRP2BIIS_UPDATE.FirstErrorCode | integer | First error code |
| $s_m_EHRP2BIIS_UPDATE.FirstErrorMsg | string | First error message |

---

## 7. Pre/Post-Load Scripts

### 7.1. ehrp2biis_preload (Pre-Load Validation)

- **Type**: Korn Shell (ksh) script
- **Purpose**: Pre-load validation - runs Oracle SQL script before the Informatica session executes

**Functionality**:

1. **Environment Setup**: Sources Informatica PowerCenter 9.6.1 environment variables
2. **Oracle Connection**: Connects to Oracle using `sqlplus` with stored credentials (`$loin2/$ps2`)
3. **SQL Execution**: Runs the SQL script at `$homedir/step01`
4. **Error Checking**: Searches the log file for the string `ERROR`
5. **Notification**:
   - On **success**: Sends email with subject `ehrp2biis_preload SUCCESS...`
   - On **failure**: Sends email with subject `ehrp2biis_preload FAILED...`
6. **Logging**: Outputs to `$logdir/ehrp2biis_preload.log`

**Key Variables**:

| Variable | Purpose |
|----------|---------|
| `$homedir` | Directory containing SQL scripts |
| `$logdir` | Log output directory |
| `$loin2/$ps2` | Oracle database credentials |
| `$maillist` | Email notification recipients |

### 7.2. ehrp2biis_afterload.sql (Post-Load Reconciliation)

- **Type**: Oracle SQL*Plus script (289 lines)
- **Purpose**: Post-load data reconciliation, cleanup, and movement to permanent tables

**Operations (in execution order)**:

1. **Data Cleanup** - Fix retained step codes:
   ```sql
   UPDATE nknight.nwk_action_secondary_tbl a
   SET a.retnd1_step_cd = NULL
   WHERE a.event_id IN (SELECT b.event_id FROM nknight.nwk_action_primary_tbl b
     WHERE b.load_date = trunc(SYSDATE) AND b.event_id < 9000000000)
   AND a.retnd1_step_cd = '0.0000000000000';
   ```

2. **Sequence Number Update** - Updates sequence tracking:
   ```sql
   EXECUTE nknight.update_sequence_number_tbl_p;
   ```

3. **Record Formatting Procedures** (executed in order):
   - `UPDT_ERP2BIIS_CRE8_REMARKS01_P` - Creates remarks records
   - `UPDATE_ERP2BIIS_NO900S01_p` - Processes non-900 series actions
   - `ERP2BIIS_CRE8_REMARKS_900s01` - Creates remarks for 900-series actions
   - `UPDATE_ERP2BIIS_900SONLY01_P` - Processes 900-series only actions

4. **Cancelled Action Handling**:
   ```sql
   EXECUTE nknight.UPDT_ORIG_CANCELLED_TRANS01_P;
   ```

5. **Data Movement to Permanent Tables**:
   - `nwk_action_primary_tbl` -> `action_primary_all`
   - `nwk_action_secondary_tbl` -> `action_secondary_all`
   - `nwk_action_remarks_tbl` -> `action_remarks_all`

   Each insert filters for today's records: `WHERE load_date = trunc(SYSDATE)`

6. **WIP Status Check**:
   ```sql
   EXECUTE nknight.chk_ehrp2biis_wip_status_p;
   ```

7. **Staging Table Cleanup**:
   ```sql
   TRUNCATE TABLE nknight.nwk_new_ehrp_actions_tbl;
   ```

8. **Record Count Reporting**: Spools counts of records in primary, secondary, remarks, and tracking tables for today's load date

### 7.3. actstage_load (Action Staging Load)

- **Type**: Korn Shell (ksh) script
- **Purpose**: Loads action staging data by executing Oracle SQL

**Functionality**:

1. **Environment Setup**: Sources Informatica PowerCenter 9.6.1 environment
2. **Oracle Connection**: Connects via `sqlplus` with credentials `$loin2/$ps2`
3. **SQL Execution**: Runs `$homedir/action_stage_load` SQL script
4. **Success Checking**: Searches log for `SUCCESS!!!` string (note: checks for success, not error)
5. **Notification**:
   - On **success**: Sends email with subject `actstage_load SUCCESS...`
   - On **failure**: Sends email with subject `actstage_load FAILED...`
6. **Logging**: Outputs to `$logdir/actstage_load.log`

---

## 8. Migration Considerations

### 8.1. Oracle-Specific SQL to Convert

| Feature | Informatica/Oracle | Databricks Equivalent |
|---------|-------------------|----------------------|
| `TRUNC(SYSDATE)` | Oracle date truncation | `CURRENT_DATE()` |
| `EXECUTE procedure_name` | PL/SQL procedure calls | Databricks stored procedures or notebooks |
| `TRUNCATE TABLE` | DDL operation | `TRUNCATE TABLE` (Delta Lake) or `DELETE FROM` |
| `SPOOL` | SQL*Plus output | Databricks logging / notebook output |
| `INSERT INTO ... SELECT` | Bulk data movement | `INSERT INTO ... SELECT` or `MERGE` |
| `col ... format` | SQL*Plus formatting | Not needed in Databricks |
| `PROMPT` | SQL*Plus messaging | `print()` in notebooks |
| `&&variable` | SQL*Plus substitution | Widget parameters |

### 8.2. Informatica Functions to Convert

| Informatica Function | Used In | Databricks/Spark SQL Equivalent |
|---------------------|---------|-------------------------------|
| `GET_DATE_PART(date, 'YYYY')` | exp_GET_EFFDT_YEAR, exp_MAIN2BIIS | `YEAR(date)`, `MONTH(date)`, `DAY(date)` |
| `IIF(condition, true_val, false_val)` | exp_MAIN2BIIS (extensively) | `CASE WHEN condition THEN true_val ELSE false_val END` or `IF()` |
| `IS_SPACES(field)` | exp_MAIN2BIIS | `TRIM(field) = ''` or `field IS NULL` |
| `ISNULL(field)` | exp_MAIN2BIIS | `field IS NULL` |
| `IN(field, val1, val2, ...)` | exp_MAIN2BIIS | `field IN (val1, val2, ...)` |
| `DECODE(...)` | exp_MAIN2BIIS | `CASE WHEN ... END` |
| `TO_CHAR(number)` | exp_MAIN2BIIS | `CAST(number AS STRING)` |
| `UPPER(string)` | exp_MAIN2BIIS | `UPPER(string)` |
| `RTRIM(string)` | exp_MAIN2BIIS | `RTRIM(string)` |
| `TRUNC(SYSDATE)` | exp_MAIN2BIIS | `CURRENT_DATE()` |
| `ERROR('message')` | Default values | `RAISE` or error handling |
| `\|\|` (concatenation) | exp_MAIN2BIIS | `CONCAT()` or `\|\|` |

### 8.3. Key Business Logic to Preserve

1. **Event ID Generation**: Sequential event IDs based on year, using `SEQUENCE_NUM_TBL` lookup. In Databricks, consider using Delta Lake `IDENTITY` columns or `ROW_NUMBER()` with state tracking.

2. **NOA Code-Based Routing**: Multiple `IIF`/`DECODE` expressions route data based on Nature of Action (NOA) codes (e.g., 815=Recruitment Bonus, 816=Relocation Bonus, 825=Buyout, 840-879=Cash Awards, 805/806=FEGLI). Must be preserved exactly.

3. **Load ID/Date Generation**: `o_LOAD_ID` format is `NK` + YYYYMMDD; `o_LOAD_DATE` is `TRUNC(SYSDATE)`. These identify daily load batches.

4. **TSP Vesting Code Logic**: Complex business rule based on retirement plan (`GVT_RETIRE_PLAN` in K/M) and appointment type (`GVT_TYPE_OF_APPT` in 55/34/36/46/44) codes.

5. **Position Temp/Perm Logic**: Determines permanent/temporary position code based on `REG_TEMP` and `GVT_WORK_SCHED` values (R+F=1, R/T+P/I=3, T+F=2).

6. **Zero-to-NULL Conversions**: Many leave balance and financial fields convert zero values to NULL (e.g., `IIF(HE_AL_BALANCE=0, NULL, HE_AL_BALANCE)`).

7. **Agency Code Construction**: Concatenates `COMPANY || GVT_SUB_AGENCY` for agency assignment code.

8. **Citizenship Mapping**: Maps PeopleSoft citizenship codes (1,2 -> '1'; others -> '8') for BIIS format.

9. **Base Hours Calculation**: Work schedule-dependent: `IIF(GVT_WORK_SCHED='I', STD_HOURS, STD_HOURS*2)`.

10. **Post-Load Stored Procedures**: 7+ Oracle stored procedures run after the Informatica load. These must be converted to Databricks-compatible logic (notebooks, stored procedures, or Delta Live Tables):
    - `update_sequence_number_tbl_p`
    - `UPDT_ERP2BIIS_CRE8_REMARKS01_P`
    - `UPDATE_ERP2BIIS_NO900S01_p`
    - `ERP2BIIS_CRE8_REMARKS_900s01`
    - `UPDATE_ERP2BIIS_900SONLY01_P`
    - `UPDT_ORIG_CANCELLED_TRANS01_P`
    - `chk_ehrp2biis_wip_status_p`

### 8.4. Data Flow Summary for Migration

```
Source (Oracle)                    Target (Oracle -> Databricks)
--------------------               -----------------------------------
PS_GVT_JOB ----+
               +--> SQ_PS_GVT_JOB --> exp_MAIN2BIIS --+--> NWK_ACTION_PRIMARY_TBL
NWK_NEW_EHRP_  |                                      +--> NWK_ACTION_SECONDARY_TBL
ACTIONS_TBL ---+                                      +--> EHRP_RECS_TRACKING_TBL

Lookup Tables (9):
  SEQUENCE_NUM_TBL, PS_GVT_EMPLOYMENT, PS_GVT_PERS_NID,
  PS_GVT_AWD_DATA, PS_GVT_EE_DATA_TRK, PS_HE_FILL_POS,
  PS_GVT_CITIZENSHIP, PS_GVT_PERS_DATA, PS_JPM_JP_ITEMS

Post-Load: nwk_* staging --> *_all permanent tables
```

