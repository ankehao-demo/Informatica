# EHRP2BIIS_UPDATE Pipeline - Validation Report

**Generated**: 2026-04-01 19:10 UTC
**Database**: informatica_migration
**Job ID**: 312735594897394
**Run ID**: 1107387383051304

## 1. Row Count Validation

| Table | Row Count |
|-------|-----------|
| nwk_new_ehrp_actions_tbl (source staging) | 2 |
| ps_gvt_job (source) | 2 |
| Source Qualifier inner join | 2 |
| nwk_action_primary_tbl (target) | 2 |
| nwk_action_secondary_tbl (target) | 2 |
| ehrp_recs_tracking_tbl (target) | 2 |

**Result**: PASS - All target tables match source qualifier join count (2 rows each)

## 2. Schema Validation

### nwk_action_primary_tbl
Key columns verified present: EVENT_ID, LOAD_ID, LOAD_DATE, EMP_ID, AGCY_ASSIGN_CD, NOA_CD, POSITION_NBR, GRADE_CD, PAY_PLAN_CD, CITIZENSHIP_STATUS, and 80+ additional fields.

### nwk_action_secondary_tbl
Key columns verified present: EVENT_ID, RETMT_SCD_DTE, TSP_SCD_DTE, TSP_VESTING_CD, RETND1_PAY_PLAN_CD, RETND1_GRADE_CD, RETND1_STEP_CD, and 30+ additional fields.

### ehrp_recs_tracking_tbl
Columns: BIIS_EVENT_ID, EMPLID, EMPL_RCD, EFFDT, EFFSEQ, NOA_CD, NOA_SUFFIX_CD, GVT_WIP_STATUS, EVENT_SUBMITTED_DT, LOAD_DATE (10 columns)

## 3. Business Logic Validation

### 3.1 EVENT_ID Uniqueness
- Distinct EVENT_IDs: 2
- Total rows: 2
- **Result**: PASS

### 3.2 EMPLID Not Null
- Null EMP_ID count: 0
- **Result**: PASS

### 3.3 LOAD_DATE Validation
- LOAD_DATE = 2026-04-01: 2 rows
- **Result**: PASS (all rows have today's date)

### 3.4 LOAD_ID Format (NKyyyymmdd)
- LOAD_ID = NK20260401: 2 rows
- Format valid: Yes (starts with "NK", 10 characters)
- **Result**: PASS

### 3.5 Agency Assignment (AGCY_ASSIGN_CD)
- Value: 'HHS00': 2 rows
- Concatenation of COMPANY + GVT_SUB_AGENCY confirmed correct
- **Result**: PASS

### 3.6 Citizenship Status Mapping
- Status '1': 2 rows (mapped from source value)
- All values are valid ('1' = US Citizen or '8' = Non-citizen)
- **Result**: PASS

### 3.7 EVENT_ID Sequencing
- Min EVENT_ID: 1000001
- Max EVENT_ID: 1000002
- Sequential numbering confirmed
- **Result**: PASS

### 3.8 EVENT_ID Cross-Table Consistency
- Primary IDs not in Secondary: 0
- Tracking IDs not in Primary: 0
- **Result**: PASS

### 3.9 TSP Vesting Code
- All values are valid (0, 2, or 3)
- **Result**: PASS

### 3.10 NOA Code Populated
- Null NOA_CD in primary: 0
- Null NOA_CD in tracking: 0
- **Result**: PASS

## 4. Sample Data

### Primary Table (NWK_ACTION_PRIMARY_TBL)
| EVENT_ID | EMP_ID | AGCY_ASSIGN_CD | NOA_CD | LOAD_ID |
|----------|--------|----------------|--------|---------|
| 1000001 | T00000001 | HHS00 | 894 | NK20260401 |
| 1000002 | T00000002 | HHS00 | 894 | NK20260401 |

### Secondary Table (NWK_ACTION_SECONDARY_TBL)
| EVENT_ID | RETMT_SCD_DTE | TSP_SCD_DTE | TSP_VESTING_CD |
|----------|---------------|-------------|----------------|
| 1000001 | 2020-03-15 | 2020-03-15 | 3 |
| 1000002 | 2022-07-01 | 2022-07-01 | 3 |

### Tracking Table (EHRP_RECS_TRACKING_TBL)
| BIIS_EVENT_ID | EMPLID | EMPL_RCD | EFFDT | EFFSEQ | NOA_CD | NOA_SUFFIX_CD | GVT_WIP_STATUS | EVENT_SUBMITTED_DT | LOAD_DATE |
|---------------|--------|----------|-------|--------|--------|---------------|----------------|--------------------|---------  |
| 1000001 | T00000001 | 0 | 2026-04-01 | 0 | 894 | 0 | APR | 2026-04-01 | 2026-04-01 |
| 1000002 | T00000002 | 0 | 2026-04-01 | 0 | 894 | 0 | APR | 2026-04-01 | 2026-04-01 |

## 5. Afterload Reconciliation Log

The afterload notebook ran 10 validation checks. All passed:

| Check | Result |
|-------|--------|
| event_id_uniqueness | PASS |
| event_id_consistency | PASS |
| load_date_check | PASS |
| no_null_emplid | PASS |
| noa_cd_populated | PASS |
| load_id_format | PASS |
| citizenship_values | PASS |
| tsp_vesting_values | PASS |
| event_id_positive | PASS |
| base_hours_distribution | INFO (Intermittent=0, Non-intermittent=2) |

Post-load fixes applied:
- Retained step code fixes: 0
- WIP status changes: 0
- Records promoted to ALL tables: 2

**Afterload Overall Status**: SUCCESS - All checks passed

## 6. Pipeline Execution Summary

| Task | Status | Duration |
|------|--------|----------|
| preload | SUCCESS | 25s |
| etl | SUCCESS | 151s |
| afterload | SUCCESS | 45s |
| **Total** | **SUCCESS** | **~221s** |

## 7. Issues Resolved During Migration

1. **Serverless compute compatibility** - Replaced `spark.sparkContext.broadcast()` with DataFrame-based `broadcast()` join (sparkContext not available on serverless)
2. **Schema variation handling** - Added `ensure_columns()` helper to create NULL columns for fields missing from source tables in this environment
3. **Delta schema evolution** - Added `overwriteSchema` option to handle existing table schema conflicts during writes
4. **Type casting in afterload** - Added defensive `.cast()` calls for cross-type comparisons (string vs numeric)

## 8. Overall Verdict

**Status: PASS**

All validation checks passed:
- Row counts match across all 3 target tables and source qualifier (2 rows each)
- EVENT_ID is unique, positive, sequential, and consistent across all tables
- LOAD_ID follows expected NKyyyymmdd format (NK20260401)
- No null EMPLIDs in any target table
- Citizenship status values correctly mapped to '1' (US Citizen)
- Agency assignment correctly computed as COMPANY + GVT_SUB_AGENCY
- TSP vesting codes are valid
- Pipeline executed successfully in ~221 seconds across 3 tasks
