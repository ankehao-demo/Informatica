# EHRP2BIIS pilot -- assumptions and ambiguous Informatica semantics

Every place where PowerCenter behaviour is not fully defined by the XML export, and
what the port does. Items marked **verify in dual-run** should be confirmed with the
reconciliation harness against a real legacy load before cut-over.

## Expression semantics

| # | Topic | Informatica behaviour assumed | Port | Status |
|---|---|---|---|---|
| A1 | `IIF` with NULL condition | Treated as FALSE -> else branch | `F.when(...).otherwise(...)` (same in Spark) | Documented PowerCenter behaviour |
| A2 | `DECODE(TRUE, ...)` truthiness | Branch taken only when the condition is TRUE; NULL never matches. `IN(NULL, ...)` is NULL, so `NOT IN(ERNCD,'RCR','RLC')` with NULL ERNCD is NULL -> an award NOA with no ERNCD gets NULL `CASH_AWARD_AMT` | `infa.decode_true` (CASE WHEN) | **verify in dual-run** (award NOAs with no `PS_GVT_AWD_DATA` row / NULL ERNCD) |
| A3 | `IS_SPACES` | TRUE for strings made only of blank/tab/CR/LF/FF/VT; NULL -> NULL. `''` treated as spaces (Oracle stores `''` as NULL, so it cannot reach PowerCenter as non-NULL anyway). PeopleSoft "blank" CHAR fields are a single `' '` -> TRUE -> NULL | `infa.is_spaces` (regex) | Documented |
| A4 | `||` with NULL | NULL operand is treated as `''`; result is NULL only when all operands are NULL. So `CITY || ', ' || STATE` is never NULL (`', '` on a lookup miss) | `infa.concat` (Spark `concat` would return NULL) | Documented PowerCenter behaviour |
| A5 | Untouched strings | No implicit trimming: PeopleSoft `' '` values pass through byte-for-byte; only `RTRIM(GVT_PAR_AUTH_D?_2)` is trimmed | no trim anywhere except where the XML says so | Documented |
| A6 | String longer than target column | Truncated to the target precision (e.g. `COMPANY || GVT_SUB_AGENCY` into `AGCY_ASSIGN_CD VARCHAR2(4)`) | `substring(1, precision)` | **verify in dual-run** -- PowerCenter may instead reject the row depending on the session's data-truncation settings |
| A7 | Implicit string -> number (string lookup port into a NUMBER target, e.g. `GVT_SPEP` -> `SPECIAL_PROGRAM_CD NUMBER(2)`) | TO_DECIMAL rules: leading blanks ignored, leading numeric part converted, non-numeric (`' '`) -> 0 | `infa.string_to_number` | **verify in dual-run** |
| A8 | Number too large for target `NUMBER(p,s)` | Oracle rejects the row (ORA-01438) -> `.bad` file | Row goes to `EHRP2BIIS_REJECTS` with reason, not loaded | Equivalent behaviour, different sink |
| A9 | `SYSDATE` / `trunc(sysdate)` | Evaluated at run time by the Integration Service | Pinned to the run's logical date (`--run-date`, Airflow `{{ ds }}`), so `LOAD_ID = 'NK'||YYYYMMDD` and `LOAD_DATE` are stable on reruns and around midnight | Intentional change for idempotency |
| A10 | `ADD_TO_DATE(d,'MM',-12)` | Day clamped to the last day of the target month (Feb 29 -> Feb 28), time kept | `d + make_interval(0,-12)` | Documented PowerCenter behaviour |
| A12 | Date port into a VARCHAR2 target (`o_LOAD_DATE` -> `AS_OF_DAY VARCHAR2(10)`) | Implicit conversion with the default *DateTime Format String* `MM/DD/YYYY HH24:MI:SS` (no override in the session), then truncated to 10 -> `'03/12/2026'` | `date_format` + truncate | **verify in dual-run** |
| A13 | Decimal with more scale than the target (`HE_AL_BALANCE` 12.50 -> `ANN_LV_CUR_BAL_HRS NUMBER(6,0)`) | Oracle rounds half away from zero on insert -> 13 | Spark decimal cast (HALF_UP) | Equivalent for positive values; verify negatives in dual-run |
| A11 | Decimal ports | Session does not enable high precision; values in this mapping are small (<= 15 digits) so double vs decimal makes no difference | Spark `DecimalType` throughout | Low risk |

## Stateful EVENT_ID (`v_EVENT_ID`)

`v_EVENT_ID = IIF(v_CURRENT_YEAR = v_PREVIOUS_YEAR, v_EVENT_ID + 1, EHRP_SEQ_NUMBER + 1)` depends on row order.
The SQ override orders by `PS_GVT_JOB.EFFDT`, so each calendar year is one contiguous run and
`EVENT_ID = EHRP_SEQ_NUMBER(year) + row_number() within the year`. PowerCenter's order **within the same EFFDT**
is whatever Oracle returns; the port breaks ties by `EMPLID, EMPL_RCD, EFFSEQ`. Consequence: rows sharing an
EFFDT may get the same set of EVENT_IDs in a different assignment than legacy. The reconciliation harness
therefore keys on the business key (not EVENT_ID) and reports EVENT_ID as a compared value.
**verify in dual-run**; if legacy order must be matched exactly, add `ORDER BY EFFDT, EMPLID, EMPL_RCD, EFFSEQ`
to the legacy SQ override during the dual-run period.

`SEQUENCE_NUM_TBL` is frozen per LOAD_ID (`EHRP2BIIS_SEQ_SNAPSHOT`) because the afterload's
`update_sequence_number_tbl_p` advances it; reruns of the same LOAD_ID reuse the snapshot and produce identical IDs.
A year with no `SEQUENCE_NUM_TBL` row yields NULL EVENT_ID -> NOT NULL violation -> reject (same as legacy).

## Lookups

| # | Topic | Assumption |
|---|---|---|
| L1 | *Use Any Value* on multiple match | PowerCenter returns an arbitrary cached row. The port picks the first row ordered by the output ports (deterministic) and records every multi-match key in `EHRP2BIIS_LOOKUP_MULTIMATCH`. Realistic for `PS_JPM_JP_ITEMS` (several active degrees per person) -- **verify in dual-run** |
| L2 | *Case Sensitive String Comparison = NO* | String keys compared with `upper()` |
| L3 | SQL overrides | Executed verbatim through Spark SQL on the bronze snapshot (schema owners `EHRP.`/`NKNIGHT.` stripped) |
| L4 | NULL keys | Never match (equi-join), same as the PowerCenter cache |
| L5 | `lkp_PS_JPM_JP_ITEMS` uses connection `BIISPRD` | Assumed readable through the same Oracle account as the other EHRP tables |

## Load / update strategy

* No Router, Filter or Update Strategy transformation and *Treat source rows as = Insert*: the legacy load is
  insert-only and a same-day rerun would duplicate rows. The port MERGEs on `(LOAD_ID, EVENT_ID)` /
  `EVENT_ID` / `(LOAD_DATE, BIIS_EVENT_ID)` and deletes rows of the same slice that are no longer produced.
  The writer flags (Insert/Update as Update/Delete = YES) are documented in the spec but have no effect without
  an Update Strategy.
* Target load order is 1 for all three targets in the XML; the port writes secondary, primary, tracking.
* `step01.sql` (preload) and `action_stage_load.sql` (actstage) are **not in this repository**; the DAG runs them
  unchanged through SQL*Plus from `EHRP2BIIS_LEGACY_SQL_HOME`. If `step01.sql` truncates the NWK work tables, the
  Oracle `publish_slice.sql` delete is a no-op and the insert repopulates them.

## Afterload / black-box procedures (separate porting effort)

The `HISTDBA.*` procedures, `update_sequence_number_tbl_p`, `chk_ehrp2biis_wip_status_p` and
`GATHER_EHRP2BIIS_RUNCOUNTS_P` are invoked, not ported -- **porting them is a separate effort**. They are not
known to be idempotent, so each call is guarded by `NKNIGHT.EHRP2BIIS_RUN_LEDGER (LOAD_ID, STEP_NAME)` and never
auto-retried. Procedures may use `SYSDATE` internally, so a back-dated rerun (run_date != today) of the
afterload is only safe once those procedures accept a date parameter.

## Pseudo-SSN

`m_EHRP2BIIS_UPDATE` does **not** mask SSN: `NWK_ACTION_PRIMARY_TBL.SSN` is a pass-through of
`PS_GVT_PERS_NID.NATIONAL_ID` (verified from the connectors). Masking is the separate `wf_Pseudossn` workflow
(SSN <-> PSEUDOSSN crosswalk loaded from a fixed-width agency file). `pipelines/pseudossn.py` ports its record
layout and record-type rules with a byte-for-byte round-trip test; porting the rest of `wf_Pseudossn` is out of
scope for this pilot.

## Distribution

`Transfer Scripts/` and `Maintenance Scripts/` are unchanged (sha256-pinned in
`tests/test_spec_and_contracts.py`) and are executed verbatim over SSH on the BIIS host. The XML/scripts do not
say which files follow an EHRP2BIIS load, so the list is configuration (`airflow/config/ehrp2biis_distribution.yaml`)
and is empty by default.
