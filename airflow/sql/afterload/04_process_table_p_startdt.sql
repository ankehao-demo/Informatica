-- ehrp2biis_afterload.sql: PROCESS_TABLE P_STARTDT reset + recompute. Deterministic
-- for a given database state, so rerunnable.
UPDATE PROCESS_TABLE
SET P_STARTDT  = NULL
/
UPDATE PROCESS_TABLE
       SET P_STARTDT =
  (SELECT effdt FROM
(
select a.biis_event_id, a.emplid,
       a.empl_rcd, a.effdt, a.effseq, b.deptid,
       a.gvt_wip_status "OLD STATUS",
       b.gvt_wip_status "NEW STATUS"
from nknight.ehrp_recs_tracking_tbl a, ehrp.ps_gvt_job b
where a.emplid = b.emplid and
      a.empl_rcd = b.empl_rcd and
      a.effdt = b.effdt and
      a.effseq = b.effseq and
      a.gvt_wip_status <> b.gvt_wip_status and
      a.changed_wip_status is null
order by 4, 1
) WHERE ROWNUM < 2
)
/
UPDATE PROCESS_TABLE
      SET P_STARTDT  = TO_DATE(:load_date, 'YYYY-MM-DD') + 10000
   where P_STARTDT  is null
/
