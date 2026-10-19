-- ehrp2biis_afterload.sql: refresh cancelled actions in the *_ALL tables.
-- Delete-then-insert of the same EVENT_ID set in one transaction -> rerunnable.
delete from ACTION_secondary_aLL d
WHERE d.EVENT_ID IN (select B.biis_event_id from nknight.ehrp_recs_tracking_tbl B
                      where B.biis_wip_status_changed_dt = TO_DATE(:load_date, 'YYYY-MM-DD'))
/
delete from ACTION_remarks_aLL d
WHERE d.EVENT_ID IN (select B.biis_event_id from nknight.ehrp_recs_tracking_tbl B
                      where B.biis_wip_status_changed_dt = TO_DATE(:load_date, 'YYYY-MM-DD'))
/
delete from ACTION_primary_aLL d
WHERE d.EVENT_ID IN (select B.biis_event_id from nknight.ehrp_recs_tracking_tbl B
                      where B.biis_wip_status_changed_dt = TO_DATE(:load_date, 'YYYY-MM-DD'))
/
insert into ACTION_primary_aLL
SELECT * FROM NKNIGHT.NWK_ACTION_primary_TBL D
WHERE d.EVENT_ID IN (select B.biis_event_id from nknight.ehrp_recs_tracking_tbl B
                      where B.biis_wip_status_changed_dt = TO_DATE(:load_date, 'YYYY-MM-DD'))
/
insert into ACTION_secondary_aLL
SELECT * FROM NKNIGHT.NWK_ACTION_secondary_TBL D
WHERE d.EVENT_ID IN (select B.biis_event_id from nknight.ehrp_recs_tracking_tbl B
                      where B.biis_wip_status_changed_dt = TO_DATE(:load_date, 'YYYY-MM-DD'))
/
insert into ACTION_remarks_aLL
SELECT * FROM NKNIGHT.NWK_ACTION_remarks_TBL D
WHERE d.EVENT_ID IN (select B.biis_event_id from nknight.ehrp_recs_tracking_tbl B
                      where B.biis_wip_status_changed_dt = TO_DATE(:load_date, 'YYYY-MM-DD'))
/
