-- ehrp2biis_afterload.sql: copy today's load into the BIIS *_ALL history tables.
-- Not idempotent on its own (plain INSERT) -> guarded by the run ledger; the ledger
-- row is written in the same transaction, so the copy happens exactly once per LOAD_ID.
insert into action_primary_all b
select * from nknight.nwk_action_primary_tbl c
where c.load_date = TO_DATE(:load_date, 'YYYY-MM-DD')
/
insert into action_secondary_all b
select * from nknight.nwk_action_secondary_tbl c
where c.event_id in (select a.event_id
                     from nknight.nwk_action_primary_tbl a
                     where a.load_date = TO_DATE(:load_date, 'YYYY-MM-DD'))
/
insert into action_remarks_all b
select * from nknight.nwk_action_remarks_tbl c
where c.event_id in (select a.event_id
                     from nknight.nwk_action_primary_tbl a
                     where a.load_date = TO_DATE(:load_date, 'YYYY-MM-DD'))
/
