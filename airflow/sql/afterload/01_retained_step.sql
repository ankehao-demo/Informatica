-- ehrp2biis_afterload.sql Step 04 (UPDATE_NWK_actsec_RETAINED_STEP). Idempotent.
UPDATE nknight.nwk_action_secondary_tbl a
SET a.retnd1_step_cd = NULL
WHERE a.event_id  IN (SELECT b.event_id
                       FROM nknight.nwk_action_primary_tbl b
                       WHERE b.load_date = TO_DATE(:load_date, 'YYYY-MM-DD') AND
                             b.event_id < 9000000000)
AND
       a.retnd1_step_cd = '0.0000000000000'
/
