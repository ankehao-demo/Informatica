-- ehrp2biis_afterload.sql: empty the driver table before the next load. Idempotent.
truncate table nknight.nwk_new_ehrp_actions_tbl
/
