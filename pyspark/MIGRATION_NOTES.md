# PowerCenter -> Databricks migration notes

These notes accompany the two ported mappings in `src/pcmigration/jobs/`. They
record the patterns used, the PowerCenter constructs that have no clean 1:1
equivalent on Databricks, and the semantic traps found while reading the exports
in `XML/`. The traps are the reason a mapping cannot be translated line by line:
each one is a place where the obvious Spark code compiles, runs, and is wrong.

Scope: `m_Pay_Calendar_Set_Pay_Calendar` and `m_COMPTIME_Build_Message_Counters`,
chosen from the 108 mappings inventoried in
[`docs/INFORMATICA_INVENTORY.md`](../docs/INFORMATICA_INVENTORY.md). Nothing under
`XML/`, `Transfer Scripts/` or `Maintenance Scripts/` was modified.

## 1. Translation table

| PowerCenter construct | PySpark / Databricks pattern | Where |
| --- | --- | --- |
| Source Qualifier over a relational source | `spark.table(...)`, or `spark.sql(...)` when the SQ carries a SQL override | `pay_calendar_set_pay_calendar.sq_pay_period` |
| Source Qualifier over a delimited flat file | `spark.read.csv(schema=...)` with the schema transcribed from the SOURCE definition | `comptime_build_message_counters.read_source` |
| Expression transformation | one function per transformation returning a new DataFrame; output ports become columns | every `exp_*` function |
| Expression *variable* port (`v_`) | a local Python variable holding a Column, inlined into the output column | `exp_build_message` |
| Expression *output* port (`o_`) | `withColumn` / a `select` projection | all |
| Filter | `DataFrame.filter` | `fil_detail` |
| Router | `transforms.router` -> a dict of DataFrames, one per group plus DEFAULT | `rtr_parameter_non_parameter` |
| Aggregator, no group-by, scope All Input | `transforms.aggregate_all_input` -> `df.agg(...)` | `agg_all_records` |
| Connected Lookup (cached) | `transforms.connected_lookup`: left join **plus** a per-input-row dedupe | both jobs |
| Unconnected Lookup (`:LKP.` call) | not exercised by these two mappings; the same helper plus a `when` on the calling condition | - |
| Update Strategy `DD_UPDATE` | tag rows, then `MERGE ... WHEN MATCHED THEN UPDATE` with **no** NOT MATCHED clause | `build_updates` / `write_updates` |
| Two target instances of one table | one union + one MERGE (the Router makes the branches exclusive) | `build_updates` |
| Target field precision | explicit `substring` on write | `exp_final_message` |
| Mapping parameter `$$X` from a parameter file | `RunConfig.parameters`, resolved in Python before the DataFrame is built | `exp_initial` |
| Mapping variable + `SETVARIABLE` | value returned from `run()`, published as a Databricks task value | `collect_mapping_variables` |
| `SESSSTARTTIME` | `RunConfig.session_start_time`, captured once | `exp_set_date`, `exp_final` |
| `$PMMappingName`, `$PMRepositoryServiceName` | `RunConfig` fields | `exp_final`, `exp_build_message` |
| `IIF`, `DECODE`, `IS_NUMBER`, `IS_DATE`, `TO_CHAR`, `LPAD`, `TRUNC`, `\|\|` | `pcmigration.expressions` - see the traps below for why these are not the Spark built-ins | `expressions.py` |

## 2. What has no clean 1:1 equivalent

**Workflows, sessions and worklets.** A workflow is a task graph whose links carry
conditions (`$s_X.Status = SUCCEEDED`); a session binds a mapping to connections,
commit intervals, caches and pre/post-SQL. Databricks Jobs cover the graph
(`depends_on`, `run_if`), but the session's per-mapping runtime configuration has
nowhere to live except job parameters and cluster config. Recommendation: one
Databricks Job per workflow, one task per session, and a `RunConfig` built from
job parameters - which is why every ported job takes `RunConfig` rather than
reading `os.environ` itself.

**Task ordering that encodes business logic.** `wf_Pay_Calendar` runs
`s_Pay_Calendar_Reset_Pay_Calendar` (clear every `CURR_PP_FLAG`) immediately before
`s_Pay_Calendar_Set_Pay_Calendar` (set exactly one). The invariant "exactly one
current pay period" lives in the *workflow*, not in either mapping, and there is a
window between the two tasks in which the calendar has no current period at all -
`m_COMPTIME_Build_Message_Counters` reads that same flag. On Databricks this
should become one MERGE that clears and sets in a single atomic statement; until
then the tasks must stay strictly sequenced in one Job.

**Parameter files.** `$Param_Root_Directory`, `$Param_COMPTIME_filename`,
`$$PP_NUM` and friends come from a text file on the Integration Service host,
untyped and unvalidated - which is why the mappings run `IS_NUMBER` over them.
Replace with Databricks job parameters (typed at the job level) and keep the
`IS_NUMBER` guard, because the fall-through behaviour it produces is load-bearing
(see trap 2).

**`pmcmd` scheduling.** Every workflow in the estate is `ON DEMAND`: the schedule
lives in an external scheduler that shells out to `pmcmd startworkflow`. Nothing in
this repository records when anything runs. That schedule has to be recovered from
the scheduler before cutover, and becomes a Databricks Job schedule or a file
arrival trigger.

**Shell transfer/maintenance scripts.** `Transfer Scripts/` moves files with
`sftp`/`scp` and mails operators on failure; `Maintenance Scripts/` archives files
by pay period. Post-session commands do the same inline (the COMPTIME load session
ends with `mv ... /data/archive/COMPTIME/u0827d01_P$$WF_PP_YEAR_NUM.txt`). On
Databricks these become Auto Loader / Volumes ingestion plus Delta time travel;
credentials that are currently host-level SSH keys become secret scopes. Do not
port them as `%sh` cells.

**Email tasks.** `email_COMPTIME_Complete` reads `$$WF_SUBJECT` / `$$WF_MESSAGE`,
which a post-session variable assignment copies out of the mapping variables. The
ported job returns those strings; publish them with
`dbutils.jobs.taskValues.set` and let a notification task (or the Job's own
notifications) send them.

**Rejected rows (`.bad` files).** A DD_UPDATE row whose key is absent from the
target is written to a `.bad` file on the Integration Service host and the session
still succeeds. Delta has no equivalent, so `transforms.split_update_strategy`
returns the rejects explicitly; route them to a quarantine table rather than
letting them vanish.

**Persistent mapping variables.** `SETVARIABLE` writes into the PowerCenter
repository and the value survives to the *next* run. Task values do not. Anything
that relies on cross-run persistence needs a Delta control table.

**Lookup caches and commit intervals.** Cache sizes, persistent caches, commit
intervals and target load order are session tuning with no meaning on Databricks;
they are dropped rather than translated.

## 3. Semantic traps found

Each trap has a test in `tests/` that fails if the ported code is "simplified"
back to the obvious Spark equivalent.

1. **`||` treats NULL as an empty string; `concat` does not.** Informatica's
   concatenation operator ignores NULLs, so a missing lookup value leaves a gap in
   the message. `F.concat` returns NULL for the whole expression - the completion
   email would go out with an empty subject. Concatenation appears in every
   message-building mapping in the estate - 14 of them, ending 8 of the 11 workflows.
   `expressions.concat_ops`, `test_expressions.py::TestConcatOperator`.

2. **A non-numeric parameter silently switches branch.** `IIF(NOT
   IS_NUMBER($$PP_NUM), 0, TO_DECIMAL($$PP_NUM))` turns a typo in the parameter
   file into `0`, no pay period has `PP_NUM = 0`, so the lookup misses and the
   mapping quietly uses the system date instead of the requested pay period. No
   error, no warning - and the wrong pay period gets flagged current.
   `test_pay_calendar_set_pay_calendar.py::TestParameterBranch`.

3. **`IS_NUMBER(NULL)` is NULL, not FALSE.** Combined with `DECODE(TRUE,
   IS_NUMBER(SSN), 'D', 'NO')`, a NULL SSN matches nothing and falls to the
   default. Implementing `is_number` as a plain boolean would flip rows between
   the detail and non-detail branches, changing the record count that goes into
   COUNTER_TBL and the completion email.
   `test_expressions.py::TestIsNumber::test_null_input_returns_null_not_false`.

4. **`DECODE` without a default returns NULL, and `||` then hides it.** The
   environment prefix comes from `DECODE(SUBSTR($PMRepositoryServiceName, 1, 4),
   'Dev_', ..., 'Test', ..., 'Prod', ...)`. Rename or re-point the repository
   service and the prefix silently disappears from the subject line rather than
   failing. `test_comptime_...py::test_unknown_repository_service_loses_the_prefix`.

5. **A Lookup never changes the row count; a join does.** `lkp_PAY_PERIOD` matches
   on `CURR_PP_FLAG = 'Y'` and nothing constrains that to one row. With two flagged
   rows a left join doubles the pipeline; PowerCenter returns an arbitrary single
   match ("Use Any Value"). The port collapses the match set deterministically.
   `test_transforms.py::TestConnectedLookup::test_multiple_matches_do_not_multiply_rows`.

6. **`Update else Insert = NO` means unmatched rows are rejected, not inserted.**
   Writing the modern-looking `MERGE ... WHEN NOT MATCHED THEN INSERT` starts
   creating rows the legacy job throws away. When the pay calendar has a gap, this
   mapping flags a row with a **NULL key** for update, and the writer silently
   rejects it - the legacy behaviour is "no current pay period, session succeeds".
   `test_pay_calendar_set_pay_calendar.py::TestDateBranch`.

7. **String ports truncate silently on assignment.** `v_SUBJECT` is `string(100)`
   and `COMPTIME_MESSAGE_FILE.MESSAGE` is `string(300)` fed from a `string(600)`
   port. Spark keeps the whole value, so a port that looks correct produces
   longer output than the legacy job - a diff-based validation will flag every row.
   `expressions.enforce_string_precision`, `test_expressions.py::TestStringPortPrecision`.

8. **`TRUNC(SESSSTARTTIME)` is what makes the date-range lookup work.** The lookup
   condition is `PP_START_DTE <= in_CURRENT_DATE AND PP_END_DTE >= in_CURRENT_DATE`
   and `PP_END_DTE` is midnight. Drop the TRUNC (or substitute
   `current_timestamp()`) and every run after 00:00:00 on the last day of a pay
   period misses. `SESSSTARTTIME` is also constant for the whole session, unlike
   `current_timestamp()`.
   `test_pay_calendar_set_pay_calendar.py::TestDateBranch::test_boundary_dates_are_inclusive`.

9. **Unconnected target ports are load-bearing.** `COUNTER_TBL.PP_NUM`,
   `PP_END_YEAR` and `CYCLE_ID` are left unconnected in
   `m_COMPTIME_Build_Message_Counters` even though the values are available two
   transformations upstream. Populating them "because we can" changes what
   downstream reporting sees. The port reproduces the NULLs.

10. **ANSI mode turns bad data into a failed job.** `to_date` raises on
    Databricks/Spark 4 where PowerCenter's `IS_DATE` merely returns FALSE, so the
    port uses `try_to_timestamp`. A file with one malformed date would abort a
    naive port instead of filtering the row.
    `expressions.is_date`, `test_expressions.py::TestIsDate`.

11. **Commented-out expression text.** `fil_Detail`'s condition contains a
    `--RECORD_TYPE_FLAG = 'H' OR` line. The repository keeps commented code inside
    expressions; it must be read as a comment, not as logic.

12. **Vestigial SQL.** `SQ_PAY_PERIOD`'s override is
    `SELECT MAX(PP_NUM), MAX(PP_END_YEAR) FROM PAY_PERIOD` - two independent
    maxima, neither of which is used downstream. Its only function is to emit
    exactly one driver row. Translating it as "read PAY_PERIOD" would run the
    mapping once per calendar row.

## 4. Running the jobs

```bash
cd pyspark
pip install -r requirements.txt
pytest                       # 44 tests, local Spark, no cluster needed
```

On Databricks (one task per session, parameters instead of a parameter file):

```bash
python -m pcmigration.jobs.pay_calendar_set_pay_calendar \
    --catalog main --schema hhs --pp-num "" --pp-end-year "" \
    --repository-service-name Prod_Repo

python -m pcmigration.jobs.comptime_build_message_counters \
    --catalog main --schema hhs \
    --source-file /Volumes/main/hhs/landing/COMPTIME/u0287d01.txt \
    --target-file-dir /Volumes/main/hhs/outbound \
    --repository-service-name Prod_Repo
```

`--session-start-time` pins `SESSSTARTTIME` for a reproducible re-run, which is how
a ported job can be replayed against a historical date during validation.
