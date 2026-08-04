"""Port of `m_Pay_Calendar_Set_Pay_Calendar` (`XML/Pay_Calendar`).

Session `s_Pay_Calendar_Set_Pay_Calendar`, workflow `wf_Pay_Calendar`, second task
of four.

What the mapping does
---------------------
Mark exactly one row of PAY_PERIOD as the current pay period (`CURR_PP_FLAG = 'Y'`).
Which row depends on the parameter file:

* if `$$PP_NUM` / `$$PP_END_YEAR` are set *and* identify an existing PAY_PERIOD row,
  that row is marked;
* otherwise the pay period whose date range contains the session start date is
  marked.

PowerCenter chain -> functions in this module
---------------------------------------------
    SQ_PAY_PERIOD (SQL override)          -> sq_pay_period
    exp_Initial                           -> exp_initial
    lkp_Existing_Pay_Period               -> lkp_existing_pay_period
    exp_Determine_Parameters_Exist        -> exp_determine_parameters_exist
    rtr_Parameter_Non_Parameter           -> rtr_parameter_non_parameter
      PARAMETERS_EXIST     -> exp_Set_Current_Pay_Period_Param   -> exp_set_current_pay_period_param
      PARAMETERS_NOT_EXIST -> exp_Set_Date                       -> exp_set_date
                              lkp_New_Current_Pay_Period         -> lkp_new_current_pay_period
                              exp_Set_Current_Pay_Period_NonParam-> exp_set_current_pay_period_non_param
    upd_Set_Current_PP_Param / _Non_Param -> build_updates (DD_UPDATE)
    PAY_PERIOD_PARAM + PAY_PERIOD targets -> write_updates (one MERGE)

Semantic notes are inline at the point where they bite.
"""

from __future__ import annotations

import argparse
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pcmigration import expressions as E
from pcmigration import transforms as T
from pcmigration.config import RunConfig, add_common_arguments, session_start_time_from_arg

MAPPING_NAME = "m_Pay_Calendar_Set_Pay_Calendar"
TARGET_KEYS = ("PP_NUM", "PP_END_YEAR")
UPDATE_COLUMNS = ("CURR_PP_FLAG",)


def sq_pay_period(spark: SparkSession, config: RunConfig) -> DataFrame:
    """SQ_PAY_PERIOD, whose SQL override is:

        SELECT MAX(PAY_PERIOD.PP_NUM) PP_NUM, MAX(PAY_PERIOD.PP_END_YEAR) PP_END_YEAR
        FROM PAY_PERIOD

    The two maxima are computed independently, so the row generally does not
    correspond to a real pay period - and neither column is used downstream (they
    are wired into the Router and dropped there).  The query is a *row generator*:
    its only job is to drive exactly one row through the mapping, which an
    aggregate always produces even when the table is empty.  It is kept verbatim
    rather than replaced with `spark.range(1)` so the port stays auditable.
    """
    return spark.sql(f"SELECT MAX(PP_NUM) AS PP_NUM, MAX(PP_END_YEAR) AS PP_END_YEAR FROM {config.table('PAY_PERIOD')}")


def exp_initial(rows: DataFrame, config: RunConfig) -> DataFrame:
    """exp_Initial: resolve the parameter-file values.

        v_PARAM_PP_NUM      = IIF(NOT IS_NUMBER($$PP_NUM),      0, TO_DECIMAL($$PP_NUM))
        v_PARAM_PP_END_YEAR = IIF(NOT IS_NUMBER($$PP_END_YEAR), 0, TO_DECIMAL($$PP_END_YEAR))

    `$$PP_NUM` is a mapping *parameter*: a string constant resolved from the
    parameter file at session start, not a column, so it is evaluated in Python
    and broadcast as a literal.  An unset or non-numeric parameter becomes 0 -
    which is the mechanism the mapping uses to fall through to the date branch,
    since no pay period has PP_NUM = 0.

    The local variable `v_TEN_DAYS_AGO` in this transformation is computed but
    never read by any output port; it is not ported.
    """
    return rows.withColumn(
        "PARAM_PP_NUM", F.lit(_resolve_numeric_parameter(config, "$$PP_NUM")).cast("decimal(10,0)")
    ).withColumn("PARAM_PP_END_YEAR", F.lit(_resolve_numeric_parameter(config, "$$PP_END_YEAR")).cast("decimal(10,0)"))


def _resolve_numeric_parameter(config: RunConfig, name: str) -> Decimal:
    raw = config.parameter(name)
    return Decimal(raw.strip()) if E.is_number_value(raw) else Decimal(0)


def lkp_existing_pay_period(rows: DataFrame, pay_period: DataFrame) -> DataFrame:
    """lkp_Existing_Pay_Period: does the parameter-file pay period exist?

        Lookup condition: PP_NUM = in_PARAM_PP_NUM AND PP_END_YEAR = in_PARAM_PP_END_YEAR

    Only the PP_NUM return port is wired downstream, and it is used purely as an
    existence test.
    """
    return T.connected_lookup(
        rows,
        pay_period,
        lambda left, lkp: (left["PARAM_PP_NUM"] == lkp["PP_NUM"]) & (left["PARAM_PP_END_YEAR"] == lkp["PP_END_YEAR"]),
        {"lkp_PP_NUM": "PP_NUM"},
    )


def exp_determine_parameters_exist(rows: DataFrame) -> DataFrame:
    """exp_Determine_Parameters_Exist:

        o_PARAM_EXISTS_FLAG = IIF(NOT ISNULL(lkp_PP_NUM), TRUE, FALSE)

    TRUE/FALSE are the integers 1/0 - the Router downstream compares against them
    numerically, so the flag must not become a Spark boolean.
    """
    return rows.withColumn(
        "PARAM_EXISTS_FLAG",
        E.iif(F.col("lkp_PP_NUM").isNotNull(), F.lit(E.TRUE), F.lit(E.FALSE)),
    )


def rtr_parameter_non_parameter(rows: DataFrame) -> dict[str, DataFrame]:
    """rtr_Parameter_Non_Parameter, groups `PARAM_EXISTS_FLAG = TRUE` / `= FALSE`.

    The DEFAULT group is wired to nothing in the mapping and is unreachable here
    (the flag is always 1 or 0), but it is returned so a caller can assert that.
    """
    return T.router(
        rows,
        {
            "PARAMETERS_EXIST": F.col("PARAM_EXISTS_FLAG") == E.TRUE,
            "PARAMETERS_NOT_EXIST": F.col("PARAM_EXISTS_FLAG") == E.FALSE,
        },
    )


def exp_set_current_pay_period_param(rows: DataFrame) -> DataFrame:
    """exp_Set_Current_Pay_Period_Param -> upd_Set_Current_PP_Param.

    The parameter branch writes the *parameter* values as the target key
    (PARAM_PP_NUM1 -> PP_NUM, PARAM_PP_END_YEAR1 -> PP_END_YEAR), not the values
    the lookup returned, and sets CURR_PP_FLAG = 'Y'.
    """
    return rows.select(
        F.col("PARAM_PP_NUM").cast("decimal(2,0)").alias("PP_NUM"),
        F.col("PARAM_PP_END_YEAR").cast("decimal(4,0)").alias("PP_END_YEAR"),
        F.lit("Y").alias("CURR_PP_FLAG"),
    )


def exp_set_date(rows: DataFrame, config: RunConfig) -> DataFrame:
    """exp_Set_Date: `o_CURRENT_DATE = TRUNC(SESSSTARTTIME)`.

    SESSSTARTTIME is fixed for the whole session, so it comes from the run config
    rather than `current_timestamp()`, which Spark may evaluate per partition.
    TRUNC with no format argument truncates to midnight - and it matters: the
    lookup below compares against PP_END_DTE, and an un-truncated timestamp on the
    last day of a pay period would fall outside the range.
    """
    return rows.withColumn("CURRENT_DATE_", E.trunc_date(F.lit(config.session_start_time).cast("timestamp")))


def lkp_new_current_pay_period(rows: DataFrame, pay_period: DataFrame) -> DataFrame:
    """lkp_New_Current_Pay_Period: the pay period containing the session date.

        Lookup condition: PP_START_DTE <= in_CURRENT_DATE AND PP_END_DTE >= in_CURRENT_DATE

    A non-equijoin lookup.  If the calendar has overlapping rows this returns one
    of them ('Use Any Value'); if the calendar has a gap it returns NULLs, and the
    mapping then flags a row with a NULL key for update - see build_updates.

    The return ports are prefixed `new_` because the row already carries
    `lkp_PP_NUM` from lkp_Existing_Pay_Period; in PowerCenter the two live in
    different transformations and cannot collide.
    """
    return T.connected_lookup(
        rows,
        pay_period,
        lambda left, lkp: (lkp["PP_START_DTE"] <= left["CURRENT_DATE_"]) & (lkp["PP_END_DTE"] >= left["CURRENT_DATE_"]),
        {"new_PP_NUM": "PP_NUM", "new_PP_END_YEAR": "PP_END_YEAR", "new_CURR_PP_FLAG": "CURR_PP_FLAG"},
    )


def exp_set_current_pay_period_non_param(rows: DataFrame) -> DataFrame:
    """exp_Set_Current_Pay_Period_Non_Param -> upd_Set_Current_PP_Non_Param.

    The looked-up CURR_PP_FLAG is read into the transformation but never used:
    the output is the literal 'Y' regardless of the row's current flag.
    """
    return rows.select(
        F.col("new_PP_NUM").cast("decimal(2,0)").alias("PP_NUM"),
        F.col("new_PP_END_YEAR").cast("decimal(4,0)").alias("PP_END_YEAR"),
        F.lit("Y").alias("CURR_PP_FLAG"),
    )


def build_updates(param_branch: DataFrame, non_param_branch: DataFrame) -> DataFrame:
    """upd_Set_Current_PP_Param + upd_Set_Current_PP_Non_Param, both `DD_UPDATE`.

    The mapping has two Update Strategy transformations writing to two *instances*
    of the same PAY_PERIOD table.  The Router makes the branches mutually
    exclusive, so they are unioned into one statement here; keeping two writers
    would mean two MERGEs against one Delta table in a single job, which is
    needless contention and is not atomic.
    """
    return T.flag_rows(param_branch.unionByName(non_param_branch), T.DD_UPDATE)


def transform(spark: SparkSession, config: RunConfig, source: DataFrame, pay_period: DataFrame) -> DataFrame:
    """The whole mapping as a pure function, for testing and for `run`."""
    initial = exp_initial(source, config)
    with_lookup = lkp_existing_pay_period(initial, pay_period)
    flagged = exp_determine_parameters_exist(with_lookup)
    groups = rtr_parameter_non_parameter(flagged)

    param_branch = exp_set_current_pay_period_param(groups["PARAMETERS_EXIST"])
    non_param_branch = exp_set_current_pay_period_non_param(
        lkp_new_current_pay_period(exp_set_date(groups["PARAMETERS_NOT_EXIST"], config), pay_period)
    )
    return build_updates(param_branch, non_param_branch)


def write_updates(spark: SparkSession, config: RunConfig, updates: DataFrame) -> None:
    """Apply the DD_UPDATE rows with a MERGE that has no NOT MATCHED clause.

    The session is configured 'Insert = YES, Update as Update = YES, Update else
    Insert = NO'.  With rows flagged DD_UPDATE that means: update when the key
    exists, reject to the .bad file when it does not.  Adding
    `WHEN NOT MATCHED THEN INSERT` would look like a harmless modernisation and
    would start inserting rows the legacy job rejects.
    """
    updates.createOrReplaceTempView("pc_updates")
    assignments = ", ".join(f"t.{c} = s.{c}" for c in UPDATE_COLUMNS)
    on_clause = " AND ".join(f"t.{k} = s.{k}" for k in TARGET_KEYS)
    spark.sql(
        f"MERGE INTO {config.table('PAY_PERIOD')} t USING pc_updates s ON {on_clause} "
        f"WHEN MATCHED THEN UPDATE SET {assignments}"
    )


def run(spark: SparkSession, config: RunConfig) -> DataFrame:
    """Order matters: `wf_Pay_Calendar` runs `s_Pay_Calendar_Reset_Pay_Calendar`
    (which clears every CURR_PP_FLAG) immediately before this session.  This job
    only *sets* the flag, so running it without the reset leaves two current pay
    periods.  On Databricks the two must stay sequenced in the same Job, or be
    folded into a single MERGE - see MIGRATION_NOTES.md."""
    pay_period = spark.table(config.table("PAY_PERIOD"))
    updates = transform(spark, config, sq_pay_period(spark, config), pay_period)
    write_updates(spark, config, updates)
    return updates


def main(argv: list[str] | None = None) -> None:
    parser = add_common_arguments(argparse.ArgumentParser(description=__doc__))
    parser.add_argument("--pp-num", default="", help="parameter-file $$PP_NUM; empty means 'use the session date'")
    parser.add_argument("--pp-end-year", default="", help="parameter-file $$PP_END_YEAR")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName(MAPPING_NAME).getOrCreate()
    config = RunConfig(
        catalog=args.catalog,
        schema=args.schema,
        session_start_time=session_start_time_from_arg(args.session_start_time),
        mapping_name=MAPPING_NAME,
        repository_service_name=args.repository_service_name,
        parameters={"$$PP_NUM": args.pp_num, "$$PP_END_YEAR": args.pp_end_year},
    )
    run(spark, config)


if __name__ == "__main__":
    main()
