"""Port of `m_COMPTIME_Build_Message_Counters` (`XML/COMPTIME`).

Session `s_COMPTIME_Build_Message_Counters`, workflow `wf_COMPTIME`, third task of
three; its post-session variable assignment feeds the `email_COMPTIME_Complete`
task.

What the mapping does
---------------------
Count the detail records on the Comp Time file that was just loaded, write that
count to COUNTER_TBL, and build the subject/body of the completion email
(`COMPTIME_MESSAGE_FILE`, plus the `$$MAP_SUBJECT` / `$$MAP_MESSAGE` mapping
variables that the workflow copies into `$$WF_SUBJECT` / `$$WF_MESSAGE`).

PowerCenter chain -> functions in this module
---------------------------------------------
    SQ_U0287D01              -> read_source
    exp_Initial              -> exp_initial               (record-type flag)
    fil_Detail               -> fil_detail
    agg_ALL_RECORDS          -> agg_all_records           (COUNT over all input)
    exp_Detail_Count         -> exp_detail_count
    lkp_PAY_PERIOD           -> lkp_pay_period            (current pay period)
    exp_Counters             -> exp_counters
    exp_Final    -> COUNTER_TBL            -> exp_final
    exp_Build_Message        -> exp_build_message         (SETVARIABLE)
    exp_Final_Message -> COMPTIME_MESSAGE_FILE -> exp_final_message
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from pcmigration import expressions as E
from pcmigration import schemas
from pcmigration import transforms as T
from pcmigration.config import RunConfig, add_common_arguments, session_start_time_from_arg

MAPPING_NAME = "m_COMPTIME_Build_Message_Counters"
COUNTER_DESCRIPTION = "Number of detail records from the COMP TIME file."
# The literal in exp_Build_Message contains a hard tab between the label and the
# '='; it survives into the email body, so it is reproduced exactly.
MESSAGE_LABEL = "Number of Detail Records from Comp Time file\t= "
SUBJECT_LABEL = "Comp Time File loaded successfully for Pay Period:  "


@dataclass(frozen=True)
class MappingVariables:
    """`$$MAP_SUBJECT` / `$$MAP_MESSAGE`, set by SETVARIABLE.

    PowerCenter persists these in the repository at the end of a successful
    session and the workflow copies them into `$$WF_SUBJECT` / `$$WF_MESSAGE` for
    the email task.  On Databricks they are task values
    (`dbutils.jobs.taskValues.set`), which is why `run` returns them instead of
    leaving them in a global.
    """

    subject: str | None
    message: str | None


def read_source(spark: SparkSession, config: RunConfig) -> DataFrame:
    """SQ_U0287D01 over the delimited flat file `$Param_COMPTIME_filename`.

    The file has no header and is comma-delimited with double-quote qualifiers;
    only SSN and NAME are wired out of the Source Qualifier.
    """
    return spark.read.csv(config.source_file, schema=schemas.COMPTIME_FILE, header=False, quote='"').select(
        "SSN", "NAME"
    )


def exp_initial(rows: DataFrame) -> DataFrame:
    """exp_Initial:

        o_RECORD_TYPE_FLAG = DECODE(TRUE, IS_NUMBER(SSN), 'D', 'NO')

    The file has no record-type column; a row is a *detail* row if its SSN parses
    as a number, which is how the header and trailer rows get excluded.  Two
    PowerCenter behaviours combine here: IS_NUMBER(NULL) is NULL (not FALSE), and
    DECODE(TRUE, NULL, ...) does not match, so a NULL SSN falls through to 'NO'
    and is filtered out.
    """
    return rows.withColumn(
        "RECORD_TYPE_FLAG",
        E.decode(F.lit(E.TRUE), E.is_number(F.col("SSN")), F.lit("D"), default=F.lit("NO")),
    )


def fil_detail(rows: DataFrame) -> DataFrame:
    """fil_Detail. The filter condition in the repository is:

        RECORD_TYPE_FLAG = 'D'

        --RECORD_TYPE_FLAG = 'H' OR

    The second line is commented out with PowerCenter's `--`, so header rows are
    excluded; only the live clause is ported.
    """
    return rows.filter(F.col("RECORD_TYPE_FLAG") == "D")


def agg_all_records(rows: DataFrame) -> DataFrame:
    """agg_ALL_RECORDS: `o_DETAIL_RECORD_COUNT = COUNT(SSN)`, no group-by ports,
    Transformation Scope = All Input.

    COUNT over a *port* ignores NULLs.  With no group-by the transformation emits
    exactly one row - including for an empty input, where the count is 0 and the
    downstream email still goes out saying zero records.  (Interpretation: the
    repository does not record what PowerCenter emits for an empty aggregator
    input; a single zero-count row is both the SQL-scalar-aggregate behaviour and
    the reading that keeps the notification email working.)

    The transformation also carries SSN / RECORD_TYPE_FLAG / RECORD_TYPE as
    pass-through ports, which would take the value of the *last* row read - none
    of them are wired downstream, so they are not ported.
    """
    return T.aggregate_all_input(rows, [F.count(F.col("SSN")).alias("DETAIL_RECORD_COUNT")])


def exp_detail_count(rows: DataFrame) -> DataFrame:
    """exp_Detail_Count: `o_CURR_PP_FLAG = 'Y'`, the lookup key for the current
    pay period."""
    return rows.withColumn("CURR_PP_FLAG", F.lit("Y"))


def lkp_pay_period(rows: DataFrame, pay_period: DataFrame) -> DataFrame:
    """lkp_PAY_PERIOD, condition `CURR_PP_FLAG = in_CURR_PP_FLAG`.

    The lookup key is the literal 'Y', so the mapping is asking "which row is
    flagged current?".  Nothing constrains PAY_PERIOD to a single flagged row -
    the flag is set by `wf_Pay_Calendar` - and on multiple matches PowerCenter
    returns an arbitrary one.  A miss (no flagged row) is not an error: PP_NUM and
    PP_END_YEAR come back NULL and flow into the message and the counter row.
    """
    return T.connected_lookup(
        rows,
        pay_period,
        lambda left, lkp: lkp["CURR_PP_FLAG"] == left["CURR_PP_FLAG"],
        {"lkp_PP_NUM": "PP_NUM", "lkp_PP_END_YEAR": "PP_END_YEAR"},
    )


def exp_counters(rows: DataFrame) -> DataFrame:
    """exp_Counters: attach the counter's description literal."""
    return rows.withColumn(
        "COUNTER_DESCRIPTION",
        E.enforce_string_precision(F.lit(COUNTER_DESCRIPTION), schemas.COUNTER_DESCRIPTION_PRECISION),
    )


def exp_final(rows: DataFrame, config: RunConfig) -> DataFrame:
    """exp_Final -> COUNTER_TBL.

        o_RUN_DATE     = SESSSTARTTIME
        o_PROCESS_NAME = $PMMappingName

    COUNTER_TBL also has PP_END_YEAR, PP_NUM and CYCLE_ID columns, and this
    mapping leaves all three unconnected even though the pay period is available
    two transformations upstream - so they land as NULL.  That is reproduced
    rather than "fixed": other mappings in the estate read this table and a
    suddenly-populated column changes their behaviour.
    """
    return rows.select(
        F.lit(config.session_start_time).cast("timestamp").alias("RUN_DATE"),
        F.lit(config.mapping_name).alias("PROCESS_NAME"),
        F.col("COUNTER_DESCRIPTION"),
        F.col("DETAIL_RECORD_COUNT").cast("decimal(15,0)").alias("COUNTER_VALUE"),
        F.lit(None).cast("decimal(4,0)").alias("PP_END_YEAR"),
        F.lit(None).cast("decimal(2,0)").alias("PP_NUM"),
        F.lit(None).cast("decimal(1,0)").alias("CYCLE_ID"),
    )


def exp_build_message(rows: DataFrame, config: RunConfig) -> DataFrame:
    """exp_Build_Message: the email subject and body.

        v_PP_NUM      = IIF(PP_NUM < 10, LPAD(TO_CHAR(PP_NUM), 2, '0'), TO_CHAR(PP_NUM))
        v_ENVIRONMENT = DECODE(SUBSTR($PMRepositoryServiceName, 1, 4),
                               'Dev_', 'Dev: ', 'Test', 'Test: ', 'Prod', 'Prod: ')
        v_SUBJECT     = v_ENVIRONMENT || 'Comp Time File loaded successfully for Pay Period:  '
                        || TO_CHAR(PP_END_YEAR) || '-' || v_PP_NUM
        v_MESSAGE     = 'Number of Detail Records from Comp Time file<TAB>= ' || TO_CHAR(COUNTER_1)
        o_SUBJECT     = SETVARIABLE($$MAP_SUBJECT, v_SUBJECT)
        o_MESSAGE     = SETVARIABLE($$MAP_MESSAGE, v_MESSAGE)

    Three things a literal translation gets wrong:

    * `||` treats NULL as an empty string.  When the pay-period lookup misses,
      PowerCenter still sends "Comp Time File loaded successfully for Pay Period:
      -"; Spark's `concat` would make the whole subject NULL and the email would
      go out empty.
    * DECODE has no default branch, so an unrecognised repository-service name
      yields a NULL environment prefix - which `||` then swallows.  That is why a
      Prod-looking subject line can silently lose its "Prod: " prefix.
    * `v_SUBJECT` is a string(100) variable port: the value is truncated to 100
      characters on assignment, before it reaches the string(100) target field.
    """
    pp_num_text = E.iif(
        F.col("lkp_PP_NUM") < 10,
        E.lpad(E.to_char(F.col("lkp_PP_NUM")), 2, "0"),
        E.to_char(F.col("lkp_PP_NUM")),
    )
    environment = E.decode(
        E.substr(F.lit(config.repository_service_name), 1, 4),
        F.lit("Dev_"),
        F.lit("Dev: "),
        F.lit("Test"),
        F.lit("Test: "),
        F.lit("Prod"),
        F.lit("Prod: "),
    )
    subject = E.concat_ops(
        environment,
        F.lit(SUBJECT_LABEL),
        E.to_char(F.col("lkp_PP_END_YEAR")),
        F.lit("-"),
        pp_num_text,
    )
    message = E.concat_ops(F.lit(MESSAGE_LABEL), E.to_char(F.col("DETAIL_RECORD_COUNT")))
    return rows.withColumn("SUBJECT", E.enforce_string_precision(subject, schemas.SUBJECT_PRECISION)).withColumn(
        "MESSAGE", E.enforce_string_precision(message, schemas.MESSAGE_PORT_PRECISION)
    )


def exp_final_message(rows: DataFrame) -> DataFrame:
    """exp_Final_Message -> COMPTIME_MESSAGE_FILE.

    The MESSAGE port here is string(600) but the target field is string(300), so
    PowerCenter truncates on write.  The truncation is applied explicitly.
    """
    return rows.select(
        F.col("SUBJECT"),
        E.enforce_string_precision(F.col("MESSAGE"), schemas.MESSAGE_TARGET_PRECISION).alias("MESSAGE"),
    )


def transform(config: RunConfig, source: DataFrame, pay_period: DataFrame) -> tuple[DataFrame, DataFrame]:
    """The whole mapping as a pure function: (COUNTER_TBL rows, message rows)."""
    detail = fil_detail(exp_initial(source))
    counted = exp_counters(lkp_pay_period(exp_detail_count(agg_all_records(detail)), pay_period))
    return exp_final(counted, config), exp_final_message(exp_build_message(counted, config))


def run(spark: SparkSession, config: RunConfig) -> MappingVariables:
    pay_period = spark.table(config.table("PAY_PERIOD"))
    counters, message = transform(config, read_source(spark, config), pay_period)

    counters.write.mode("append").saveAsTable(config.table("COUNTER_TBL"))
    message.coalesce(1).write.mode("overwrite").option("header", False).csv(
        f"{config.target_file_dir}/comptime_message_file"
    )
    return collect_mapping_variables(message)


def collect_mapping_variables(message: DataFrame) -> MappingVariables:
    """SETVARIABLE, evaluated after the mapping has run.

    The mapping variables are declared with `AGGFUNCTION = MAX`, meaning the value
    persisted is the maximum across all rows that called SETVARIABLE. This mapping
    only ever produces one row, so MAX is a no-op - but it is applied rather than
    assumed, because reusing this helper on a multi-row mapping would otherwise
    silently pick an arbitrary row.
    """
    row: Row | None = message.agg(F.max("SUBJECT").alias("SUBJECT"), F.max("MESSAGE").alias("MESSAGE")).first()
    if row is None:
        return MappingVariables(subject=None, message=None)
    return MappingVariables(subject=row["SUBJECT"], message=row["MESSAGE"])


def main(argv: list[str] | None = None) -> None:
    parser = add_common_arguments(argparse.ArgumentParser(description=__doc__))
    parser.add_argument("--source-file", required=True, help="the Comp Time file ($Param_COMPTIME_filename)")
    parser.add_argument("--target-file-dir", required=True, help="output location for the message file")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName(MAPPING_NAME).getOrCreate()
    config = RunConfig(
        catalog=args.catalog,
        schema=args.schema,
        session_start_time=session_start_time_from_arg(args.session_start_time),
        mapping_name=MAPPING_NAME,
        repository_service_name=args.repository_service_name,
        source_file=args.source_file,
        target_file_dir=args.target_file_dir,
    )
    variables = run(spark, config)
    # Equivalent of the post-session variable assignment that hands
    # $$MAP_SUBJECT / $$MAP_MESSAGE to the workflow's email task.
    print(f"$$WF_SUBJECT={variables.subject}")
    print(f"$$WF_MESSAGE={variables.message}")


if __name__ == "__main__":
    main()
