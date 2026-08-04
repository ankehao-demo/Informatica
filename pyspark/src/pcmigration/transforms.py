"""Reusable PySpark equivalents of PowerCenter transformation *shapes*.

These are the pieces where the PowerCenter transformation has behaviour that a
direct Spark idiom would get wrong - principally the Lookup (which can never
change the row count) and the Router (whose groups are independent, not a chain
of else-ifs).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

# Update Strategy constants.  PowerCenter's DD_* macros are the integers below.
DD_INSERT = 0
DD_UPDATE = 1
DD_DELETE = 2
DD_REJECT = 3

_ROW_ID = "__pc_row_id"
_LKP_RANK = "__pc_lookup_rank"


class LookupSide:
    """Accessor for the lookup source inside a lookup condition.

    The lookup's columns are renamed behind the scenes so that a lookup on the
    same table the pipeline is reading from cannot produce an ambiguous-column
    self-join; conditions still address them by their original names.
    """

    def __init__(self, frame: DataFrame, prefix: str) -> None:
        self._frame = frame
        self._prefix = prefix

    def __getitem__(self, name: str) -> Column:
        return self._frame[f"{self._prefix}{name}"]


def connected_lookup(
    rows: DataFrame,
    lookup: DataFrame,
    condition: Callable[[DataFrame, LookupSide], Column],
    outputs: Mapping[str, str],
) -> DataFrame:
    """A cached, connected Lookup with 'Lookup policy on multiple match = Use Any Value'.

    Why this is not a left join: a left join fans out when the lookup source has
    several matching rows, and a PowerCenter Lookup never changes the row count -
    it returns exactly one row per input row (or NULLs on a miss).  Silent row
    multiplication is the single easiest way to break a ported mapping, so the
    helper collapses the match set back to one row per input row.

    'Use Any Value' is nondeterministic in PowerCenter (it returns whatever the
    cache yields first).  Reproducing nondeterminism is not useful, so the match
    is made deterministic by ordering on the returned columns and taking the
    first - a defensible choice that also makes the port testable.  Mappings that
    genuinely depend on which duplicate is returned would be broken already.

    `outputs` maps output column name -> lookup source column name.
    """
    collisions = sorted(set(outputs) & set(rows.columns))
    if collisions:
        raise ValueError(
            f"lookup output(s) {collisions} would shadow an incoming port; "
            "give the lookup's return ports distinct names, as PowerCenter does"
        )
    prefix = "__lkp_"
    tagged = rows.withColumn(_ROW_ID, F.monotonically_increasing_id())
    renamed = lookup.select([F.col(c).alias(f"{prefix}{c}") for c in lookup.columns])
    joined = tagged.join(renamed, condition(tagged, LookupSide(renamed, prefix)), "left")

    order = [F.col(f"{prefix}{src}").asc_nulls_last() for src in outputs.values()]
    window = Window.partitionBy(_ROW_ID).orderBy(*order)
    picked = joined.withColumn(_LKP_RANK, F.row_number().over(window)).filter(F.col(_LKP_RANK) == 1)
    return picked.select(
        *[tagged[c] for c in rows.columns],
        *[F.col(f"{prefix}{src}").alias(name) for name, src in outputs.items()],
    )


def router(rows: DataFrame, groups: Mapping[str, Column]) -> dict[str, DataFrame]:
    """A Router: every group is evaluated independently against every row.

    A Router is not a chain of else-ifs - a row satisfying two group conditions is
    emitted to both - so this cannot be collapsed into a single CASE expression.
    The returned dict also carries the DEFAULT group (rows matching no condition),
    which is where PowerCenter quietly drops rows when the group conditions do not
    cover the domain.
    """
    result = {name: rows.filter(condition) for name, condition in groups.items()}
    matched_any = F.lit(False)
    for condition in groups.values():
        # `isNotNull() & condition` keeps the clause boolean-false (never NULL) for
        # rows where the group condition is NULL, which is how a Router treats them.
        matched_any = matched_any | (condition.isNotNull() & condition)
    result["DEFAULT"] = rows.filter(~matched_any)
    return result


def aggregate_all_input(
    rows: DataFrame,
    aggregations: Sequence[Column],
    group_by: Sequence[str] = (),
) -> DataFrame:
    """An Aggregator with 'Transformation Scope = All Input'.

    With no GROUP BY ports the transformation emits exactly one row, including for
    an empty input - Spark's `groupBy().agg()` behaves the same way, but
    `groupBy(cols).agg()` on empty input returns no rows, matching PowerCenter.

    Interpretation: PowerCenter's COUNT(port) ignores NULLs (COUNT(*) does not);
    `F.count(col)` matches that.
    """
    if group_by:
        return rows.groupBy(*group_by).agg(*aggregations)
    return rows.agg(*aggregations)


def flag_rows(rows: DataFrame, strategy: int, column: str = "PC_UPDATE_STRATEGY") -> DataFrame:
    """An Update Strategy transformation: tag rows with their DD_* disposition."""
    return rows.withColumn(column, F.lit(strategy))


def split_update_strategy(
    rows: DataFrame,
    target: DataFrame,
    keys: Sequence[str],
    column: str = "PC_UPDATE_STRATEGY",
) -> tuple[DataFrame, DataFrame]:
    """Split DD_UPDATE-flagged rows into (applied, rejected) against the target.

    A session with 'Insert = YES, Update as Update = YES, Update else Insert = NO'
    only applies a DD_UPDATE row when the key already exists; a row whose key is
    absent is *rejected to the .bad file*, not inserted.  A MERGE with
    WHEN NOT MATCHED THEN INSERT would therefore change behaviour, and the
    rejected rows - which the legacy job surfaces only as a file on the
    Integration Service host - need somewhere to go on Databricks.
    """
    if not keys:
        raise ValueError("split_update_strategy() needs the target's key columns")
    flagged = rows.filter(F.col(column) == DD_UPDATE)
    existing = target.select(*keys).distinct()
    condition = None
    for key in keys:
        clause = flagged[key].eqNullSafe(existing[key])
        condition = clause if condition is None else (condition & clause)
    return flagged.join(existing, condition, "left_semi"), flagged.join(existing, condition, "left_anti")
