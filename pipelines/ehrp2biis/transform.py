"""Spark DataFrame implementation of m_EHRP2BIIS_UPDATE.

The mapping graph (ports, connectors, lookup conditions/overrides, target lineage and
target column types) is read from ``mapping_spec.json`` -- generated from the XML by
``tools/infa_xml_spec.py`` -- so the port wiring is not re-typed by hand. Only the
expression logic lives in code (``expressions.py``).

Stage order replicates PowerCenter:
    SQ_PS_GVT_JOB (inner join)  ->  exp_GET_EFFDT_YEAR  ->  10 lookups (LEFT JOIN,
    "Use Any Value")  ->  exp_PERS_DATA  ->  exp_MAIN2BIIS  ->  target conform/reject
The session has no Router / Update Strategy and "Treat source rows as" = Insert, so
every row is an insert; MERGE semantics are applied by ``writers.py``.
"""
from __future__ import annotations

import datetime as dt
import json
import re
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .expressions import CATALOG
from .infa import string_to_number

SPEC_PATH = Path(__file__).with_name("mapping_spec.json")
SQ = "SQ_PS_GVT_JOB"
EXP_MAIN = "exp_MAIN2BIIS"
EVENT_ORDER = ("EFFDT", "EMPLID", "EMPL_RCD", "EFFSEQ")
NATURAL_KEY = ("EMPLID", "EMPL_RCD", "EFFDT", "EFFSEQ")
# Session has no "DateTime Format String" override -> PowerCenter default MM/DD/YYYY HH24:MI:SS
INFA_DEFAULT_DATETIME_FORMAT = "MM/dd/yyyy HH:mm:ss"


def load_spec(path: Path | str = SPEC_PATH) -> dict:
    return json.loads(Path(path).read_text())


def col_name(instance: str, port: str) -> str:
    return f"{instance}__{port}"


@dataclass
class TransformResult:
    targets: dict[str, DataFrame]
    rejects: DataFrame
    lookup_stats: list[dict] = field(default_factory=list)


class MappingGraph:
    def __init__(self, spec: dict):
        self.spec = spec
        self.sources = {s["name"]: s for s in spec["sources"]}
        self.targets = {t["name"]: t for t in spec["targets"]}
        self.trans = {t["name"]: t for t in spec["transformations"]}
        self.port_types = {(t["name"], f["name"]): f["datatype"] for t in spec["transformations"] for f in t["fields"]}
        self.upstream: dict[tuple[str, str], tuple[str, str]] = {}
        for c in spec["connectors"]:
            self.upstream[(c["to_instance"], c["to_field"])] = (c["from_instance"], c["from_field"])

    def order(self) -> list[str]:
        """Topological order of transformation instances (sources/targets excluded)."""
        deps: dict[str, set[str]] = defaultdict(set)
        for (to_i, _), (from_i, _) in self.upstream.items():
            if to_i in self.trans and from_i in self.trans:
                deps[to_i].add(from_i)
        indeg = {t: len(deps[t]) for t in self.trans}
        q = deque(sorted(t for t, d in indeg.items() if d == 0))
        out = []
        while q:
            n = q.popleft()
            out.append(n)
            for t in sorted(self.trans):
                if n in deps[t]:
                    deps[t].discard(n)
                    if not deps[t]:
                        q.append(t)
        if len(out) != len(self.trans):
            raise ValueError("cycle in mapping graph")
        return out


def _spark_type(infa_type: str, precision: str | None, scale: str | None) -> T.DataType:
    t = infa_type.lower()
    if t in ("string", "text", "nstring", "ntext", "varchar2", "char", "nvarchar2"):
        return T.StringType()
    if t in ("integer", "small integer"):
        return T.IntegerType()
    if t in ("bigint",):
        return T.LongType()
    if t in ("decimal", "number(p,s)", "number", "double"):
        p = int(precision or 38) or 38
        s = int(scale or 0)
        return T.DecimalType(min(p, 38), min(s, p, 38))
    if t in ("date/time", "date", "timestamp"):
        return T.TimestampType()
    raise ValueError(f"unmapped datatype {infa_type}")


def target_schema(spec: dict, target: str) -> T.StructType:
    t = next(x for x in spec["targets"] if x["name"] == target)
    return T.StructType(
        [
            T.StructField(f["name"], _spark_type(f["datatype"], f["precision"], f["scale"]), f["nullable"] != "NOTNULL")
            for f in t["fields"]
        ]
    )


def _strip_owner(sql: str) -> str:
    return re.sub(r"\b(EHRP|NKNIGHT|HISTDBA)\.", "", sql, flags=re.IGNORECASE)


class Ehrp2BiisTransform:
    def __init__(self, spark: SparkSession, spec: dict | None = None):
        self.spark = spark
        self.spec = spec or load_spec()
        self.g = MappingGraph(self.spec)
        self.lookup_stats: list[dict] = []

    # ------------------------------------------------------------------ helpers
    def _resolve(self, instance: str, port: str) -> str | None:
        """Wide-frame column holding the value of ``instance.port`` (None = unconnected)."""
        if instance in self.g.trans:
            return col_name(instance, port)
        return None

    def _input_value(self, instance: str, port: str) -> Column:
        up = self.g.upstream.get((instance, port))
        if up is None:
            return F.lit(None)
        return F.col(col_name(*up))

    # ------------------------------------------------------------------ stages
    def source_qualifier(self, tables: dict[str, DataFrame]) -> DataFrame:
        """SQ_PS_GVT_JOB: PS_GVT_JOB INNER JOIN NWK_NEW_EHRP_ACTIONS_TBL on the 4 keys."""
        actions = tables["NWK_NEW_EHRP_ACTIONS_TBL"].select(*NATURAL_KEY).alias("n")
        job = tables["PS_GVT_JOB"].alias("j")
        cond = [F.col(f"n.{k}") == F.col(f"j.{k}") for k in NATURAL_KEY]
        joined = job.join(actions, cond, "inner")
        cols = []
        for f in self.g.trans[SQ]["fields"]:
            src_inst, src_field = self.g.upstream.get((SQ, f["name"]), (None, None))
            if src_inst == "PS_GVT_JOB":
                cols.append(F.col(f"j.{src_field}").alias(col_name(SQ, f["name"])))
            elif src_inst == "NWK_NEW_EHRP_ACTIONS_TBL":
                cols.append(F.col(f"n.{src_field}").alias(col_name(SQ, f["name"])))
            else:
                cols.append(F.lit(None).alias(col_name(SQ, f["name"])))
        return joined.select(*cols)

    def expression(self, wide: DataFrame, name: str, ctx: dict) -> DataFrame:
        t = self.g.trans[name]
        new_cols: dict[str, Column] = {}
        for f in t["fields"]:
            if f["porttype"] in ("INPUT", "INPUT/OUTPUT"):
                new_cols[col_name(name, f["name"])] = self._input_value(name, f["name"])
        wide = wide.select("*", *[c.alias(n) for n, c in new_cols.items()])

        if name == EXP_MAIN:
            wide = self._event_id_columns(wide, name)

        def c(port: str) -> Column:
            return F.col(col_name(name, port))

        for f in t["fields"]:
            if f["porttype"] not in ("LOCAL VARIABLE", "OUTPUT"):
                continue
            entry = CATALOG.get(f["name"])
            if entry is None:
                raise KeyError(f"no expression catalog entry for {name}.{f['name']}: {f['expression']}")
            value = entry.pyspark(c, ctx)
            dtype = _spark_type(f["datatype"], f["precision"], f["scale"])
            if isinstance(dtype, T.StringType):
                value = F.substring(value.cast("string"), 1, int(f["precision"]))
            elif not isinstance(dtype, T.DecimalType):
                value = value.cast(dtype)
            wide = wide.withColumn(col_name(name, f["name"]), value)
        return wide

    def _event_id_columns(self, wide: DataFrame, name: str) -> DataFrame:
        """Emulate v_EVENT_ID / v_PREVIOUS_YEAR (stateful variable ports).

        Rows arrive ordered by EFFDT (SQ ORDER BY), so each calendar year is one run;
        v_EVENT_ID restarts at EHRP_SEQ_NUMBER + 1 at the start of every run.
        """
        c = lambda p: F.col(col_name(name, p))
        order = [c(k).asc_nulls_last() for k in EVENT_ORDER]
        year = F.year(c("EFFDT"))
        w_all = Window.orderBy(*order)
        w_year = Window.partitionBy(year).orderBy(*order)
        return (
            wide.withColumn(col_name(name, "__PREV_YEAR"), F.lag(year).over(w_all))
            .withColumn(
                col_name(name, "__EVENT_ID"),
                (c("EHRP_SEQ_NUMBER") + F.row_number().over(w_year)).cast("int"),
            )
        )

    def lookup(self, wide: DataFrame, name: str, tables: dict[str, DataFrame]) -> DataFrame:
        t = self.g.trans[name]
        attrs = t["attributes"]
        table = attrs["Lookup table name"]
        override = attrs.get("Lookup Sql Override")
        if override:
            for tbl_name, df in tables.items():
                df.createOrReplaceTempView(tbl_name)
            src = self.spark.sql(_strip_owner(override))
        else:
            src = tables[table]
        lkp_ports = {f["name"]: f for f in t["fields"] if f["porttype"] in ("LOOKUP", "LOOKUP/OUTPUT")}
        out_ports = [f["name"] for f in t["fields"] if f["porttype"] == "LOOKUP/OUTPUT"]

        conds = [tuple(x.strip() for x in part.split("=")) for part in re.split(r"\s+AND\s+", attrs["Lookup condition"])]
        key_cols_l, key_cols_r = [], []
        for i, (lkp_col, in_port) in enumerate(conds):
            kname = f"__k{i}"
            lval = F.col(lkp_col)
            rval = F.col(col_name(name, in_port))
            if lkp_ports[lkp_col]["datatype"] in ("string", "text") and attrs.get("Case Sensitive String Comparison") == "NO":
                lval, rval = F.upper(lval), F.upper(rval)
            key_cols_l.append(lval.alias(kname))
            key_cols_r.append((kname, rval))

        # "Use Any Value": keep one deterministic row per condition key.
        keys = [k for k, _ in key_cols_r]
        lk = src.select(*key_cols_l, *[F.col(p).alias(col_name(name, p)) for p in out_ports])
        w = Window.partitionBy(*keys).orderBy(*[F.col(col_name(name, p)).asc_nulls_last() for p in out_ports])
        lk = lk.withColumn("__rn", F.row_number().over(w)).withColumn("__cnt", F.count(F.lit(1)).over(Window.partitionBy(*keys)))
        self.lookup_stats.append({"lookup": name, "frame": lk.filter("__rn = 1 AND __cnt > 1").select(*keys, "__cnt")})
        lk = lk.filter("__rn = 1").drop("__rn", "__cnt")

        for f in t["fields"]:
            if f["porttype"] in ("INPUT", "INPUT/OUTPUT"):
                wide = wide.withColumn(col_name(name, f["name"]), self._input_value(name, f["name"]))
        for kname, rval in key_cols_r:
            wide = wide.withColumn(kname, rval)
        return wide.join(lk, keys, "left").drop(*keys)

    # ------------------------------------------------------------------ targets
    def conform_targets(self, wide: DataFrame) -> tuple[dict[str, DataFrame], DataFrame]:
        lineage = self.spec["target_lineage"]
        nk = [F.col(col_name(SQ, k)).alias(f"__{k}") for k in NATURAL_KEY]
        targets, rejects = {}, []
        for tname in sorted(self.g.targets, key=self._load_order):
            schema = target_schema(self.spec, tname)
            cols, checks = [], []
            for fld, tf in zip(schema.fields, self.g.targets[tname]["fields"]):
                srcs = lineage[tname].get(fld.name)
                src_port = tuple(srcs[0].split(".", 1)) if srcs else None
                raw = F.col(col_name(*src_port)) if src_port else F.lit(None)
                src_is_string = src_port and self.g.port_types.get(src_port) in ("string", "text")
                src_is_date = src_port and self.g.port_types.get(src_port) == "date/time"
                if isinstance(fld.dataType, T.StringType):
                    if src_is_date:  # implicit date -> string uses the default DateTime Format String
                        raw = F.date_format(raw, INFA_DEFAULT_DATETIME_FORMAT)
                    val = F.substring(raw.cast("string"), 1, int(tf["precision"]))
                elif isinstance(fld.dataType, T.DecimalType) and src_is_string:
                    raw = string_to_number(raw, T.DecimalType(38, 10))
                    val = raw.cast(fld.dataType)
                else:
                    val = raw.cast(fld.dataType)
                cols.append(val.alias(fld.name))
                if not fld.nullable:
                    checks.append(F.when(val.isNull(), F.lit(f"{fld.name}: NOT NULL constraint")))
                if isinstance(fld.dataType, T.DecimalType) and srcs:
                    checks.append(
                        F.when(raw.isNotNull() & val.isNull(), F.lit(f"{fld.name}: value exceeds NUMBER({tf['precision']},{tf['scale']})"))
                    )
            reason = F.concat_ws("; ", *checks) if checks else F.lit("")
            df = wide.select(*cols, *nk, reason.alias("__reject_reason"))
            ok = df.filter(F.col("__reject_reason") == "")
            bad = df.filter(F.col("__reject_reason") != "")
            targets[tname] = ok.drop("__reject_reason", *[f"__{k}" for k in NATURAL_KEY])
            rejects.append(
                bad.select(
                    F.lit(tname).alias("TARGET_TABLE"),
                    *[F.col(f"__{k}").cast("string").alias(k) for k in NATURAL_KEY],
                    F.col("__reject_reason").alias("REJECT_REASON"),
                )
            )
        rej = rejects[0]
        for r in rejects[1:]:
            rej = rej.unionByName(r)
        return targets, rej

    def _load_order(self, target: str) -> tuple[int, int]:
        order = {x["target"]: (int(x["order"]), i) for i, x in enumerate(self.spec["target_load_order"])}
        return order.get(target, (99, 99))

    # ------------------------------------------------------------------ driver
    def run(self, tables: dict[str, DataFrame], run_date: dt.date) -> TransformResult:
        ctx = {"run_ts": F.lit(dt.datetime.combine(run_date, dt.time())).cast("timestamp"), "run_date": run_date}
        self.lookup_stats = []
        wide = None
        for inst in self.g.order():
            ttype = self.g.trans[inst]["type"]
            if ttype == "Source Qualifier":
                wide = self.source_qualifier(tables)
            elif ttype == "Expression":
                wide = self.expression(wide, inst, ctx)
            elif ttype == "Lookup Procedure":
                wide = self.lookup(wide, inst, tables)
            else:
                raise NotImplementedError(f"{inst}: {ttype}")
        targets, rejects = self.conform_targets(wide)
        return TransformResult(targets=targets, rejects=rejects, lookup_stats=self.lookup_stats)


def load_id_for(run_date: dt.date) -> str:
    return "NK" + run_date.strftime("%Y%m%d")
