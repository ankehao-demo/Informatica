"""Render docs/migration/EHRP2BIIS_UPDATE_mapping_spec.md from mapping_spec.json and
the expression catalog. Re-run after regenerating the JSON:

    python tools/infa_xml_spec.py XML/EHRP2BIIS_UPDATE pipelines/ehrp2biis/mapping_spec.json
    python tools/render_mapping_spec.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipelines.ehrp2biis.expressions import CATALOG
from pipelines.ehrp2biis.transform import load_spec

OUT = ROOT / "docs" / "migration" / "EHRP2BIIS_UPDATE_mapping_spec.md"

FUNCTION_MAP = [
    ("IIF(c, a, b)", "CASE WHEN c THEN a ELSE b END", "F.when(c, a).otherwise(b)", "NULL condition = FALSE -> b"),
    ("DECODE(TRUE, c1, v1, ..., d)", "CASE WHEN c1 THEN v1 ... ELSE d END", "infa.decode_true", "first TRUE wins; NULL conditions never match"),
    ("DECODE(x, s1, r1, ..., d)", "CASE x WHEN s1 THEN r1 ... ELSE d END", "infa.decode_value", "NULL x never matches"),
    ("IN(x, a, b)", "x IN (a, b)", "col.isin(a, b)", "NULL x -> NULL (three-valued)"),
    ("NOT(...)", "NOT (...)", "~col", "NOT NULL -> NULL"),
    ("IS_SPACES(x)", "x RLIKE '^[ \\t\\n\\r\\f\\v]*$'", "infa.is_spaces", "NULL -> NULL; '' and ' ' -> TRUE"),
    ("ISNULL(x)", "x IS NULL", "col.isNull()", ""),
    ("a || b", "CASE WHEN a IS NULL AND b IS NULL THEN NULL ELSE concat(coalesce(a,''), coalesce(b,'')) END", "infa.concat",
     "NULL operand = '' (Spark concat would return NULL)"),
    ("RTRIM(x) / UPPER(x)", "rtrim(x) / upper(x)", "F.rtrim / F.upper", "RTRIM trims blanks only"),
    ("GET_DATE_PART(d, 'YYYY'|'MM'|'DD')", "year(d) / month(d) / day(d)", "F.year / F.month / F.dayofmonth", ""),
    ("GET_DATE_PART(SYSDATE, 'MM')", "month(:run_date)", "F.month(run_ts)", "SYSDATE pinned to the run's logical date"),
    ("trunc(sysdate)", "CAST(trunc(:run_date,'DD') AS TIMESTAMP)", "F.date_trunc('DD', run_ts)", "LOAD_DATE / AS_OF_DAY"),
    ("ADD_TO_DATE(d, 'MM', n)", "d + make_interval(0, n)", "infa.add_to_date_months", "day clamped to month end, time kept"),
    ("TO_CHAR(int)", "CAST(x AS STRING)", "infa.to_char_int", ""),
    ("implicit string -> number", "leading numeric part, else 0", "infa.string_to_number", "' ' -> 0 (TO_DECIMAL rules)"),
]


def esc(s) -> str:
    return "" if s is None else " ".join(str(s).split()).replace("|", "\\|")


def main() -> int:
    spec = load_spec()
    t_by = {t["name"]: t for t in spec["transformations"]}
    lineage = spec["target_lineage"]
    n_fields = sum(len(t["fields"]) for t in spec["transformations"])
    o = [
        "# m_EHRP2BIIS_UPDATE -- mapping specification",
        "",
        (f"Generated from `{spec['source_file']}` by `tools/infa_xml_spec.py` + `tools/render_mapping_spec.py`. "
        "Do not edit by hand."),
        "",
        f"* Folder `{spec['folder']}`, workflow `{spec['workflow']}`, session `{spec['session']}`, mapping `{spec['mapping']}`",
        f"* {len(spec['transformations'])} transformations, {n_fields} TRANSFORMFIELDs, {len(spec['connectors'])} connectors",
        "* Session: " + ", ".join(f"{k} = `{v}`" for k, v in spec["session_attributes"].items()),
        (f"* Schedule: start {spec['schedule']['start_date']} {spec['schedule']['start_time']}, recurring "
        f"`{spec['schedule']['recurring']}` (daily)"),
        ("* No Router, Filter or Update Strategy transformation exists: every row is an INSERT. The port uses "
        "Delta `MERGE` on deterministic keys so reruns replace the load slice instead of duplicating it "
        "(see `pipelines/ehrp2biis/writers.py`)."),
        ("* Reconciliation keys: `LOAD_ID = 'NK' || YYYYMMDD` and `LOAD_DATE = trunc(sysdate)` are preserved exactly "
        "(sysdate = the run's logical date)."),
        "",
        "## 1. Stage order (PowerCenter -> Spark)",
        "",
        "| # | Instance | Type | Spark implementation |",
        "|---|---|---|---|",
    ]
    from pipelines.ehrp2biis.transform import MappingGraph

    impl = {
        "Source Qualifier": "`source_qualifier`: PS_GVT_JOB INNER JOIN NWK_NEW_EHRP_ACTIONS_TBL on 4 keys",
        "Expression": "`expression`: input ports -> variable ports in order -> output ports (catalog)",
        "Lookup Procedure": "`lookup`: LEFT JOIN on the lookup condition, one row per key (Use Any Value)",
    }
    for i, inst in enumerate(MappingGraph(spec).order(), 1):
        o.append(f"| {i} | `{inst}` | {t_by[inst]['type']} | {impl[t_by[inst]['type']]} |")
    o += ["| - | targets | Relational Writer | `conform_targets` (cast/truncate to target type, reject NOT NULL / overflow) + `writers.merge_targets` |", ""]

    o += ["## 2. Function translation", "", "| Informatica | Spark SQL | PySpark | Semantics preserved |", "|---|---|---|---|"]
    o += [f"| `{esc(a)}` | `{esc(b)}` | `{esc(c)}` | {esc(d)} |" for a, b, c, d in FUNCTION_MAP]

    sq = t_by["SQ_PS_GVT_JOB"]
    o += ["", "## 3. Sources and Source Qualifier", ""]
    for s in spec["sources"]:
        o.append(f"* `{s['owner']}.{s['name']}` ({len(s['fields'])} columns)")
    o += ["", "SQ override (verbatim):", "", "```sql", sq["attributes"].get("Sql Query", "").replace("\r", ""), "```", "",
          ("Note: `ORDER BY PS_GVT_JOB.EFFDT` is what makes the stateful `v_EVENT_ID` sequence reproducible; the port "
          "orders by `EFFDT, EMPLID, EMPL_RCD, EFFSEQ` (tie-break documented in ASSUMPTIONS.md).")]

    o += ["", "## 4. Lookup procedures", "",
          "| Lookup | Table | Condition | Policy | Conn | Case-sensitive | SQL override | Output ports |", "|---|---|---|---|---|---|---|---|"]
    for t in spec["transformations"]:
        if t["type"] != "Lookup Procedure":
            continue
        a = t["attributes"]
        outs = [f["name"] for f in t["fields"] if f["porttype"] == "LOOKUP/OUTPUT"]
        o.append(f"| `{t['name']}` | {a.get('Lookup table name')} | `{esc(a.get('Lookup condition'))}` | "
                 f"{a.get('Lookup policy on multiple match')} | {a.get('Connection Information')} | "
                 f"{a.get('Case Sensitive String Comparison')} | {('`' + esc(a['Lookup Sql Override']) + '`') if a.get('Lookup Sql Override') else '-'} | "
                 f"{len(outs)}: {', '.join(outs[:12])}{' ...' if len(outs) > 12 else ''} |")
    o += ["", ("PySpark: each lookup is `wide.join(lkp, keys, 'left')` where `lkp` is the table (or the override run "
          "verbatim through Spark SQL with schema owners stripped), de-duplicated with `row_number()` per condition "
          "key ordered by the output ports (deterministic stand-in for *Use Any Value*). Keys with more than one "
          "candidate row are written to `EHRP2BIIS_LOOKUP_MULTIMATCH` for review. String keys are compared with "
          "`upper()` because *Case Sensitive String Comparison = NO*.")]

    o += ["", "## 5. Expression ports (business logic)", ""]
    for name in ("exp_GET_EFFDT_YEAR", "exp_PERS_DATA", "exp_MAIN2BIIS"):
        t = t_by[name]
        o += [f"### {name}", "", "| Port | Port type | Datatype | Informatica expression | Spark SQL equivalent | Notes |",
              "|---|---|---|---|---|---|"]
        for f in t["fields"]:
            dtype = f"{f['datatype']}({f['precision']},{f['scale']})"
            e = f["expression"]
            if f["porttype"] in ("LOCAL VARIABLE", "OUTPUT"):
                c = CATALOG[f["name"]]
                o.append(f"| `{f['name']}` | {f['porttype']} | {dtype} | `{esc(e)}` | `{esc(c.sql)}` | {esc(c.note)} |")
            else:
                up = next((f"{x['from_instance']}.{x['from_field']}" for x in spec["connectors"]
                           if x["to_instance"] == name and x["to_field"] == f["name"]), "unconnected (NULL)")
                o.append(f"| `{f['name']}` | {f['porttype']} | {dtype} | `{esc(e) or f['name']}` | pass-through of `{up}` | |")
        o.append("")

    o += ["## 6. Targets", ""]
    for tgt in sorted(spec["targets"], key=lambda x: x["name"]):
        w = spec["relational_writers"].get(tgt["name"], {})
        o += [f"### {tgt['name']}", "",
              "Relational Writer: " + ", ".join(f"{k} = `{w.get(k)}`" for k in
                                                ("Insert", "Update as Update", "Update as Insert", "Update else Insert", "Delete",
                                                 "Truncate target table option", "Reject file directory")),
              "",
              f"{len(lineage[tgt['name']])} of {len(tgt['fields'])} columns are connected; unconnected columns load NULL.",
              "", "| Column | Oracle type | Nullable | Source port |", "|---|---|---|---|"]
        for f in tgt["fields"]:
            src = lineage[tgt["name"]].get(f["name"])
            typ = f"{f['datatype']}({f['precision']},{f['scale']})" if f["datatype"] != "date" else "date"
            o.append(f"| {f['name']} | {typ} | {f['nullable']} | {('`' + src[0] + '`') if src else '-'} |")
        o.append("")

    o += ["## 7. All TRANSFORMFIELDs (pass-through and lookup ports)", "",
          "Complete port inventory for the remaining transformations (expression ports are in section 5).", ""]
    for t in spec["transformations"]:
        if t["type"] == "Expression":
            continue
        o += [f"<details><summary><code>{t['name']}</code> ({t['type']}, {len(t['fields'])} ports)</summary>", "",
              "| Port | Port type | Datatype |", "|---|---|---|"]
        o += [f"| {f['name']} | {f['porttype']} | {f['datatype']}({f['precision']},{f['scale']}) |" for f in t["fields"]]
        o += ["", "</details>", ""]
    OUT.write_text("\n".join(o) + "\n")
    print(f"wrote {OUT} ({len(o)} lines)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
