"""Parse an Informatica PowerCenter workflow export into a machine-readable spec.

Usage:
    python tools/infa_xml_spec.py XML/EHRP2BIIS_UPDATE docs/migration/ehrp2biis_update_spec.json

The JSON is the single source of truth used by the PySpark job (target schemas),
by the mapping-spec renderer and by the lineage parity tests.
"""
from __future__ import annotations

import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

LOOKUP_ATTRS = (
    "Lookup table name",
    "Lookup Sql Override",
    "Lookup condition",
    "Lookup policy on multiple match",
    "Connection Information",
    "Lookup caching enabled",
    "Case Sensitive String Comparison",
    "Null ordering",
)
SQ_ATTRS = ("User Defined Join", "Sql Query", "Source Filter", "Select Distinct", "Number Of Sorted Ports")


def _field(el: ET.Element, *names: str) -> dict:
    return {n.lower(): el.get(n) for n in names}


def parse(path: str) -> dict:
    root = ET.parse(path).getroot()
    folder = next(root.iter("FOLDER"))
    mapping = next(root.iter("MAPPING"))

    sources = [
        {
            "name": s.get("NAME"),
            "owner": s.get("OWNERNAME"),
            "dbd": s.get("DBDNAME"),
            "fields": [
                _field(f, "NAME", "DATATYPE", "PRECISION", "SCALE", "NULLABLE", "KEYTYPE")
                for f in s.iter("SOURCEFIELD")
            ],
        }
        for s in folder.findall("SOURCE")
    ]
    targets = [
        {
            "name": t.get("NAME"),
            "fields": [
                _field(f, "NAME", "DATATYPE", "PRECISION", "SCALE", "NULLABLE", "KEYTYPE")
                for f in t.iter("TARGETFIELD")
            ],
        }
        for t in folder.findall("TARGET")
    ]

    transformations = []
    for t in mapping.findall("TRANSFORMATION"):
        attrs = {a.get("NAME"): a.get("VALUE") for a in t.findall("TABLEATTRIBUTE")}
        keep = LOOKUP_ATTRS if t.get("TYPE") == "Lookup Procedure" else SQ_ATTRS
        transformations.append(
            {
                "name": t.get("NAME"),
                "type": t.get("TYPE"),
                "fields": [
                    {
                        "name": f.get("NAME"),
                        "datatype": f.get("DATATYPE"),
                        "precision": f.get("PRECISION"),
                        "scale": f.get("SCALE"),
                        "porttype": f.get("PORTTYPE"),
                        "expression": (f.get("EXPRESSION") or "").strip() or None,
                        "default_value": f.get("DEFAULTVALUE") or None,
                    }
                    for f in t.findall("TRANSFORMFIELD")
                ],
                "attributes": {k: v for k, v in attrs.items() if k in keep and v not in (None, "")},
            }
        )

    connectors = [
        {
            "from_instance": c.get("FROMINSTANCE"),
            "from_field": c.get("FROMFIELD"),
            "to_instance": c.get("TOINSTANCE"),
            "to_field": c.get("TOFIELD"),
        }
        for c in mapping.findall("CONNECTOR")
    ]
    load_order = [
        {"target": x.get("TARGETINSTANCE"), "order": x.get("ORDER")} for x in mapping.findall("TARGETLOADORDER")
    ]

    session = next(root.iter("SESSION"))
    writers = {}
    for ext in session.findall("SESSIONEXTENSION"):
        if ext.get("SUBTYPE") == "Relational Writer":
            writers[ext.get("SINSTANCENAME")] = {a.get("NAME"): a.get("VALUE") for a in ext.findall("ATTRIBUTE")}
    session_attrs = {
        a.get("NAME"): a.get("VALUE")
        for a in session.findall("ATTRIBUTE")
        if a.get("NAME") in ("Treat source rows as", "Commit Type", "Commit Interval", "Commit On End Of File", "Recovery Strategy")
    }
    workflow = next(root.iter("WORKFLOW"))
    sched = workflow.find(".//SCHEDULEINFO")
    schedule = None
    if sched is not None:
        start, rec = sched.find("STARTOPTIONS"), sched.find(".//RECURRING")
        schedule = {
            "start_date": start.get("STARTDATE") if start is not None else None,
            "start_time": start.get("STARTTIME") if start is not None else None,
            "recurring": dict(rec.attrib) if rec is not None else None,
        }

    return {
        "source_file": str(path),
        "folder": folder.get("NAME"),
        "mapping": mapping.get("NAME"),
        "workflow": workflow.get("NAME"),
        "session": session.get("NAME"),
        "session_attributes": session_attrs,
        "schedule": schedule,
        "sources": sources,
        "targets": targets,
        "transformations": transformations,
        "connectors": connectors,
        "target_load_order": load_order,
        "relational_writers": writers,
    }


def target_lineage(spec: dict) -> dict[str, dict[str, list[str]]]:
    """target table -> target column -> ["instance.port", ...] (from CONNECTORs)."""
    target_names = {t["name"] for t in spec["targets"]}
    out: dict[str, dict[str, list[str]]] = {t: {} for t in target_names}
    for c in spec["connectors"]:
        if c["to_instance"] in target_names:
            out[c["to_instance"]].setdefault(c["to_field"], []).append(f'{c["from_instance"]}.{c["from_field"]}')
    return out


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(__doc__)
        return 2
    spec = parse(argv[1])
    spec["target_lineage"] = target_lineage(spec)
    Path(argv[2]).write_text(json.dumps(spec, indent=2, sort_keys=False) + "\n")
    print(f"wrote {argv[2]}: {len(spec['transformations'])} transformations, {len(spec['connectors'])} connectors")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
