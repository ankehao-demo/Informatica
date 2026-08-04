#!/usr/bin/env python3
"""Generate docs/INFORMATICA_INVENTORY.md from the exported PowerCenter XML in XML/.

The exports are repository-level POWERMART documents: one FOLDER per file, each
containing SOURCE / TARGET / MAPPING / SESSION / WORKFLOW definitions.  This
script walks them and emits a migration-scoping inventory: every mapping with
its sources, targets, transformation chain and a complexity rating.

Usage:  python3 tools/inventory_powercenter.py [--check]

  --check  regenerate in memory and fail if docs/INFORMATICA_INVENTORY.md is stale
"""

from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
XML_DIR = REPO_ROOT / "XML"
OUTPUT = REPO_ROOT / "docs" / "INFORMATICA_INVENTORY.md"

# Per-transformation-type weight used for the complexity score.  Weights reflect
# porting effort rather than runtime cost: a Normalizer or an Update Strategy has
# no clean Spark equivalent, an Expression usually maps 1:1 to withColumn.
TYPE_WEIGHTS = {
    "Source Qualifier": 1,
    "Expression": 1,
    "Filter": 1,
    "Sorter": 1,
    "Sequence": 2,
    "Router": 3,
    "Aggregator": 3,
    "Joiner": 3,
    "Lookup Procedure": 3,
    "Update Strategy": 4,
    "Normalizer": 4,
}
DEFAULT_WEIGHT = 3

COMPLEXITY_BANDS = [(10, "Low"), (25, "Medium"), (45, "High")]
VERY_HIGH = "Very High"

# Mappings that have a PySpark equivalent in this repository.
PORTED = [
    (
        "m_Pay_Calendar_Set_Pay_Calendar",
        "Pay_Calendar",
        (
            "the only Router + Update Strategy pair in the estate, two lookups (one a date-range "
            "non-equijoin) and parameter-file driven branching"
        ),
        "pyspark/src/pcmigration/jobs/pay_calendar_set_pay_calendar.py",
    ),
    (
        "m_COMPTIME_Build_Message_Counters",
        "COMPTIME",
        (
            "the message/counter shape repeated in 14 mappings and ending 8 of the 11 workflows: "
            "flat-file source, record-type filter, aggregation, lookup, SETVARIABLE and two "
            "heterogeneous targets"
        ),
        "pyspark/src/pcmigration/jobs/comptime_build_message_counters.py",
    ),
]


@dataclass
class Mapping:
    export: str
    folder: str
    name: str
    description: str
    sources: list[str]
    targets: list[str]
    chain: Counter
    connectors: int
    parameters: list[str]
    variables: list[str]

    @property
    def score(self) -> int:
        """Weighted transformation score plus a term for port-level wiring volume.

        The connector count is what makes a mapping expensive to port by hand: two
        mappings can both be "Expression -> Expression" yet differ by 2000 wired
        ports.  It is bucketed (1 point per 100 connectors) so it informs the band
        without swamping the transformation weights.
        """
        weighted = sum(TYPE_WEIGHTS.get(t, DEFAULT_WEIGHT) * n for t, n in self.chain.items())
        return weighted + len(self.sources) + len(self.targets) + self.connectors // 100

    @property
    def complexity(self) -> str:
        for threshold, band in COMPLEXITY_BANDS:
            if self.score < threshold:
                return band
        return VERY_HIGH

    @property
    def chain_text(self) -> str:
        ordered = sorted(self.chain.items(), key=lambda kv: (-kv[1], kv[0]))
        return ", ".join(f"{t} x{n}" if n > 1 else t for t, n in ordered)


@dataclass
class Workflow:
    export: str
    folder: str
    name: str
    integration_service: str
    schedule: str
    sessions: list[str]
    other_tasks: list[str] = field(default_factory=list)
    links: list[tuple[str, str, str]] = field(default_factory=list)


def parse_folder(path: Path) -> tuple[list[Mapping], list[Workflow]]:
    root = ET.parse(path).getroot()
    folder = root.find(".//FOLDER")
    if folder is None:
        return [], []
    folder_name = folder.get("NAME") or path.name

    mappings = []
    for m in folder.findall("MAPPING"):
        instances = m.findall("INSTANCE")
        mappings.append(
            Mapping(
                export=path.name,
                folder=folder_name,
                name=m.get("NAME") or "",
                description=" ".join((m.get("DESCRIPTION") or "").split()),
                sources=sorted({i.get("TRANSFORMATION_NAME") or "" for i in instances if i.get("TYPE") == "SOURCE"}),
                targets=sorted({i.get("TRANSFORMATION_NAME") or "" for i in instances if i.get("TYPE") == "TARGET"}),
                chain=Counter(t.get("TYPE") or "?" for t in m.findall("TRANSFORMATION")),
                connectors=len(m.findall("CONNECTOR")),
                parameters=[v.get("NAME") or "" for v in m.findall("MAPPINGVARIABLE") if v.get("ISPARAM") == "YES"],
                variables=[v.get("NAME") or "" for v in m.findall("MAPPINGVARIABLE") if v.get("ISPARAM") != "YES"],
            )
        )

    workflows = []
    for w in folder.findall("WORKFLOW"):
        schedule = "unknown"
        for info in w.iter("SCHEDULEINFO"):
            schedule = info.get("SCHEDULETYPE") or schedule
        instances = w.findall("TASKINSTANCE")
        workflows.append(
            Workflow(
                export=path.name,
                folder=folder_name,
                name=w.get("NAME") or "",
                integration_service=w.get("SERVERNAME") or "",
                schedule=schedule,
                sessions=[i.get("NAME") or "" for i in instances if i.get("TASKTYPE") == "Session"],
                other_tasks=[
                    f"{i.get('NAME')} ({i.get('TASKTYPE')})" for i in instances if i.get("TASKTYPE") != "Session"
                ],
                links=[
                    (
                        link.get("FROMTASK") or "",
                        link.get("TOTASK") or "",
                        " ".join((link.get("CONDITION") or "").split()),
                    )
                    for link in w.findall("WORKFLOWLINK")
                ],
            )
        )
    return mappings, workflows


def render(mappings: list[Mapping], workflows: list[Workflow]) -> str:
    exports = sorted({m.export for m in mappings})
    folders = sorted({m.folder for m in mappings})
    bands = Counter(m.complexity for m in mappings)
    all_types = Counter()
    for m in mappings:
        all_types.update(m.chain)

    out: list[str] = []
    add = out.append

    add("# Informatica PowerCenter Inventory")
    add("")
    add(
        "Generated from the repository exports in `XML/` by `tools/inventory_powercenter.py`. "
        "Re-run that script after any repository re-export; do not hand-edit this file."
    )
    add("")
    add("## Summary")
    add("")
    add(f"- **Export files (`XML/`):** {len(exports)}")
    add(
        f"- **PowerCenter repository folders:** {len(folders)} ({', '.join(folders)}) — several "
        "exports were taken from the same folder, so the export file is the unit of scoping below"
    )
    add(f"- **Workflows:** {len(workflows)} (one per export, all `ON DEMAND` — externally scheduled, see below)")
    add(f"- **Mappings:** {len(mappings)}")
    add(f"- **Sessions:** {sum(len(w.sessions) for w in workflows)}")
    add(
        "- **Distinct transformation instances:** "
        f"{sum(all_types.values())} across {len(all_types)} transformation types"
    )
    add("")
    add("### Complexity spread")
    add("")
    add("| Complexity | Mappings | Share |")
    add("| --- | ---: | ---: |")
    for band in ("Low", "Medium", "High", VERY_HIGH):
        count = bands.get(band, 0)
        add(f"| {band} | {count} | {count / len(mappings):.0%} |")
    add(f"| **Total** | **{len(mappings)}** | |")
    add("")
    add(
        "Complexity is a weighted score over the mapping's transformation chain "
        "(Update Strategy / Normalizer weigh most, Expression / Filter least), plus its "
        "source and target count, plus one point per 100 port-level connectors. Bands: "
        "Low `<10`, Medium `<25`, High `<45`, Very High `>=45`. The connector term matters — "
        "several mappings wire more than 1,000 ports, which is the real hand-porting cost."
    )
    add("")
    add("### Transformation usage across all mappings")
    add("")
    add("| Transformation | Instances | Mappings using it |")
    add("| --- | ---: | ---: |")
    for t, n in sorted(all_types.items(), key=lambda kv: (-kv[1], kv[0])):
        used_by = sum(1 for m in mappings if t in m.chain)
        add(f"| {t} | {n} | {used_by} |")
    add("")
    add("### Mappings per export")
    add("")
    add("| Export | Repository folder | Mappings | Low | Medium | High | Very High |")
    add("| --- | --- | ---: | ---: | ---: | ---: | ---: |")
    for e in exports:
        fm = [m for m in mappings if m.export == e]
        fb = Counter(m.complexity for m in fm)
        add(
            f"| `XML/{e}` | {fm[0].folder} | {len(fm)} | {fb.get('Low', 0)} | {fb.get('Medium', 0)} | "
            f"{fb.get('High', 0)} | {fb.get(VERY_HIGH, 0)} |"
        )
    add("")
    add("### Highest-complexity mappings")
    add("")
    add("| Score | Export | Mapping | Chain |")
    add("| ---: | --- | --- | --- |")
    for m in sorted(mappings, key=lambda m: (-m.score, m.name))[:10]:
        add(f"| {m.score} | `XML/{m.export}` | `{m.name}` | {m.chain_text} |")
    add("")
    add("### Ported to PySpark so far")
    add("")
    add("| Mapping | Export | Why it is representative | Port |")
    add("| --- | --- | --- | --- |")
    for name, export, why, module in PORTED:
        add(f"| `{name}` | `XML/{export}` | {why} | [`{module}`](../{module}) |")
    add("")
    add(
        "The remaining mappings are not ported here; see `pyspark/MIGRATION_NOTES.md` for the "
        "translation patterns and the semantic traps that apply across the estate."
    )
    add("")

    add("## Mappings by export")
    add("")
    for e in exports:
        fm = sorted((m for m in mappings if m.export == e), key=lambda m: m.name)
        add(f"### `XML/{e}` (repository folder `{fm[0].folder}`)")
        add("")
        add("| Mapping | Sources | Targets | Transformation chain | Conn. | Score | Complexity |")
        add("| --- | --- | --- | --- | ---: | ---: | --- |")
        for m in fm:
            add(
                f"| `{m.name}` | {', '.join(m.sources) or '—'} | {', '.join(m.targets) or '—'} | "
                f"{m.chain_text or '—'} | {m.connectors} | {m.score} | {m.complexity} |"
            )
        add("")
        parameterised = [m for m in fm if m.parameters or m.variables]
        if parameterised:
            add("Mapping parameters / variables in this export:")
            add("")
            for m in parameterised:
                bits = []
                if m.parameters:
                    bits.append("parameters " + ", ".join(f"`{p}`" for p in m.parameters))
                if m.variables:
                    bits.append("variables " + ", ".join(f"`{v}`" for v in m.variables))
                add(f"- `{m.name}`: {'; '.join(bits)}")
            add("")
        described = [m for m in fm if m.description]
        if described:
            add("<details><summary>Mapping descriptions from the repository</summary>")
            add("")
            for m in described:
                add(f"- **`{m.name}`** — {m.description}")
            add("")
            add("</details>")
            add("")

    add("## Workflows and sessions")
    add("")
    add(
        "Every export ships exactly one workflow whose scheduler is `ON DEMAND`: nothing in "
        "the repository schedules itself. The workflows are started externally (`pmcmd` from "
        "cron, per the shell scripts in `Transfer Scripts/`), which is the piece that has no "
        "PowerCenter-side definition to migrate — see `pyspark/MIGRATION_NOTES.md`."
    )
    add("")
    for w in sorted(workflows, key=lambda w: w.export):
        add(f"### `{w.name}` (`XML/{w.export}`)")
        add("")
        add(f"- Integration Service: `{w.integration_service}` — schedule: `{w.schedule}`")
        add(f"- Sessions ({len(w.sessions)}): {', '.join(f'`{s}`' for s in sorted(w.sessions))}")
        if w.other_tasks:
            add(f"- Non-session tasks: {', '.join(f'`{t}`' for t in sorted(w.other_tasks))}")
        conditional = [ln for ln in w.links if ln[2]]
        add(f"- Task links: {len(w.links)} ({len(conditional)} guarded by a `$task.Status = Succeeded` condition)")
        add("")

    add("## Non-PowerCenter assets in this repository")
    add("")
    add(
        "- `Transfer Scripts/` — Korn shell SFTP senders (`cdc_transfer`, `nih_transfer_les`, "
        "`oig_transfer`, `fda_transfer`, `afps_transfer`, `nih_cpm_transfer`, `nih_les_transfer`). "
        "Each checks for an output file, SFTPs it to a partner host and emails a hard-coded "
        "distribution list."
    )
    add(
        "- `Maintenance Scripts/` — `archive_files` (renames processed files with a pay-period "
        "suffix) and `remove_file`."
    )
    add(
        "- `actstage_load`, `ehrp2biis_preload`, `Pseudossn` — shell drivers; "
        "`ehrp2biis_afterload.sql` — post-load SQL."
    )
    add("- `main.tf` — Terraform.")
    add("")
    return "\n".join(out) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail if the checked-in inventory is stale")
    args = parser.parse_args()

    mappings: list[Mapping] = []
    workflows: list[Workflow] = []
    for path in sorted(XML_DIR.iterdir()):
        if path.is_file():
            m, w = parse_folder(path)
            mappings.extend(m)
            workflows.extend(w)

    content = render(mappings, workflows)
    if args.check:
        current = OUTPUT.read_text(encoding="utf-8") if OUTPUT.exists() else ""
        if current != content:
            print(f"{OUTPUT.relative_to(REPO_ROOT)} is out of date; re-run tools/inventory_powercenter.py")
            return 1
        print(f"{OUTPUT.relative_to(REPO_ROOT)} is up to date ({len(mappings)} mappings).")
        return 0

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(content, encoding="utf-8")
    print(f"Wrote {OUTPUT.relative_to(REPO_ROOT)}: {len(mappings)} mappings, {len(workflows)} workflows.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
