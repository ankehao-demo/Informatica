"""Static checks: XML -> spec drift, catalog coverage, afterload order, external contracts."""
import hashlib
import json
import re
from pathlib import Path

from tools.infa_xml_spec import parse, target_lineage

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "pipelines" / "ehrp2biis" / "mapping_spec.json"


def test_spec_json_matches_xml():
    import os

    os.chdir(ROOT)
    spec = parse("XML/EHRP2BIIS_UPDATE")
    spec["target_lineage"] = target_lineage(spec)
    assert spec == json.loads(SPEC.read_text()), "regenerate: python tools/infa_xml_spec.py XML/EHRP2BIIS_UPDATE " + str(SPEC)


def test_spec_counts():
    spec = json.loads(SPEC.read_text())
    assert sum(len(t["fields"]) for t in spec["transformations"]) == 874
    assert len([t for t in spec["transformations"] if t["type"] == "Lookup Procedure"]) == 9
    assert {t["name"] for t in spec["targets"]} == {"NWK_ACTION_PRIMARY_TBL", "NWK_ACTION_SECONDARY_TBL", "EHRP_RECS_TRACKING_TBL"}


def test_every_expression_port_has_a_catalog_entry():
    from pipelines.ehrp2biis.expressions import CATALOG

    spec = json.loads(SPEC.read_text())
    missing = [
        f"{t['name']}.{f['name']}"
        for t in spec["transformations"] if t["type"] == "Expression"
        for f in t["fields"] if f["porttype"] in ("LOCAL VARIABLE", "OUTPUT") and f["name"] not in CATALOG
    ]
    assert not missing


def test_every_target_column_source_port_exists():
    spec = json.loads(SPEC.read_text())
    ports = {(t["name"], f["name"]) for t in spec["transformations"] for f in t["fields"]}
    for table, cols in spec["target_lineage"].items():
        for col, srcs in cols.items():
            assert len(srcs) == 1, (table, col)
            assert tuple(srcs[0].split(".", 1)) in ports, (table, col, srcs)


def test_mapping_spec_markdown_is_current():
    import subprocess
    import sys

    md = ROOT / "docs" / "migration" / "EHRP2BIIS_UPDATE_mapping_spec.md"
    before = md.read_text()
    subprocess.run([sys.executable, str(ROOT / "tools" / "render_mapping_spec.py")], check=True, capture_output=True)
    assert md.read_text() == before


def test_afterload_order_matches_legacy_sql():
    legacy = (ROOT / "ehrp2biis_afterload.sql").read_text()
    dag_src = (ROOT / "airflow" / "dags" / "ehrp2biis_pipeline.py").read_text()
    block = dag_src[dag_src.index("AFTERLOAD = ["):dag_src.index("]\n\ndefault_args")]
    targets = re.findall(r'"(?:proc|sql|sql_ledger)", "([^"]+)"', block)
    procs = [t for t in targets if not t.endswith(".sql")]
    positions = [legacy.lower().index(f"exec {p.lower()}") if f"exec {p.lower()}" in legacy.lower()
                 else legacy.lower().index(f"execute {p.lower()}") for p in procs]
    assert positions == sorted(positions), procs
    assert "HISTDBA.GATHER_EHRP2BIIS_RUNCOUNTS_P" in procs
    # P_STARTDT reset happens after UPDT_ORIG_CANCELLED_TRANS01_P and before chk_ehrp2biis_wip_status_p
    assert targets.index("afterload/04_process_table_p_startdt.sql") == targets.index("HISTDBA.UPDT_ORIG_CANCELLED_TRANS01_P") + 1
    assert targets.index("chk_ehrp2biis_wip_status_p") == targets.index("afterload/04_process_table_p_startdt.sql") + 1


def test_repo_sql_files_split_cleanly():
    from ehrp2biis_lib.oracle_tasks import split_statements

    files = sorted((ROOT / "airflow" / "sql").rglob("*.sql"))
    assert files
    for f in files:
        stmts = split_statements(f.read_text())
        assert stmts, f
        for s in stmts:
            if "ddl" not in f.parts:  # DDL may use SYSDATE as an audit default
                assert "sysdate" not in s.lower() or "TO_DATE(:load_date" in s, f"{f}: use :load_date, not SYSDATE"
            assert not s.rstrip().endswith(";"), f"{f}: statements end with a '/' line, not ';'"


# sha256 of the external-contract scripts at the time of the pilot. These files must not change.
CONTRACT_FILES = {
    "Transfer Scripts/afps_transfer": "ee639735408fd23374c9501adb60861c36c6db025af9706a7ef686abd8e6054c",
    "Transfer Scripts/cdc_transfer": "40d03e3354b076ea6f6003852cffc1254f0c72aa617946cd3b08e1580556c489",
    "Transfer Scripts/fda_transfer": "9231df6986ccc58c0d7d9ae96b9462affe24039fc69aecf3d8397eae6f9ffe63",
    "Transfer Scripts/nih_cpm_transfer": "0dc79dc7e92436c81cbffabfe23291f3e2be869a03f2711861e3ebba04ce6144",
    "Transfer Scripts/nih_les_transfer": "c4253f0de2bb7760849d6116f1d2c07cb88df3cb9f32f620e76e50b22a99e058",
    "Transfer Scripts/nih_transfer_les": "6d49c0e4e0e2a5ec85c2137fae59fc1f4daec47f2173eb3bdef20efaddca9b6d",
    "Transfer Scripts/oig_transfer": "aeb21eae4dd947a886c26bbf6891729b27f724ec3dbc7ad38e479b8dcbdb04c7",
    "Maintenance Scripts/archive_files": "3127065322b386b7b950e276cd47bc895a6625a9e1f8cace77e98e900ec2ccc7",
    "Maintenance Scripts/remove_file": "ed6334facaf26b3ccbdb8ed2958d905e55168937e01f66a5b20802abf18d4d1f",
}


def test_transfer_and_maintenance_scripts_unchanged():
    for rel, digest in CONTRACT_FILES.items():
        assert hashlib.sha256((ROOT / rel).read_bytes()).hexdigest() == digest, f"{rel} is an external contract"
