# EHRP2BIIS pilot: Informatica PowerCenter -> Databricks + Airflow

Pilot port of `wf_EHRP2BIIS_UPDATE` / `m_EHRP2BIIS_UPDATE` (`XML/EHRP2BIIS_UPDATE`).

| Artifact | Path |
|---|---|
| Mapping spec (every TRANSFORMFIELD, lookup, writer attribute) | [`EHRP2BIIS_UPDATE_mapping_spec.md`](EHRP2BIIS_UPDATE_mapping_spec.md) |
| Assumptions / ambiguous semantics | [`ASSUMPTIONS.md`](ASSUMPTIONS.md) |
| XML -> JSON parser / spec renderer | `tools/infa_xml_spec.py`, `tools/render_mapping_spec.py` |
| PySpark transformation (Delta, MERGE) | `pipelines/ehrp2biis/` |
| Reconciliation harness | `pipelines/reconcile/reconcile.py` |
| Airflow DAG + Oracle SQL | `airflow/dags/ehrp2biis_pipeline.py`, `airflow/sql/` |
| Databricks resources | `main.tf` |
| Tests | `tests/` |

## Architecture

```
Airflow DAG ehrp2biis_update (daily 12:00, max_active_runs=1, LOAD_ID = NK{{ds_nodash}})
 1 preload_step01        sqlplus step01.sql          (was ehrp2biis_preload; conn oracle_biis_load)
 2 actstage_load         sqlplus action_stage_load   (was actstage_load; success marker SUCCESS!!!)
 3 databricks job ehrp2biis-update-pilot (main.tf)
     extract   Oracle -> bronze Delta snapshot per LOAD_ID   (secrets: scope "ehrp2biis")
     transform bronze -> m_EHRP2BIIS_UPDATE in PySpark -> silver Delta MERGE + rejects
     publish   silver slice -> NKNIGHT.STG_* (JDBC)
 4 publish_slice_to_oracle   STG_* -> NKNIGHT.NWK_* (delete+insert of the slice, one transaction)
 5 afterload (TaskGroup, same order as ehrp2biis_afterload.sql)
     retained_step_cd -> update_sequence_number_tbl_p -> HISTDBA.UPDT_ERP2BIIS_CRE8_REMARKS01_P
     -> HISTDBA.UPDATE_ERP2BIIS_NO900S01_p -> HISTDBA.ERP2BIIS_CRE8_REMARKS_900s01
     -> HISTDBA.UPDATE_ERP2BIIS_900SONLY01_P -> HISTDBA.UPDT_ORIG_CANCELLED_TRANS01_P
     -> PROCESS_TABLE P_STARTDT reset/recompute -> chk_ehrp2biis_wip_status_p
     -> *_ALL copy -> HISTDBA.GATHER_EHRP2BIIS_RUNCOUNTS_P(NULL) -> cancelled actions -> truncate driver table
 6 distribution          Transfer Scripts/* unchanged, via SSH (config: airflow/config/ehrp2biis_distribution.yaml)
```

Why both orchestrators: Airflow owns the end-to-end chain because most steps run against Oracle and the BIIS
host; the Spark work is one Databricks Workflows job (3 tasks, one job cluster) managed by Terraform.

## Legacy -> new mapping of operational concerns

| Legacy | New |
|---|---|
| `cat $HOME/.use / .pw / .use1 / .pw1` plaintext credentials | Airflow connections `oracle_biis_nknight`, `oracle_biis_load`, `ssh_biisint`, `databricks_default` (resolve through the configured secrets backend, e.g. Vault/AWS Secrets Manager); Databricks secret scope `ehrp2biis` for JDBC |
| `grep -i ERROR $logfile` + `mailx` | sqlplus `WHENEVER SQLERROR EXIT`, non-zero exit/ORA-/SP2- text/missing success marker fail the task; `on_failure_callback` (email + optional Slack) per task, `on_success_callback` per run |
| Informatica scheduler (daily 12:00) | DAG `schedule="0 12 * * *"`, `catchup=False` |
| `SYSDATE` | logical date `{{ ds }}`; `LOAD_ID = 'NK'||YYYYMMDD`, `LOAD_DATE = trunc(ds)` |
| insert-only target load | Delta MERGE per load slice; Oracle slice delete+insert; run ledger for black-box procs |
| `.bad` reject files | `EHRP2BIIS_REJECTS` Delta table (`--max-rejects` gate optional) |

## Idempotency per step

| Step | Rerun behaviour |
|---|---|
| preload / actstage | legacy scripts, run as before (they rebuild the driver table for the day) |
| extract | `replaceWhere LOAD_ID = ...` on bronze |
| transform | sequence snapshot pinned per LOAD_ID; MERGE + delete of stale rows in the slice |
| publish | staging tables truncated and reloaded |
| publish_slice_to_oracle | delete+insert of the LOAD_ID slice in one transaction |
| afterload SQL | deterministic UPDATEs / delete+insert; `*_ALL` copy guarded by the run ledger (atomic) |
| afterload procs | run ledger `NKNIGHT.EHRP2BIIS_RUN_LEDGER`, `retries=0` |
| distribution | `sftp put` of the same file name overwrites |

## Running locally

```bash
python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements-pipeline.txt
# (if Maven Central rate-limits the Delta jar download)
export SPARK_JARS_REPOSITORIES=https://maven-central.storage-download.googleapis.com/maven2/
export PYTHONPATH=.
python -m pipelines.ehrp2biis.job sample    --run-date 2026-03-12 --bronze /tmp/ehrp/bronze --silver /tmp/ehrp/silver
python -m pipelines.ehrp2biis.job transform --run-date 2026-03-12 --bronze /tmp/ehrp/bronze --silver /tmp/ehrp/silver
python -m pipelines.reconcile.reconcile --run-date 2026-03-12 --legacy <legacy extract> --new /tmp/ehrp/silver --report-dir out/
pytest tests                                   # Spark tests need Java 11+
# DAG test (separate venv with airflow/requirements.txt):
AIRFLOW_HOME=/tmp/af airflow db migrate && pytest tests/test_airflow_dag.py
```

## Dual-run procedure (before cut-over)

1. Keep the Informatica workflow as the system of record. Run the DAG with `transfers: []` and point
   `publish_slice_to_oracle`/afterload at a shadow schema (or disable them) so BIIS is not written twice.
2. The `extract` task snapshots exactly the batch Informatica consumed (`NWK_NEW_EHRP_ACTIONS_TBL` + PS rows) in bronze.
3. After the legacy load, run the `ehrp2biis-reconcile-dual-run` Databricks job (or the CLI) for the same `run_date`.
   It reads the legacy `NKNIGHT.NWK_*` tables and the silver tables and writes `reconcile_NK<date>.md/.json`
   with row counts, only-in-legacy/only-in-new keys, duplicate keys and per-column mismatch counts with samples.
4. Cut over when consecutive pay periods reconcile clean (resolve every **verify in dual-run** item in ASSUMPTIONS.md).

## Out of scope (flagged)

* Porting `HISTDBA.*`, `update_sequence_number_tbl_p`, `chk_ehrp2biis_wip_status_p` (separate effort).
* `step01.sql` / `action_stage_load.sql` are not in the repo; they are executed unchanged.
* The remaining Informatica workflows under `XML/` (CPM*, LES, COMPTIME, FDA_Leave, Pay_Calendar, rest of Pseudossn).
