"""EHRP2BIIS daily load -- modernized orchestration of wf_EHRP2BIIS_UPDATE.

Legacy chain (cron + KornShell + Informatica):
    ehrp2biis_preload (step01.sql) -> actstage_load (action_stage_load.sql)
    -> wf_EHRP2BIIS_UPDATE (m_EHRP2BIIS_UPDATE) -> ehrp2biis_afterload.sql
    -> Transfer Scripts/*  (SFTP to agency jail dirs)

New chain (this DAG):
    preload -> actstage -> databricks[extract -> transform -> publish]
    -> publish_slice (Oracle) -> afterload (black-box HISTDBA procs, same order)
    -> distribution (Transfer Scripts invoked unchanged)

Every task is keyed by LOAD_ID = 'NK' || {{ ds_nodash }} and is safe to clear/rerun.
"""
from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import pendulum
import yaml
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.task_group import TaskGroup
from ehrp2biis_lib import notify, oracle_tasks

from airflow import DAG

REPO_ROOT = Path(os.environ.get("EHRP2BIIS_REPO_ROOT", Path(__file__).resolve().parents[2]))
SQL_DIR = REPO_ROOT / "airflow" / "sql"
DIST = yaml.safe_load((REPO_ROOT / "airflow" / "config" / "ehrp2biis_distribution.yaml").read_text())
LEGACY_SQL_HOME = os.environ.get("EHRP2BIIS_LEGACY_SQL_HOME", "/home/sa-biisint/ehrp2biis")  # step01.sql etc.

ORA_LOAD = "oracle_biis_load"      # was $HOME/.use1 + $HOME/.pw1
ORA_NKNIGHT = "oracle_biis_nknight"  # was $HOME/.use + $HOME/.pw
LOAD_ID = "NK{{ ds_nodash }}"
LOAD_DATE = "{{ ds }}"
RUN_ID = "{{ run_id }}"

# Same order as ehrp2biis_afterload.sql. Each entry: (task_id, kind, target, kwargs)
AFTERLOAD = [
    ("retained_step_cd", "sql", "afterload/01_retained_step.sql", {}),
    ("update_sequence_number_tbl_p", "proc", "update_sequence_number_tbl_p", {"compile_first": True}),
    ("UPDT_ERP2BIIS_CRE8_REMARKS01_P", "proc", "HISTDBA.UPDT_ERP2BIIS_CRE8_REMARKS01_P", {}),
    ("UPDATE_ERP2BIIS_NO900S01_P", "proc", "HISTDBA.UPDATE_ERP2BIIS_NO900S01_p", {}),
    ("ERP2BIIS_CRE8_REMARKS_900S01", "proc", "HISTDBA.ERP2BIIS_CRE8_REMARKS_900s01", {}),
    ("UPDATE_ERP2BIIS_900SONLY01_P", "proc", "HISTDBA.UPDATE_ERP2BIIS_900SONLY01_P", {}),
    ("UPDT_ORIG_CANCELLED_TRANS01_P", "proc", "HISTDBA.UPDT_ORIG_CANCELLED_TRANS01_P", {}),
    ("process_table_p_startdt", "sql", "afterload/04_process_table_p_startdt.sql", {}),
    ("chk_ehrp2biis_wip_status_p", "proc", "chk_ehrp2biis_wip_status_p", {"compile_first": True}),
    ("publish_all_tables", "sql_ledger", "afterload/06_publish_all_tables.sql", {}),
    ("GATHER_EHRP2BIIS_RUNCOUNTS_P", "proc", "HISTDBA.GATHER_EHRP2BIIS_RUNCOUNTS_P", {"args": [None]}),
    ("cancelled_actions", "sql", "afterload/08_cancelled_actions.sql", {}),
    ("truncate_new_actions", "sql", "afterload/09_truncate_new_actions.sql", {}),
]

default_args = {
    "owner": "biis",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": notify.on_failure,
    "email_on_failure": False,  # notify.on_failure handles email + Slack
}

with DAG(
    dag_id="ehrp2biis_update",
    description="EHRP -> BIIS personnel action load (pilot port of wf_EHRP2BIIS_UPDATE)",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/New_York"),
    schedule="0 12 * * *",  # Informatica scheduler: daily, 12:00 (SCHEDULEINFO in XML/EHRP2BIIS_UPDATE)
    catchup=False,
    max_active_runs=1,  # the Oracle work tables are single-batch, like the legacy flow
    default_args=default_args,
    on_success_callback=notify.on_success,
    params={"pay_period": Param("", type="string", description="pay period for distribution file names")},
    tags=["ehrp2biis", "informatica-migration", "pilot"],
) as dag:
    preload = PythonOperator(
        task_id="preload_step01",
        python_callable=oracle_tasks.run_sqlplus_script,
        op_kwargs={"conn_id": ORA_LOAD, "script": f"{LEGACY_SQL_HOME}/step01"},
        queue="oracle_client",
    )
    actstage = PythonOperator(
        task_id="actstage_load",
        python_callable=oracle_tasks.run_sqlplus_script,
        op_kwargs={"conn_id": ORA_LOAD, "script": f"{LEGACY_SQL_HOME}/action_stage_load", "success_marker": "SUCCESS!!!"},
        queue="oracle_client",
    )
    databricks = DatabricksRunNowOperator(
        task_id="databricks_ehrp2biis_update",
        databricks_conn_id="databricks_default",
        job_name="ehrp2biis-update-pilot",  # created by main.tf
        json={"job_parameters": {"run_date": LOAD_DATE}},  # Jobs API 2.1 run-now job_parameters
        wait_for_termination=True,
    )
    publish_slice = PythonOperator(
        task_id="publish_slice_to_oracle",
        python_callable=oracle_tasks.run_sql_file,
        op_kwargs={"conn_id": ORA_NKNIGHT, "path": str(SQL_DIR / "publish_slice.sql"), "load_id": LOAD_ID, "load_date": LOAD_DATE},
    )

    with TaskGroup("afterload", tooltip="ehrp2biis_afterload.sql, same order; HISTDBA procs are black boxes") as afterload:
        prev = None
        for task_id, kind, target, kw in AFTERLOAD:
            if kind == "proc":
                t = PythonOperator(
                    task_id=task_id,
                    python_callable=oracle_tasks.call_procedure,
                    op_kwargs={"conn_id": ORA_NKNIGHT, "proc": target, "load_id": LOAD_ID, "run_id": RUN_ID, **kw},
                    retries=0,  # never auto-retry a black-box proc; ledger makes manual clears safe
                )
            else:
                t = PythonOperator(
                    task_id=task_id,
                    python_callable=oracle_tasks.run_sql_file,
                    op_kwargs={
                        "conn_id": ORA_NKNIGHT, "path": str(SQL_DIR / target), "load_id": LOAD_ID, "load_date": LOAD_DATE,
                        "run_id": RUN_ID, "ledger_step": task_id if kind == "sql_ledger" else None,
                    },
                )
            if prev:
                prev >> t
            prev = t

    with TaskGroup("distribution", tooltip="Transfer Scripts/ invoked unchanged on the BIIS host") as distribution:
        done = EmptyOperator(task_id="distribution_done", trigger_rule="none_failed")
        for item in DIST.get("transfers") or []:
            script = f"{DIST['scripts_dir']}/{item['script']}"
            SSHOperator(
                task_id=f"sftp_{item['agency'].lower()}_{item['script']}",
                ssh_conn_id="ssh_biisint",
                command=f'"{script}" "{item["file"]}"',  # re-sending the same file name is idempotent (sftp put overwrites)
                cmd_timeout=1800,
            ) >> done
        for i, arc in enumerate(DIST.get("archive") or []):
            done >> SSHOperator(
                task_id=f"archive_files_{i}",
                ssh_conn_id="ssh_biisint",
                command=f'"{DIST["maintenance_dir"]}/archive_files" "{arc["inputdir"]}" "{arc["destination"]}" "{{{{ params.pay_period }}}}"',
            )

    preload >> actstage >> databricks >> publish_slice >> afterload >> distribution
