"""Oracle task helpers for the EHRP2BIIS DAG.

Credentials come from Airflow connections (backed by the configured secrets backend),
never from ~/.use, ~/.pw, ~/.use1, ~/.pw1:

    oracle_biis_nknight   replaces $HOME/.use  / $HOME/.pw   (loin / ps1)
    oracle_biis_load      replaces $HOME/.use1 / $HOME/.pw1  (loin2 / ps2)
"""
from __future__ import annotations

import logging
import os
import re
import subprocess
import tempfile
from pathlib import Path

log = logging.getLogger(__name__)

LEDGER = "NKNIGHT.EHRP2BIIS_RUN_LEDGER"
# Legacy scripts grep the spool for "ERROR"; we also fail on ORA-/SP2-/PLS- codes.
ERROR_PATTERN = re.compile(r"\b(ERROR|ORA-\d{5}|SP2-\d{4}|PLS-\d{5})", re.IGNORECASE)


def split_statements(sql: str) -> list[str]:
    """Split a repo SQL file on lines containing only '/' (SQL*Plus block terminator)."""
    out, buf = [], []
    for line in sql.splitlines():
        if line.strip() == "/":
            stmt = "\n".join(buf).strip()
            if stmt:
                out.append(stmt)
            buf = []
        elif not line.strip().startswith("--"):
            buf.append(line)
    if "\n".join(buf).strip():
        raise ValueError("SQL file must terminate every statement with a line containing '/'")
    return out


def _binds_for(stmt: str, binds: dict) -> dict:
    return {k: v for k, v in binds.items() if f":{k}" in stmt}


def ledger_done(cursor, load_id: str, step: str) -> bool:
    cursor.execute(f"SELECT 1 FROM {LEDGER} WHERE LOAD_ID = :l AND STEP_NAME = :s", l=load_id, s=step)
    return cursor.fetchone() is not None


def run_sql_file(conn_id: str, path: str, load_id: str, load_date: str, ledger_step: str | None = None, run_id: str = "") -> str:
    """Run a repo SQL file in ONE transaction. With ``ledger_step`` the step is skipped
    if already recorded for this LOAD_ID, and the ledger row commits atomically with it."""
    from airflow.providers.oracle.hooks.oracle import OracleHook

    stmts = split_statements(Path(path).read_text())
    binds = {"load_id": load_id, "load_date": load_date}
    conn = OracleHook(oracle_conn_id=conn_id).get_conn()
    try:
        cur = conn.cursor()
        if ledger_step and ledger_done(cur, load_id, ledger_step):
            log.info("%s already completed for %s -- skipping (idempotent rerun)", ledger_step, load_id)
            return "skipped"
        for stmt in stmts:
            if stmt.lower().startswith("truncate"):
                cur.execute(stmt)  # DDL: implicit commit, idempotent by nature
                continue
            log.info("executing:\n%s", stmt)
            cur.execute(stmt, _binds_for(stmt, binds))
            log.info("%s rows", cur.rowcount)
        if ledger_step:
            cur.execute(f"INSERT INTO {LEDGER} (LOAD_ID, STEP_NAME, AIRFLOW_RUN) VALUES (:l, :s, :r)", l=load_id, s=ledger_step, r=run_id[:250])
        conn.commit()
        return "done"
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def call_procedure(conn_id: str, proc: str, load_id: str, args: list | None = None, compile_first: bool = False, run_id: str = "") -> str:
    """EXEC a black-box stored procedure once per LOAD_ID (run-ledger guarded).

    The HISTDBA procedures are not ported -- they run exactly as the legacy
    ehrp2biis_afterload.sql ran them. Procedures may COMMIT internally, so the ledger
    row is written right after the call returns successfully.
    """
    from airflow.providers.oracle.hooks.oracle import OracleHook

    conn = OracleHook(oracle_conn_id=conn_id).get_conn()
    try:
        cur = conn.cursor()
        if ledger_done(cur, load_id, proc):
            log.info("%s already completed for %s -- skipping", proc, load_id)
            return "skipped"
        if compile_first:
            cur.execute(f"ALTER PROCEDURE {proc} COMPILE")
        cur.callproc("dbms_output.enable", [1000000])
        cur.callproc(proc, args or [])
        _drain_dbms_output(cur)
        conn.commit()
        cur.execute(f"INSERT INTO {LEDGER} (LOAD_ID, STEP_NAME, AIRFLOW_RUN) VALUES (:l, :s, :r)", l=load_id, s=proc, r=run_id[:250])
        conn.commit()
        return "done"
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _drain_dbms_output(cur) -> None:
    line, status = cur.var(str), cur.var(int)
    while True:
        cur.callproc("dbms_output.get_line", (line, status))
        if status.getvalue() != 0:
            break
        log.info("dbms_output: %s", line.getvalue())


def run_sqlplus_script(conn_id: str, script: str, success_marker: str | None = None, fail_on_error_text: bool = True) -> None:
    """Run an existing SQL*Plus script (step01.sql, action_stage_load.sql) unchanged.

    The connect string is fed on stdin, so the password never appears in argv, the
    rendered template or the task log. Fails on non-zero exit, on ERROR/ORA-/SP2-
    text in the spool (legacy `grep -i ERROR`), or when ``success_marker`` (legacy
    `grep -c "SUCCESS!!!"`) is missing.
    """
    from airflow.exceptions import AirflowException
    from airflow.hooks.base import BaseHook

    c = BaseHook.get_connection(conn_id)
    dsn = c.extra_dejson.get("dsn") or f"{c.host}:{c.port or 1521}/{c.extra_dejson.get('service_name', c.schema)}"
    sqlplus = os.path.join(os.environ.get("ORACLE_HOME", "/usr"), "bin", "sqlplus")
    with tempfile.NamedTemporaryFile("w", suffix=".log", delete=False) as spool:
        spool_path = spool.name
    stdin = (
        "WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK\n"
        "WHENEVER OSERROR EXIT FAILURE ROLLBACK\n"
        f"connect {c.login}/\"{c.password}\"@{dsn}\n"
        f"spool {spool_path}\nshow user\nset serveroutput on size 1000000\n"
        f"@{script}\nspool off\nEXIT\n"
    )
    proc = subprocess.run([sqlplus, "-s", "/nolog"], input=stdin, text=True, capture_output=True, check=False)
    output = Path(spool_path).read_text(errors="replace") if os.path.exists(spool_path) else ""
    log.info("sqlplus output:\n%s", output or proc.stdout)
    if proc.returncode != 0:
        raise AirflowException(f"sqlplus {script} exited with {proc.returncode}")
    if fail_on_error_text and ERROR_PATTERN.search(output):
        raise AirflowException(f"sqlplus {script}: error text in spool")
    if success_marker and success_marker not in output:
        raise AirflowException(f"sqlplus {script}: success marker {success_marker!r} not found")
