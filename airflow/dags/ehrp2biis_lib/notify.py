"""Failure / success notifications replacing `mailx` on `grep ERROR`.

Recipients and channels are configuration, not code:
    Variable ehrp2biis_alert_emails   comma-separated list (optional)
    Connection ehrp2biis_slack        Slack incoming-webhook connection (optional)
"""
from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def _recipients() -> list[str]:
    from airflow.models import Variable

    raw = Variable.get("ehrp2biis_alert_emails", default_var="")
    return [r.strip() for r in raw.split(",") if r.strip()]


def _send(subject: str, body: str) -> None:
    to = _recipients()
    if to:
        from airflow.utils.email import send_email

        send_email(to=to, subject=subject, html_content=body.replace("\n", "<br>"))
    try:
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

        SlackWebhookHook(slack_webhook_conn_id="ehrp2biis_slack").send(text=f"*{subject}*\n{body}")
    except Exception as exc:  # noqa: BLE001 - Slack is optional
        log.info("slack notification skipped: %s", exc)


def on_failure(context) -> None:
    ti = context["task_instance"]
    _send(
        f"[EHRP2BIIS] FAILED {ti.task_id} for {context['ds']}",
        f"DAG {ti.dag_id} run {context['run_id']}\nTask {ti.task_id} try {ti.try_number}\n"
        f"Error: {context.get('exception')}\nLog: {ti.log_url}",
    )


def on_success(context) -> None:
    dag_run = context["dag_run"]
    _send(
        f"[EHRP2BIIS] completed for {context['ds']} (LOAD_ID NK{context['ds_nodash']})",
        f"DAG {dag_run.dag_id} run {dag_run.run_id} finished successfully.",
    )
