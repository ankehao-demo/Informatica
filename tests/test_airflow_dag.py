"""DAG structure test -- run with the Airflow venv (skipped when Airflow is absent)."""
import itertools

import pytest

pytest.importorskip("airflow")


def test_dag_imports_and_order():
    from pathlib import Path

    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(Path(__file__).resolve().parents[1] / "airflow" / "dags"), include_examples=False)
    assert not bag.import_errors, bag.import_errors
    dag = bag.get_dag("ehrp2biis_update")
    order = ["preload_step01", "actstage_load", "databricks_ehrp2biis_update", "publish_slice_to_oracle"]
    for a, b in itertools.pairwise(order):
        assert b in dag.get_task(a).downstream_task_ids
    assert "afterload.retained_step_cd" in dag.get_task("publish_slice_to_oracle").downstream_task_ids
    procs = [t for t in dag.topological_sort() if t.task_id.startswith("afterload.")]
    ids = [t.task_id.split(".", 1)[1] for t in procs]
    assert ids.index("UPDT_ERP2BIIS_CRE8_REMARKS01_P") < ids.index("UPDATE_ERP2BIIS_NO900S01_P") < ids.index(
        "ERP2BIIS_CRE8_REMARKS_900S01") < ids.index("UPDATE_ERP2BIIS_900SONLY01_P") < ids.index(
        "UPDT_ORIG_CANCELLED_TRANS01_P") < ids.index("process_table_p_startdt") < ids.index("truncate_new_actions")
    assert dag.max_active_runs == 1 and dag.on_success_callback and dag.default_args["on_failure_callback"]
    assert "distribution.distribution_done" in dag.task_ids
