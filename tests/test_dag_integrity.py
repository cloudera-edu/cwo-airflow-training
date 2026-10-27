"""Checks that every DAG in the repo parses on Airflow 3.2.1 and that DAG IDs don't clash.

Run locally:  pytest tests/  (see the CI workflow for the install command)
"""
import os
from pathlib import Path

import pytest
from airflow.models.dagbag import DagBag

ROOT = Path(__file__).resolve().parents[1]
FOLDERS = ["dags", "solutions", "custom-image/dags"]
DISABLED = {  # Workflow Orchestrator's default disabled-operator list
    "KubernetesPodOperator", "KubernetesJobOperator", "KubernetesDeleteJobOperator",
    "KubernetesPatchJobOperator", "KubernetesInstallKueueOperator", "KubernetesStartKueueJobOperator",
    "PythonVirtualenvOperator", "BranchPythonVirtualenvOperator",
}
# Lab files that use a disabled operator on purpose
EXPECTED_DISABLED = {"debug_disabled_operator_broken.py"}


@pytest.mark.parametrize("folder", FOLDERS)
def test_no_import_errors(folder):
    bag = DagBag(dag_folder=str(ROOT / folder), include_examples=False)
    assert not bag.import_errors, "\n".join(f"{k}: {v}" for k, v in bag.import_errors.items())
    assert bag.dags, f"no DAGs found in {folder}"


def test_disabled_operators_only_where_intended():
    bag = DagBag(dag_folder=str(ROOT / "dags"), include_examples=False)
    for dag in bag.dags.values():
        for t in dag.tasks:
            if type(t).__name__ in DISABLED:
                assert Path(dag.fileloc).name in EXPECTED_DISABLED, (
                    f"{dag.dag_id}.{t.task_id} uses disabled operator {type(t).__name__}"
                )


def test_dagrun_timeout_for_waiting_dags():
    """DAGs that defer or wait for people must set dagrun_timeout (platform default is 1 hour)."""
    bag = DagBag(dag_folder=str(ROOT / "dags"), include_examples=False)
    waiting = ("hitl_backfill_example", "time_sensor_example", "deferrable_s3_file_sensor_example")
    for dag_id, dag in bag.dags.items():
        if dag_id.startswith(waiting):
            assert dag.dagrun_timeout is not None, f"{dag_id} needs dagrun_timeout"
