"""Module 19 - Deadline Alerts (Workflow Orchestrator, Airflow 3.2.1).

Deadline Alerts replace SLAs (removed in Airflow 3). This version uses a
SyncCallback (new in 3.2): the callback runs on a worker, like a task, so it
can be a plain Python function from the repo - no Slack, no connection,
no custom image. The function lives in dags/training_callbacks.py.

The run is given 2 minutes; the task takes 4. After 2 minutes the callback
fires, and the task still finishes: a deadline alerts, it doesn't stop the run.
"""
import re
import sys
from pathlib import Path
from datetime import timedelta

from airflow.sdk import DAG, DeadlineAlert, DeadlineReference, SyncCallback
from airflow.providers.standard.operators.bash import BashOperator

# The callback lives in dags/training_callbacks.py (shared by everyone).
# Put the dags/ root on the import path so "training_callbacks" can be found.
DAGS_ROOT = str(Path(__file__).resolve().parents[2])
if DAGS_ROOT not in sys.path:
    sys.path.insert(0, DAGS_ROOT)

from training_callbacks import log_deadline_miss  # noqa: E402

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())

with DAG(
    dag_id=f"deadline_alert_example_{STUDENT}",
    schedule=None,
    catchup=False,
    tags=["training", "deadline-alerts"],
    deadline=DeadlineAlert(
        # Measure from when the run was queued...
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        # ...and alert if it hasn't finished 2 minutes later.
        interval=timedelta(minutes=2),
        callback=SyncCallback(log_deadline_miss),
    ),
):
    # Deliberately takes 4 minutes, so it misses the 2-minute deadline.
    BashOperator(task_id="long_running_task", bash_command="sleep 240")
