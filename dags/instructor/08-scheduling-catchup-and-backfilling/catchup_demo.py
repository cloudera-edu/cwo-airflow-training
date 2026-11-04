"""Module 08 - Scheduling and catchup behaviour (Workflow Orchestrator, Airflow 3.2.1).

Two DAGs with identical settings except catchup:
  - catchup_demo_<student>     catchup=True  -> one run per missed 5-minute slot since START
  - no_catchup_demo_<student>  catchup=False -> only the latest slot runs

START is computed so the lab works on any day without editing:
it is 30 minutes before the top of the current hour (UTC).

Airflow 3 change: catchup now defaults to False (it was True in Airflow 2),
so set it explicitly. Part C of the lab starts a backfill from the UI
(Trigger > Backfill), which the scheduler runs.
"""
import re
from pathlib import Path
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())

# 30 minutes before the top of the current hour, in UTC.
# Example: at 10:20 UTC, START is 09:30 UTC, so 10 slots have been missed.
START = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) - timedelta(minutes=30)


def process_slot(**context):
    print(f"run_id:       {context['run_id']}")
    print(f"logical date: {context['logical_date']}")


for dag_id, catchup in ((f"catchup_demo_{STUDENT}", True), (f"no_catchup_demo_{STUDENT}", False)):
    with DAG(
        dag_id=dag_id,
        start_date=START,
        schedule="*/5 * * * *",   # every 5 minutes
        catchup=catchup,
        max_active_runs=3,        # keep the catchup from flooding the shared pool
        tags=["training", "scheduling"],
    ):
        PythonOperator(task_id="process_slot", python_callable=process_slot)
