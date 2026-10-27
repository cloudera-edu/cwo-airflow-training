"""Module 08 - Scheduling and catchup behaviour (Workflow Orchestrator, Airflow 3.2.1).

Two DAGs with identical settings except catchup:
  - catchup_demo_<student>     catchup=True  -> one run per missed day since start_date
  - no_catchup_demo_<student>  catchup=False -> only the latest interval runs

Airflow 3 change: catchup now defaults to False (it was True in Airflow 2),
so set it explicitly. Part C of the lab starts a backfill from the UI
(DAG > Trigger > Backfill), which the scheduler runs - there is no
`airflow dags backfill` CLI on Workflow Orchestrator.
"""
import re
from pathlib import Path
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())
START = datetime(2026, 10, 20)  # TODO: set to 7 days before today


def process_day(**context):
    print(f"processing logical date: {context['ds']}")


for dag_id, catchup in ((f"catchup_demo_{STUDENT}", True), (f"no_catchup_demo_{STUDENT}", False)):
    with DAG(
        dag_id=dag_id,
        start_date=START,
        schedule="@daily",
        catchup=catchup,
        max_active_runs=2,   # keep a catchup from flooding the shared pool
        tags=["training", "scheduling"],
    ):
        PythonOperator(task_id="process_day", python_callable=process_day)
