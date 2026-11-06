"""Module 20 - Time sensor (Workflow Orchestrator, Airflow 3.2.1).

Waits until a point in time, then continues, without holding a worker:
deferrable=True hands the wait to the triggerer.

The target is "2 minutes after this sensor starts", worked out by Jinja when
the task runs, so there is nothing to edit and the wait is always about
2 minutes. (TimeSensor does the same for a fixed time of day, e.g. 06:00.)
"""
import re
from pathlib import Path
from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.date_time import DateTimeSensor

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())

with DAG(
    dag_id=f"time_sensor_example_{STUDENT}",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training", "sensors"],
) as dag:

    wait_two_minutes = DateTimeSensor(
        task_id="wait_until_target_time",
        # Rendered when the task starts: its own start time + 2 minutes.
        target_time="{{ (ti.start_date + macros.timedelta(minutes=2)).isoformat() }}",
        deferrable=True,          # frees the worker slot while waiting
        timeout=60 * 30,          # give up after 30 minutes
    )

    run_after = EmptyOperator(task_id="run_after")

    wait_two_minutes >> run_after
