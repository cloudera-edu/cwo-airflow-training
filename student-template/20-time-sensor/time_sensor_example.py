"""Module 20 - TimeSensor (Workflow Orchestrator, Airflow 3.2.1).

Waits until a time of day, then continues. deferrable=True is now on:
Workflow Orchestrator has a triggerer, so the wait doesn't hold a worker slot.
dagrun_timeout is set because the platform default (1 hour) would end a
longer wait.
"""
import re
from pathlib import Path
from datetime import datetime, time, timedelta, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.time import TimeSensor

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())

# TODO: set to about 10 minutes from now. Example: 10:30 IST (UTC+05:30).
IST = timezone(timedelta(hours=5, minutes=30))
TARGET_TIME = time(10, 30, tzinfo=IST)

with DAG(
    dag_id=f"time_sensor_example_{STUDENT}",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(hours=4),
    tags=["training", "sensors"],
) as dag:

    wait_until_target = TimeSensor(
        task_id="wait_until_target_time",
        target_time=TARGET_TIME,
        deferrable=True,          # frees the worker slot while waiting
        timeout=60 * 60 * 3,      # give up after 3 hours
    )

    run_after = EmptyOperator(task_id="run_after")

    wait_until_target >> run_after
