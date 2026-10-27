"""Module 17, part 2 - Pull-based watchers (Workflow Orchestrator, Airflow 3.2.1, CUSTOM IMAGE ONLY).

Copy this file into the dags/ folder only on an instance running the custom
image in custom-image/ - on the standard image the import below fails.

An AssetWatcher attaches a trigger to the Asset itself. The triggerer polls
Object Store; when the flag file appears, the asset is updated and the DAG
runs, with no Airflow task producing the event. The trigger deletes the flag,
so the next upload fires again.

Changes from the earlier version: the custom HDFS trigger (which needed the
hdfs CLI on the triggerer) is replaced by an Object Store trigger installed in
the image, and you check the result in the UI (Assets, and the DAG's runs)
instead of querying the metadata database.
"""
import re
from pathlib import Path
from datetime import datetime

from airflow.sdk import DAG, Asset, AssetWatcher, task
from training_triggers.s3_watch import S3KeyArrivalEventTrigger

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())
BUCKET = "airflow-training"
WATCH_KEY = f"{STUDENT}/watched/file.flag"

watched_asset = Asset(
    f"watched_flag_{STUDENT}",
    watchers=[
        AssetWatcher(
            name=f"s3_flag_watcher_{STUDENT}",
            trigger=S3KeyArrivalEventTrigger(bucket=BUCKET, key=WATCH_KEY, aws_conn_id="object_store"),
        )
    ],
)

with DAG(
    dag_id=f"watcher_triggered_dag_{STUDENT}",
    schedule=[watched_asset],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["training", "assets", "watchers"],
):

    @task
    def on_file_arrived():
        print(f"Triggered because s3://{BUCKET}/{WATCH_KEY} appeared")

    on_file_arrived()
