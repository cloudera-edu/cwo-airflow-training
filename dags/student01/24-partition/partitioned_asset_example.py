"""Module 24 - Partitioned assets (Workflow Orchestrator, Airflow 3.2.1).

The producer runs hourly on CronPartitionTimetable; each scheduled run gets a
partition key (the hour), and the asset event it emits carries that key.
The consumer runs once per partition and can see which one triggered it.

Changes for 3.2.1:
  - outlet_events[...].add_partitions() does not exist in 3.2.1. An event
    takes its partition key from the DAG run.
  - So for a manual run, pass the key when you trigger it - through the
    REST API (ties in with the REST API lab):
      curl -X POST -H "Authorization: Bearer $ACCESS_TOKEN" \
           -H "Content-Type: application/json" \
           "${CWO_URL}api/v2/dags/player_stats_etl_<student>/dagRuns" \
           -d '{"logical_date": null, "partition_key": "2026-10-27T09:00:00"}'
  - hdfs:// URI -> s3:// (it is only an identifier here; nothing is read or written).
"""
import re
from pathlib import Path
from airflow.sdk import Asset, CronPartitionTimetable, IdentityMapper, PartitionedAssetTimetable, dag, task

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())
BUCKET = "airflow-training"

player_stats = Asset(
    uri=f"s3://{BUCKET}/{STUDENT}/player-stats/",
    name=f"player_stats_{STUDENT}",
)


# --- Producer: one partitioned event per hourly run ----------------------
@dag(
    dag_id=f"player_stats_etl_{STUDENT}",
    schedule=CronPartitionTimetable("0 * * * *", timezone="UTC"),
    catchup=False,
    tags=["training", "partitions"],
)
def player_stats_etl():
    @task(outlets=[player_stats])
    def ingest(*, outlet_events, dag_run=None):
        print(f"Materialising player stats for partition {dag_run.partition_key}")
        outlet_events[player_stats].extra = {"rows_written": 1000}

    ingest()


player_stats_etl()


# --- Consumer: one run per partition, and it knows which one --------------
@dag(
    dag_id=f"player_stats_report_{STUDENT}",
    schedule=PartitionedAssetTimetable(
        assets=player_stats,
        default_partition_mapper=IdentityMapper(),  # pass the upstream key through unchanged
    ),
    catchup=False,
    tags=["training", "partitions"],
)
def player_stats_report():
    @task
    def build_report(dag_run=None):
        print(f"Building report for partition: {dag_run.partition_key}")

    build_report()


player_stats_report()
