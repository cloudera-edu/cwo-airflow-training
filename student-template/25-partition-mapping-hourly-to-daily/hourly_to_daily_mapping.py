"""Module 25 - Mapping hourly partitions to a daily partition (Workflow Orchestrator, Airflow 3.2.1).

REPLACES the Rollup lab. The original used RollupMapper, SegmentWindow,
FixedKeyMapper and PartitionedAtRuntime, which arrive in Airflow 3.3 and are
not in 3.2.1. In 3.2.1 the scheduler does not yet wait for every upstream
partition before running the downstream DAG (true rollup is 3.3).

What 3.2.1 does support, and what this lab shows: a partition MAPPER.
StartOfDayMapper turns an hourly key such as "2026-10-27T09:00:00" into the
daily key "2026-10-27", so the daily DAG's run is labelled with the day it
belongs to.

Try it: trigger the producer through the REST API for 09:00 and 10:00 on the
same day. In 3.2.1 the daily DAG runs once PER hourly event - two runs, both
with partition_key "2026-10-27". Discuss why that isn't a rollup yet, and what
RollupMapper + DayWindow change in Airflow 3.3.
  curl -X POST -H "Authorization: Bearer $ACCESS_TOKEN" -H "Content-Type: application/json" \
       "${CWO_URL}api/v2/dags/ingest_hourly_sales_<student>/dagRuns" \
       -d '{"logical_date": null, "partition_key": "2026-10-27T09:00:00"}'
"""
import re
from pathlib import Path
from airflow.sdk import Asset, CronPartitionTimetable, PartitionedAssetTimetable, StartOfDayMapper, dag, task

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())
BUCKET = "airflow-training"

hourly_sales = Asset(
    uri=f"s3://{BUCKET}/{STUDENT}/hourly-sales/",
    name=f"hourly_sales_{STUDENT}",
)


@dag(
    dag_id=f"ingest_hourly_sales_{STUDENT}",
    schedule=CronPartitionTimetable("0 * * * *", timezone="UTC"),  # keys like 2026-10-27T09:00:00
    catchup=False,
    tags=["training", "partitions"],
)
def ingest_hourly_sales():
    @task(outlets=[hourly_sales])
    def ingest(dag_run=None):
        print(f"Ingested hourly partition {dag_run.partition_key}")

    ingest()


ingest_hourly_sales()


@dag(
    dag_id=f"daily_sales_summary_{STUDENT}",
    schedule=PartitionedAssetTimetable(
        assets=hourly_sales,
        default_partition_mapper=StartOfDayMapper(),  # 2026-10-27T09:00:00 -> 2026-10-27
    ),
    catchup=False,
    tags=["training", "partitions"],
)
def daily_sales_summary():
    @task
    def summarize(dag_run=None):
        print(f"Daily summary for partition: {dag_run.partition_key}")

    summarize()


daily_sales_summary()
