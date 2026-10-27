# Test results

Run on 27 Oct 2026 against **Apache Airflow 3.2.1** on Python 3.12, with the providers that Workflow Orchestrator ships (Standard 1.12.3, Amazon 9.25.0, Slack 9.10.0).

**What wasn't tested:** a real Workflow Orchestrator instance, the real Cloudera CDE provider (a stub stood in for `CdeRunJobOperator`), the platform's disabled-operator policy, Slack delivery, and the Spark jobs.

## 1. Parsing

All 40 DAGs (`dags/`, `solutions/`, `custom-image/dags/`) load with no import errors.

## 2. Single runs (`airflow dags test`)

| DAG | Result |
|---|---|
| sample_intro_dag, xcom_exercise, my_first_dag, training_first_dag | success |
| jinja_templating_example | success |
| no_catchup_demo | success |
| dynamic_table_processing, retry_and_alerting_example | success |
| debug_exercise (broken) | failed, as intended |
| debug_exercise, branching_exercise, debug_disabled_operator (solutions) | success |
| deferrable_local_example | success |
| producer_with_extras, parent_dag, dynamic_task_mapping_example, xcom_explicit_example | success |
| cde_orchestration, staging_raw_events_pipeline (CDE stub) | success |
| player_stats_etl, ingest_hourly_sales | success |

## 3. Full scheduler run

Setup: Airflow standalone (API server, scheduler, DAG processor, triggerer), with a local S3 server standing in for Object Store and DAGs triggered through REST API v2.

| Module | Scenario | Result |
|---|---|---|
| 10 | Producer writes to Object Store; consumer runs on the asset | success, consumer asset-triggered |
| 13 | Minimal deferrable operator resumes through the triggerer | success |
| 13 | Three S3 waits (custom deferrable, built-in deferrable, reschedule) wait, then resume after upload | all three deferred / up_for_reschedule, then success |
| 16 | `orders \| customers` runs after orders only | success |
| 16 | `orders & customers` waits for both | no run after orders only; ran after customers |
| 16 | AssetAlias producer triggers alias consumer | success |
| 17 | Extras passed from producer to consumer | success |
| 17 | Watcher (custom image trigger) fires on file upload and consumes the flag | success, flag deleted |
| 24 | Manual trigger with partition_key; consumer gets the same key | success, key 2026-10-27T09:00:00 |
| 25 | Two hourly producer runs on the same day | two daily runs, both key 2026-10-27 (no rollup in 3.2.1) |

The triggerer stayed healthy for the whole run.

**Bug found and fixed:** the first version of the module 17 trigger called the synchronous S3 hook from a thread, and that crashed the triggerer process ("Response read out of order"). On a shared instance this would stop every student's deferred tasks. The trigger is now fully async.

## 4. Repository layout check

The repo was tested with three folders in `dags/` (instructor plus two students created with `scripts/new_student.sh`): 108 DAGs, no import errors, and no clashing DAG IDs. Asset URIs were separate per student (for example `s3://airflow-training/asha/assets/...`). `pytest tests/` (the CI checks) passed. Sample DAGs from a student folder ran to success with `airflow dags test`.
