# Airflow on Cloudera Workflow Orchestrator: lab repository

Lab DAGs for the Airflow administration and operation course on **Cloudera Workflow Orchestrator 1.6.0** (Apache Airflow 3.2.1, Python 3.12) on Anywhere Cloud. Workflow Orchestrator deploys DAGs by syncing them from this repository.

## Repository layout

```
.
├── dags/                    <- Git SubPath synced by Workflow Orchestrator
│   ├── instructor/          <- trainer's copy of every lab
│   └── <student_name>/      <- one folder per student (created with scripts/new_student.sh)
├── student-template/        <- master copy of the labs; never synced
├── solutions/               <- solutions for modules 05 and 12 (kept out of dags/ so DAG IDs don't clash)
├── spark/                   <- PySpark scripts to upload as CDE resources (modules 06 and 11)
├── custom-image/            <- Dockerfile, training_triggers package and the module 17 watcher DAG
├── scripts/new_student.sh   <- creates dags/<student_name>/
├── tests/                   <- DAG checks run by CI
├── .github/workflows/       <- CI: parses every DAG on Airflow 3.2.1 on each push and pull request
└── docs/TEST_RESULTS.md
```

Each DAG takes the student name from its folder (`dags/<student_name>/<module>/file.py`). That name goes into every DAG ID and asset URI, so students on one shared instance never trigger each other's DAGs, and nobody edits the DAG files to set a name.

## 1. Connect the repository to Workflow Orchestrator

When deploying the instance (Marketplace > Cloudera Workflow Orchestrator > Deploy), set:

| Field | Value |
|---|---|
| Git Repository URL | This repository's SSH URL, e.g. `git@github.com:<org>/cwo-airflow-training.git` |
| Git Branch | `main` |
| Git SubPath | `dags` |
| SSH Private Key | Base64-encoded private key of a **read-only deploy key** on this repo (`base64 -w0 id_ed25519`) |

The cluster needs network access to GitHub. For a public repository, use the HTTPS URL and leave the key empty.

## 2. Instructor setup before the class

1. **Connections** (Admin > Connections in the Workflow Orchestrator UI):

| Connection ID | Type | Used by |
|---|---|---|
| `cde_runtime_api` | Cloudera Data Engineering. Host: virtual cluster Jobs API URL; Schema: `default`; Access Key / Private Key: AWC OAuth client ID / secret; Extra: `{"awc_console_url": "<console URL>", "auth_mode": "awc", "cache_dir": "/tmp"}` | 06, 11 |
| `object_store` | Amazon Web Services. Login / Password: Object Store access key / secret; Extra: `{"endpoint_url": "https://<object-store-endpoint>", "verify": false}` | 10, 13, 16, 17 |
| `slack_alerts` | Slack Incoming Webhook | 19 (optional in 09) |

2. **Bucket:** create the class bucket. If it isn't `airflow-training`, change `BUCKET` in `student-template/` (and `dags/instructor/`) before students create their folders.
3. **CDE jobs:** create `word_count_job`, `ingest_job`, `validate_job`, `transform_job` and `publish_job` from `spark/`. The module 11 jobs take the arguments `<base_path> [load_date]`.
4. **Access:** give students write access to this repository, or ask them to fork it and open pull requests (CI checks each pull request).

## 3. Student workflow

```bash
git clone git@github.com:<org>/cwo-airflow-training.git
cd cwo-airflow-training
scripts/new_student.sh asha            # creates dags/asha/
git add dags/asha
git commit -m "Add lab folder for asha"
git push
```

Wait for the next Git sync, then find your DAGs in the UI by filtering on your name (for example `xcom_exercise_asha`). To change a lab, edit the file under `dags/asha/`, then commit and push.

## 4. Before you push: check locally (optional)

```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip install "apache-airflow==3.2.1" apache-airflow-providers-standard apache-airflow-providers-amazon \
    apache-airflow-providers-slack aiobotocore pytest \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.2.1/constraints-3.12.txt
export AIRFLOW_HOME=$PWD/airflow-home PYTHONPATH=$PWD/tests/stubs:$PWD/custom-image
airflow db migrate && pytest -q tests/
```

## Notes

- **Module 12, part 2:** `debug_disabled_operator_broken.py` uses a disabled operator on purpose, so the DAG processor reports it as an import error on Workflow Orchestrator. CI allows it.
- **Module 17 watcher:** this needs the custom image. Build it from `custom-image/Dockerfile`, enable it on the instance, then copy `custom-image/dags/17-extras-pull-based-watchers/` into your `dags/<name>/` folder.
- **Module 25:** this is the partition-mapping lab. Rollup needs Airflow 3.3.
- **Tests:** CI uses a stub for the Cloudera CDE provider, which is pre-installed on Workflow Orchestrator but not on PyPI. See `docs/TEST_RESULTS.md` for what has been tested.
