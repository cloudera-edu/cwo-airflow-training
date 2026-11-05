"""Shared callbacks for the training labs.

Lives at the root of dags/ (the bundle root), so both the DAG processor and
the workers can import it as `training_callbacks`. Lab folders start with a
digit and contain hyphens, so a callback inside them can't be imported by name.
"""
from datetime import datetime, timezone


def log_deadline_miss(**kwargs):
    """SyncCallback for Deadline Alerts: runs on a worker, needs no connection."""
    context = kwargs.get("context") or {}
    dag_run = context.get("dag_run")
    dag_id = getattr(dag_run, "dag_id", None) or kwargs.get("dag_id", "unknown")
    run_id = getattr(dag_run, "run_id", "unknown")
    missed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"[DEADLINE MISSED] {dag_id} run {run_id} was still running at {missed_at}")

    # Leave a trace students can see in the UI: Admin > Variables.
    try:
        from airflow.sdk import Variable

        Variable.set(f"deadline_missed_{dag_id}", f"{run_id} at {missed_at}")
    except Exception as exc:  # the print above is still in the log
        print(f"Could not set the Variable: {exc}")
