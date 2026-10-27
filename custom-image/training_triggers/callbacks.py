"""Deadline Alert callbacks that the triggerer can import (install in the custom image)."""


async def log_deadline_miss(**kwargs):
    """Minimal AsyncCallback for Deadline Alerts: writes the miss to the triggerer log."""
    context = kwargs.get("context", {})
    dag_run = context.get("dag_run")
    print(f"[ALERT] Deadline exceeded for {getattr(dag_run, 'dag_id', 'unknown')}: {kwargs.get('alert_type')}")
