"""Module 19 - Deadline Alerts (Workflow Orchestrator, Airflow 3.2.1).

Deadline Alerts replace SLAs (removed in Airflow 3). The callback runs in
the triggerer (AsyncCallback), and the triggerer can only import callables
installed in its image - not functions defined in a DAG file. So this
version uses the pre-installed Slack notifier.

Needs a Slack webhook connection named `slack_alerts` (Admin > Connections,
type Slack Incoming Webhook). For a custom callback instead, install it in a
custom image - see custom-image/training_triggers/callbacks.py.
"""
import re
from pathlib import Path
from datetime import timedelta

from airflow.sdk import DAG, AsyncCallback, DeadlineAlert, DeadlineReference
from airflow.providers.slack.notifications.slack_webhook import SlackWebhookNotifier
from airflow.providers.standard.operators.bash import BashOperator

# Your student name is taken from your folder: dags/<your_name>/<module>/<this file>.
# Use lowercase letters, digits or underscores for the folder name.
STUDENT = re.sub(r"[^a-z0-9_]", "_", Path(__file__).resolve().parents[1].name.lower())

with DAG(
    dag_id=f"deadline_alert_example_{STUDENT}",
    schedule=None,
    catchup=False,
    tags=["training", "deadline-alerts"],
    deadline=DeadlineAlert(
        # Measure from when the run was queued...
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        # ...and alert if it hasn't finished 15 minutes later.
        interval=timedelta(minutes=15),
        callback=AsyncCallback(
            SlackWebhookNotifier,
            kwargs={
                "slack_webhook_conn_id": "slack_alerts",
                "text": "[ALERT] DAG {{ dag_run.dag_id }} missed its deadline. Run: {{ dag_run.run_id }}",
            },
        ),
    ),
):
    # Deliberately exceeds the 15-minute deadline (and stays under the 1-hour default timeout)
    BashOperator(task_id="long_running_task", bash_command="sleep 1200")
