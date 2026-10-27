# Custom Workflow Orchestrator image for the training (module 17 watcher, optional custom deadline callback).
# aiobotocore is added for async S3 access from the triggerer (skip it if the base image already has it).
# Follows the Workflow Orchestrator custom-image rules: build on the Cloudera base image, keep its
# packages, Airflow/Python versions, AIRFLOW_HOME and entrypoint, and run as UID 50000.
ARG REGISTRY=registry.example.com
FROM ${REGISTRY}/cloudera/cloudera-airflow-3.2.1-py3.12:1.6.0

USER 0
COPY pyproject.toml /tmp/training-triggers/
COPY training_triggers /tmp/training-triggers/training_triggers
RUN uv pip install --python /opt/airflow_venv/bin/python \
      --constraint /usr/local/airflow/constraints.txt /tmp/training-triggers aiobotocore \
 && rm -rf /tmp/training-triggers
USER 50000
