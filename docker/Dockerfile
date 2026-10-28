# Airflow image for the SightSync Stage 1 extraction and Stage 2 transformation DAGs.
# Adds the Polars engine and the `extraction` / `transformation` packages on top
# of the official Airflow image so the DAG can import standalone pipeline code.
FROM apache/airflow:2.9.3-python3.12

USER root
RUN pip install --no-cache-dir "polars>=1.0,<2.0" "python-dotenv>=1.0,<2.0"
USER airflow

# Project code is bind-mounted at runtime (see docker-compose.yml), but we copy
# it too so the image is self-contained.
COPY --chown=airflow:root extraction /opt/airflow/extraction
COPY --chown=airflow:root transformation /opt/airflow/transformation
COPY --chown=airflow:root dags /opt/airflow/dags
COPY --chown=airflow:root scripts /opt/airflow/scripts
