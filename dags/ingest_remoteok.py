"""Airflow DAG: daily RemoteOK ingestion into the bronze layer.

On top of the resilience already built into the extractor (HTTP retries,
timeout, validate-before-land), Airflow adds:
  - scheduling: run automatically every day, untouched,
  - task-level retries with backoff: a second safety net above the HTTP layer,
  - monitoring + a full run history in the UI, with alerting hooks,
  - backfills: re-run any past date on demand.

The package `pulse` is imported INSIDE the task (not at the top of the file)
so DAG parsing stays fast and import errors surface as task failures, not as
a broken DAG that won't even show up in the UI.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="ingest_remoteok",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,  # do NOT replay every day since start_date on first unpause
    max_active_runs=1,  # never run two copies at once
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=2),
        "retry_exponential_backoff": True,  # 2m, 4m, 8m...
        "max_retry_delay": pendulum.duration(minutes=15),
    },
    tags=["pulse", "ingestion", "bronze"],
)
def ingest_remoteok():

    @task
    def extract(data_interval_start=None):
        from pulse.extractors.remoteok import RemoteOKExtractor

        # Airflow injects this run's data-interval date. We land into THAT
        # date's partition, so re-running any date overwrites the same file
        # (idempotent) instead of creating duplicates. Falls back to "now"
        # if the context value isn't available.
        ingestion_date = (data_interval_start or pendulum.now("UTC")).date()

        path = RemoteOKExtractor().run(logical_date=ingestion_date)
        print(f"Landed bronze data at: {path}")
        return path

    extract()


ingest_remoteok()
