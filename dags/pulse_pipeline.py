"""End-to-end Pulse pipeline: extract -> silver -> load -> dbt.

Extract and load run in-process (the pulse package is in the Airflow image).
Silver runs the existing pulse-spark image as a sibling container via the
mounted Docker socket, so the Spark/Java environment is reused as-is.
"""
from __future__ import annotations

import os
from datetime import date

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator

SOURCES = ("remoteok", "adzuna", "usajobs")
SPARK_IMAGE = "pulse-spark"
DBT_DIR = "/usr/local/airflow/dbt"
# dbt's profile is written fresh at task runtime (see _dbt_bash) into this dir,
# so we never depend on a host-provided profiles.yml or its encoding.
DBT_PROFILES_DIR = "/tmp/pulse_dbt"

COPY_JOBS = """
    copy into raw_jobs from @pulse_silver_stage
    pattern = '.*silver/jobs/.*[.]parquet'
    file_format = (format_name = pulse_parquet_format)
    match_by_column_name = case_insensitive
    on_error = abort_statement
"""

COPY_SKILLS = """
    copy into raw_job_skills from @pulse_silver_stage
    pattern = '.*silver/job_skills/.*[.]parquet'
    file_format = (format_name = pulse_parquet_format)
    match_by_column_name = case_insensitive
    on_error = abort_statement
"""


def _spark_env() -> dict[str, str]:
    keys = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION", "S3_BUCKET")
    env = {k: os.environ[k] for k in keys if k in os.environ}
    env["DATA_LAKE_BACKEND"] = os.environ.get("DATA_LAKE_BACKEND", "s3")
    return env


def _dbt_env() -> dict[str, str]:
    # Forward Snowflake creds into the task env; the profiles.yml written at
    # runtime reads them via shell expansion.
    keys = (
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
    )
    return {k: os.environ[k] for k in keys if k in os.environ}


def _dbt_bash(cmd: str) -> str:
    # Write a clean profiles.yml at runtime from the task env, then run dbt
    # against it. Unquoted heredoc => the shell substitutes the real cred
    # values, so there is no host file, no encoding risk, no path guessing.
    return (
        "set -e\n"
        f"mkdir -p {DBT_PROFILES_DIR}\n"
        f"cat > {DBT_PROFILES_DIR}/profiles.yml <<PROFILESEOF\n"
        "pulse:\n"
        "  target: prod\n"
        "  outputs:\n"
        "    prod:\n"
        "      type: snowflake\n"
        '      account: "$SNOWFLAKE_ACCOUNT"\n'
        '      user: "$SNOWFLAKE_USER"\n'
        '      password: "$SNOWFLAKE_PASSWORD"\n'
        '      role: "${SNOWFLAKE_ROLE:-SYSADMIN}"\n'
        '      warehouse: "${SNOWFLAKE_WAREHOUSE:-PULSE_WH}"\n'
        '      database: "${SNOWFLAKE_DATABASE:-PULSE}"\n'
        "      schema: MARTS\n"
        "      threads: 4\n"
        "PROFILESEOF\n"
        f"cd {DBT_DIR} && dbt {cmd} --profiles-dir {DBT_PROFILES_DIR}\n"
    )


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["pulse"],
)
def pulse_pipeline():
    @task
    def extract(source: str, ds: str | None = None) -> str:
        from pulse.extractors.adzuna import AdzunaExtractor
        from pulse.extractors.remoteok import RemoteOKExtractor
        from pulse.extractors.usajobs import USAJobsExtractor

        extractors = {
            "remoteok": RemoteOKExtractor,
            "adzuna": AdzunaExtractor,
            "usajobs": USAJobsExtractor,
        }
        return extractors[source]().run(date.fromisoformat(ds))

    @task
    def load_raw() -> None:
        import snowflake.connector

        conn = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "PULSE_WH"),
            database=os.environ.get("SNOWFLAKE_DATABASE", "PULSE"),
            schema="RAW",
        )
        try:
            cur = conn.cursor()
            cur.execute(COPY_JOBS)
            cur.execute(COPY_SKILLS)
        finally:
            conn.close()

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=_dbt_bash("run"),
        env=_dbt_env(),
        append_env=True,
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=_dbt_bash("test"),
        env=_dbt_env(),
        append_env=True,
    )

    load = load_raw()
    for source in SOURCES:
        extracted = extract.override(task_id=f"extract_{source}")(source)
        silver = DockerOperator(
            task_id=f"silver_{source}",
            image=SPARK_IMAGE,
            command=f"spark/silver_{source}.py --date {{{{ ds }}}}",
            environment=_spark_env(),
            docker_url="unix://var/run/docker.sock",
            network_mode="bridge",
            auto_remove="success",
            mount_tmp_dir=False,
        )
        extracted >> silver >> load

    load >> dbt_run >> dbt_test


pulse_pipeline()