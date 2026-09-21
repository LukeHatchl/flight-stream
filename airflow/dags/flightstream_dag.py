"""Phase 3 — every 10 minutes: check for new bronze data, build staging models,
run dbt tests as a quality gate, then refresh the gold marts."""
from __future__ import annotations

import os
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator

DATA_DIR = os.environ.get("FLIGHTSTREAM_DATA_DIR", "/opt/flightstream/data")
DBT_DIR = os.environ.get("FLIGHTSTREAM_DBT_DIR", "/opt/flightstream/dbt")
DBT_CMD = f"cd {DBT_DIR} && dbt"


@dag(
    dag_id="flightstream_pipeline",
    description="load new bronze -> dbt run (staging) -> dbt test -> refresh marts",
    schedule="*/10 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["flightstream"],
)
def flightstream_pipeline():

    @task
    def check_new_bronze_data() -> None:
        bronze_files = list(Path(DATA_DIR, "bronze").glob("**/*.parquet"))
        if not bronze_files:
            raise AirflowException(
                f"No bronze snapshots found under {DATA_DIR}/bronze — is the collector running?"
            )
        print(f"Found {len(bronze_files)} bronze snapshot(s).")

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT_CMD} run --select staging --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_CMD} test --profiles-dir .",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"{DBT_CMD} run --select marts --profiles-dir .",
    )

    check_new_bronze_data() >> dbt_run_staging >> dbt_test >> dbt_run_marts


flightstream_pipeline()
