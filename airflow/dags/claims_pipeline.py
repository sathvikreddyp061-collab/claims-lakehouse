"""End-to-end claims pipeline DAG.

  synthea_generate ─▶ edi_build ─▶ kafka_produce ─▶ wait_bronze ─▶ dbt_build ─▶ ge_gate ─▶ pg_mirror

`wait_bronze` is a sensor that polls the Iceberg bronze row count — it's the
only async piece; everything else is shell-out idempotent.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.sensors.bash import BashSensor


PROJECT_ROOT = "/workspace"

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(minutes=30),
}

with DAG(
    dag_id="claims_pipeline",
    description="Synthea → EDI 837 → Kafka → Spark bronze → dbt → GE → Postgres mirror",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["claims", "lakehouse", "hipaa"],
) as dag:

    synthea_generate = BashOperator(
        task_id="synthea_generate",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "test -d data/synthea_output/csv || make synthea"
        ),
        doc_md="Idempotent — only runs Synthea if the CSVs aren't cached locally yet.",
    )

    edi_build = BashOperator(
        task_id="edi_build",
        bash_command=f"cd {PROJECT_ROOT} && make edi",
        doc_md="Deterministic EDI 837 file generation from the Synthea CSV export.",
    )

    kafka_produce = BashOperator(
        task_id="kafka_produce",
        bash_command=f"cd {PROJECT_ROOT} && make produce",
        execution_timeout=timedelta(minutes=15),
    )

    # The bronze stream runs continuously outside Airflow; we just wait for it
    # to have ingested enough rows for the dbt build to be meaningful.
    wait_bronze = BashSensor(
        task_id="wait_bronze",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "ls data/synthea_output/csv/claims.csv >/dev/null && "
            "rows=$(docker exec cl-redpanda rpk topic describe claims.edi837.v1 -p | "
            "awk 'NR>1 {sum+=$NF} END {print sum}'); "
            "test \"$rows\" -gt 0"
        ),
        poke_interval=30,
        timeout=60 * 30,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {PROJECT_ROOT} && make dbt",
        doc_md="Runs `dbt build` (deps + run + test) against DuckDB.",
    )

    ge_gate = BashOperator(
        task_id="ge_gate",
        bash_command=f"cd {PROJECT_ROOT} && make ge",
        doc_md="Hard gate — non-zero exit fails downstream tasks before they touch Postgres.",
    )

    pg_mirror = BashOperator(
        task_id="pg_mirror",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            "python -m scripts.pg_mirror"
        ),
        doc_md="Sync the gold marts to the Postgres mirror that the Vercel dashboard reads.",
    )

    synthea_generate >> edi_build >> kafka_produce >> wait_bronze >> dbt_build >> ge_gate >> pg_mirror
