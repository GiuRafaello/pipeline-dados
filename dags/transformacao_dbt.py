"""
DAG: transformacao_dbt
Descrição: Executa dbt run para transformar bronze -> silver -> gold.
Frequência: Diária, após as ingestões (7h30 UTC)
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

DEFAULT_ARGS = {
    "owner": "pipeline-dados",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="transformacao_dbt",
    default_args=DEFAULT_ARGS,
    description="Executa dbt run: bronze -> silver -> gold",
    schedule_interval="30 7 * * *",   # 30 min após as ingestões (7h UTC)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["silver", "gold", "dbt", "transformacao"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --profiles-dir /opt/airflow/dbt"
        ),
    )
