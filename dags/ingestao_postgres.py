"""
DAG: ingestao_postgres
Descrição: Extração incremental do Supabase (ERP fictício) para o BigQuery bronze.
Frequência: Diária
Tabelas: clientes, lojas, produtos, vendas, itens_venda
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from google.cloud import bigquery
import psycopg2
import psycopg2.extras
import os

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
DATASET    = os.environ.get("GCP_DATASET_BRONZE", "bronze")

DEFAULT_ARGS = {
    "owner": "pipeline-dados",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# Tabelas e suas colunas de controle incremental
TABELAS = {
    "clientes":    {"incremental": "updated_at", "pk": "id"},
    "lojas":       {"incremental": "created_at",  "pk": "id"},
    "produtos":    {"incremental": "created_at",  "pk": "id"},
    "vendas":      {"incremental": "updated_at", "pk": "id"},
    "itens_venda": {"incremental": "created_at",  "pk": "id"},
}


def get_pg_conn():
    return psycopg2.connect(
        host=os.environ["SUPABASE_HOST"],
        dbname=os.environ["SUPABASE_DB"],
        user=os.environ["SUPABASE_USER"],
        password=os.environ["SUPABASE_PASSWORD"],
        port=os.environ["SUPABASE_PORT"],
    )


def get_ultima_carga(bq_client, tabela_bq: str) -> str:
    """Retorna o maior updated_at/created_at já carregado no BigQuery."""
    try:
        query = f"SELECT MAX(_loaded_at) FROM `{tabela_bq}`"
        result = list(bq_client.query(query).result())
        val = result[0][0]
        return val if val else "1970-01-01T00:00:00"
    except Exception:
        return "1970-01-01T00:00:00"


def extrair_tabela(tabela: str, coluna_incremental: str, desde: str, pg_conn) -> list:
    """Extrai linhas novas/atualizadas do Postgres."""
    cur = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        f"SELECT * FROM {tabela} WHERE {coluna_incremental} > %s ORDER BY {coluna_incremental}",
        (desde,)
    )
    linhas = cur.fetchall()
    cur.close()
    print(f"  {tabela}: {len(linhas)} linhas extraídas")
    return [dict(r) for r in linhas]


def serializar(linhas: list, loaded_at: str, source: str) -> list:
    """Converte tipos Python para JSON-serializável e adiciona colunas de auditoria."""
    resultado = []
    for r in linhas:
        row = {}
        for k, v in r.items():
            if hasattr(v, "isoformat"):
                row[k] = v.isoformat()
            else:
                row[k] = v
        row["_loaded_at"] = loaded_at
        row["_source"] = source
        resultado.append(row)
    return resultado


def carregar_bigquery(bq_client, tabela_bq: str, linhas: list):
    """Carrega linhas no BigQuery com WRITE_APPEND."""
    if not linhas:
        print(f"  {tabela_bq}: nada a carregar")
        return

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    job = bq_client.load_table_from_json(linhas, tabela_bq, job_config=job_config)
    job.result()
    print(f"  ✅ {len(linhas)} linhas carregadas em {tabela_bq}")


def ingerir_postgres(**context):
    loaded_at = datetime.utcnow().isoformat()
    bq_client = bigquery.Client(project=PROJECT_ID)
    pg_conn   = get_pg_conn()

    for tabela, config in TABELAS.items():
        tabela_bq = f"{PROJECT_ID}.{DATASET}.erp_{tabela}"
        desde     = get_ultima_carga(bq_client, tabela_bq)
        linhas    = extrair_tabela(tabela, config["incremental"], desde, pg_conn)
        linhas_ok = serializar(linhas, loaded_at, f"supabase.{tabela}")
        carregar_bigquery(bq_client, tabela_bq, linhas_ok)

    pg_conn.close()
    print("\n🎉 Ingestão do Postgres concluída!")


with DAG(
    dag_id="ingestao_postgres",
    default_args=DEFAULT_ARGS,
    description="Extração incremental do Supabase para BigQuery bronze",
    schedule_interval="0 7 * * *",   # todo dia às 7h UTC (após o BCB)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bronze", "postgres", "ingestao"],
) as dag:

    task_ingerir = PythonOperator(
        task_id="ingerir_postgres",
        python_callable=ingerir_postgres,
    )
