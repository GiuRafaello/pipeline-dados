"""
DAG: ingestao_bcb
Descrição: Extrai séries do Banco Central (dólar, Selic, IPCA) e carrega no BigQuery bronze.
Frequência: Diária
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from google.cloud import bigquery
import requests
import os

# ── Configurações ─────────────────────────────────────────────────────────────
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
DATASET    = os.environ.get("GCP_DATASET_BRONZE", "bronze")

SERIES = {
    "dolar": 1,    # Dólar comercial (venda)
    "selic": 11,   # Taxa Selic diária
    "ipca":  433,  # IPCA mensal
}

DEFAULT_ARGS = {
    "owner": "pipeline-dados",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ── Funções ───────────────────────────────────────────────────────────────────
def extrair_serie(nome: str, codigo: int, data_inicio: str, data_fim: str) -> list:
    """Chama a API do BCB e retorna lista de dicts."""
    url = (
        f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"
        f"?formato=json&dataInicial={data_inicio}&dataFinal={data_fim}"
    )
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    dados = resp.json()
    print(f"  {nome}: {len(dados)} registros extraídos")
    return dados


def carregar_bigquery(tabela: str, linhas: list, schema: list):
    """Carrega uma lista de dicts no BigQuery."""
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT_ID)
    ref = f"{PROJECT_ID}.{DATASET}.{tabela}"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",  # recria a tabela a cada execução
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    job = client.load_table_from_json(linhas, ref, job_config=job_config)
    job.result()
    print(f"  ✅ {len(linhas)} linhas carregadas em {ref}")


def ingerir_bcb(**context):
    """Task principal: extrai as 3 séries e carrega no BigQuery."""
    data_fim   = context["ds"]                          # data de execução da DAG (YYYY-MM-DD)
    data_inicio = (datetime.strptime(data_fim, "%Y-%m-%d") - timedelta(days=3*365)).strftime("%d/%m/%Y")
    data_fim_br = datetime.strptime(data_fim, "%Y-%m-%d").strftime("%d/%m/%Y")

    loaded_at = datetime.utcnow().isoformat()

    # ── Dólar ──────────────────────────────────────────────────────────────────
    dolar = extrair_serie("dolar", SERIES["dolar"], data_inicio, data_fim_br)
    linhas_dolar = [
        {
            "data":      r["data"],           # DD/MM/YYYY
            "valor":     float(r["valor"].replace(",", ".")),
            "_loaded_at": loaded_at,
            "_source":   "bcb_sgs_1",
        }
        for r in dolar
    ]
    carregar_bigquery(
        "bcb_dolar",
        linhas_dolar,
        [
            bigquery.SchemaField("data",       "STRING"),
            bigquery.SchemaField("valor",      "FLOAT"),
            bigquery.SchemaField("_loaded_at", "STRING"),
            bigquery.SchemaField("_source",    "STRING"),
        ],
    )

    # ── Selic ──────────────────────────────────────────────────────────────────
    selic = extrair_serie("selic", SERIES["selic"], data_inicio, data_fim_br)
    linhas_selic = [
        {
            "data":      r["data"],
            "valor":     float(r["valor"].replace(",", ".")),
            "_loaded_at": loaded_at,
            "_source":   "bcb_sgs_11",
        }
        for r in selic
    ]
    carregar_bigquery(
        "bcb_selic",
        linhas_selic,
        [
            bigquery.SchemaField("data",       "STRING"),
            bigquery.SchemaField("valor",      "FLOAT"),
            bigquery.SchemaField("_loaded_at", "STRING"),
            bigquery.SchemaField("_source",    "STRING"),
        ],
    )

    # ── IPCA ───────────────────────────────────────────────────────────────────
    ipca = extrair_serie("ipca", SERIES["ipca"], data_inicio, data_fim_br)
    linhas_ipca = [
        {
            "data":      r["data"],
            "valor":     float(r["valor"].replace(",", ".")),
            "_loaded_at": loaded_at,
            "_source":   "bcb_sgs_433",
        }
        for r in ipca
    ]
    carregar_bigquery(
        "bcb_ipca",
        linhas_ipca,
        [
            bigquery.SchemaField("data",       "STRING"),
            bigquery.SchemaField("valor",      "FLOAT"),
            bigquery.SchemaField("_loaded_at", "STRING"),
            bigquery.SchemaField("_source",    "STRING"),
        ],
    )



# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="ingestao_bcb",
    default_args=DEFAULT_ARGS,
    description="Ingere séries do Banco Central no BigQuery bronze",
    schedule_interval="0 6 * * *",   # todo dia às 6h UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bronze", "bcb", "ingestao"],
) as dag:

    task_ingerir = PythonOperator(
        task_id="ingerir_bcb",
        python_callable=ingerir_bcb,
    )
