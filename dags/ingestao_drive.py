"""
DAG: ingestao_drive
Descrição: Baixa CSVs do Google Drive e carrega no BigQuery bronze.
Frequência: Diária
Arquivos: metas_vendas.csv, lojas.csv
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from google.cloud import bigquery
from googleapiclient.discovery import build
from google.oauth2 import service_account
import csv
import io
import os

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "")
DATASET    = os.environ.get("GCP_DATASET_BRONZE", "bronze")
FOLDER_ID  = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "")
SA_FILE    = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")

DEFAULT_ARGS = {
    "owner": "pipeline-dados",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

ARQUIVOS = {
    "metas_vendas.csv": f"{PROJECT_ID}.{DATASET}.drive_metas_vendas",
    "lojas.csv":        f"{PROJECT_ID}.{DATASET}.drive_lojas",
}


def get_drive_service():
    creds = service_account.Credentials.from_service_account_file(
        SA_FILE,
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )
    return build("drive", "v3", credentials=creds)


def listar_arquivos(service, nomes: list) -> dict:
    """Busca arquivos pelo nome (compartilhados com a Service Account)."""
    resultado = {}
    for nome in nomes:
        result = service.files().list(
            q=f"name='{nome}' and trashed=false",
            fields="files(id, name)",
            spaces="drive",
        ).execute()
        arquivos = result.get("files", [])
        if arquivos:
            resultado[nome] = arquivos[0]["id"]
    return resultado


def baixar_csv(service, file_id: str) -> list:
    """Baixa um CSV do Drive e retorna lista de dicts."""
    content = service.files().get_media(fileId=file_id).execute()
    text = content.decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


def converter_tipos(linhas: list) -> list:
    """Converte strings numéricas para int/float."""
    resultado = []
    for r in linhas:
        row = {}
        for k, v in r.items():
            try:
                row[k] = int(v)
            except (ValueError, TypeError):
                try:
                    row[k] = float(v)
                except (ValueError, TypeError):
                    row[k] = v
        resultado.append(row)
    return resultado


def carregar_bigquery(bq_client, tabela_bq: str, linhas: list, loaded_at: str):
    if not linhas:
        print(f"  {tabela_bq}: nada a carregar")
        return

    for row in linhas:
        row["_loaded_at"] = loaded_at
        row["_source"] = "google_drive"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    job = bq_client.load_table_from_json(linhas, tabela_bq, job_config=job_config)
    job.result()
    print(f"  ✅ {len(linhas)} linhas carregadas em {tabela_bq}")


def ingerir_drive(**context):
    loaded_at  = datetime.utcnow().isoformat()
    bq_client  = bigquery.Client(project=PROJECT_ID)
    service    = get_drive_service()
    arquivos   = listar_arquivos(service, list(ARQUIVOS.keys()))

    print(f"Arquivos encontrados no Drive: {list(arquivos.keys())}")

    for nome_arquivo, tabela_bq in ARQUIVOS.items():
        if nome_arquivo not in arquivos:
            print(f"  ⚠️ {nome_arquivo} não encontrado no Drive — pulando")
            continue

        file_id = arquivos[nome_arquivo]
        linhas  = baixar_csv(service, file_id)
        linhas  = converter_tipos(linhas)
        print(f"  {nome_arquivo}: {len(linhas)} linhas extraídas")
        carregar_bigquery(bq_client, tabela_bq, linhas, loaded_at)

    print("\n🎉 Ingestão do Drive concluída!")


with DAG(
    dag_id="ingestao_drive",
    default_args=DEFAULT_ARGS,
    description="Baixa CSVs do Google Drive e carrega no BigQuery bronze",
    schedule_interval="0 7 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bronze", "drive", "ingestao"],
) as dag:

    task_ingerir = PythonOperator(
        task_id="ingerir_drive",
        python_callable=ingerir_drive,
    )
