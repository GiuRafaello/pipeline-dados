# Pipeline de Dados — Vendas vs Indicadores Econômicos

Dashboard analítico que responde: **"Como as vendas se comportam frente ao câmbio e à inflação, e estamos batendo as metas?"**

## Arquitetura

```
Supabase (PostgreSQL)  ──┐
Banco Central (API)    ──┼──► Airflow ──► BigQuery Bronze ──► dbt ──► Silver/Gold ──► Power BI
Google Drive (CSV)     ──┘
```

### Camadas do Data Warehouse (BigQuery)

| Camada | Conteúdo |
|--------|----------|
| **Bronze** | Dados brutos das fontes (append/truncate) |
| **Silver** | Views com limpeza, deduplicação e tipagem (dbt) |
| **Gold** | Tabelas analíticas em star schema (dbt) |

### Modelo estrela (Gold)

- `fct_vendas` — fato de vendas com 21.5k registros
- `fct_indicadores_economicos` — dólar, Selic e IPCA diários/mensais
- `vendas_vs_meta_mensal` — vendas agregadas vs metas por loja/mês
- `dim_loja`, `dim_produto`, `dim_cliente`, `dim_calendario`

## Stack

| Ferramenta | Função |
|-----------|--------|
| GitHub Codespaces | Ambiente de desenvolvimento cloud |
| Docker Compose | Orquestração local dos containers |
| Apache Airflow | Agendamento e orquestração dos pipelines |
| Supabase (PostgreSQL) | Fonte ERP fictícia |
| Banco Central API | Indicadores econômicos reais (dólar, Selic, IPCA) |
| Google Drive | Arquivos CSV de metas e lojas |
| Google BigQuery | Data warehouse (bronze/silver/gold) |
| dbt-core | Transformações SQL versionadas |
| Power BI Desktop | Dashboard analítico final |

## DAGs do Airflow

| DAG | Horário | Descrição |
|-----|---------|-----------|
| `ingestao_bcb` | 06:00 UTC | Extrai séries do Banco Central |
| `ingestao_postgres` | 07:00 UTC | Extração incremental do Supabase |
| `ingestao_drive` | 07:00 UTC | Download de CSVs do Google Drive |
| `transformacao_dbt` | 07:30 UTC | Executa dbt run (silver + gold) |

## Estrutura do Projeto

```
pipeline-dados/
├── dags/                        # DAGs do Airflow
│   ├── ingestao_bcb.py
│   ├── ingestao_postgres.py
│   ├── ingestao_drive.py
│   └── transformacao_dbt.py
├── dbt/                         # Modelos dbt
│   ├── models/
│   │   ├── silver/              # Views de staging
│   │   └── gold/                # Tabelas analíticas
│   ├── macros/
│   ├── dbt_project.yml
│   └── profiles.yml
├── scripts/
│   └── gerar_dados_ficticios.py # Popula o Supabase com dados fictícios
├── secrets/                     # Credenciais (não versionadas)
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

## Como Reproduzir

### Pré-requisitos

- Conta GitHub (para Codespaces)
- Projeto no Google Cloud Platform com BigQuery ativado
- Conta Supabase (gratuita)
- Google Drive com os CSVs compartilhados com a Service Account

### 1. Configurar o ambiente

```bash
# No GitHub Codespaces, após clonar o repositório:
cp .env.example .env
# Preencha o .env com suas credenciais
```

### 2. Adicionar credenciais GCP

```bash
# Cole o conteúdo do arquivo JSON da Service Account:
sudo tee secrets/gcp-sa.json > /dev/null << 'EOF'
{ ... conteúdo do JSON ... }
EOF
```

### 3. Subir o Airflow

```bash
docker compose up airflow-init
docker compose up -d
```

Acesse em `http://localhost:8080` (usuário: `admin`, senha: `admin`).

### 4. Gerar dados fictícios no Supabase

```bash
docker compose exec airflow-webserver python /opt/airflow/scripts/gerar_dados_ficticios.py
```

### 5. Executar as DAGs

Ative e execute manualmente na ordem:
1. `ingestao_bcb`
2. `ingestao_postgres`
3. `ingestao_drive`
4. `transformacao_dbt`

### 6. Conectar o Power BI

- **Obter Dados → Google BigQuery**
- Projeto: `seu-projeto-id`
- Carregue as 7 tabelas do dataset `gold`

## Dashboard

Três páginas analíticas:

1. **Visão Geral** — KPIs de total de vendas, nº de vendas, ticket médio e % de meta atingida
2. **Vendas vs Indicadores** — Evolução mensal de vendas comparada ao dólar
3. **Metas por Loja** — Vendas realizadas vs meta e ranking de desempenho por loja

## Segurança

- `secrets/gcp-sa.json` e `.env` estão no `.gitignore` e nunca são versionados
- Use variáveis de ambiente para todas as credenciais
