"""
Script para gerar dados fictícios e popular o Supabase.
Cria as tabelas: clientes, lojas, produtos, vendas, itens_venda
"""

import os
import random
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

fake = Faker("pt_BR")
random.seed(42)
np.random.seed(42)

# ── Conexão ──────────────────────────────────────────────────────────────────
conn = psycopg2.connect(
    host=os.environ["SUPABASE_HOST"],
    dbname=os.environ["SUPABASE_DB"],
    user=os.environ["SUPABASE_USER"],
    password=os.environ["SUPABASE_PASSWORD"],
    port=os.environ["SUPABASE_PORT"],
)
cur = conn.cursor()
print("✅ Conectado ao Supabase")

# ── Criar tabelas ─────────────────────────────────────────────────────────────
cur.execute("""
DROP TABLE IF EXISTS itens_venda CASCADE;
DROP TABLE IF EXISTS vendas CASCADE;
DROP TABLE IF EXISTS clientes CASCADE;
DROP TABLE IF EXISTS produtos CASCADE;
DROP TABLE IF EXISTS lojas CASCADE;

CREATE TABLE lojas (
    id          SERIAL PRIMARY KEY,
    nome        VARCHAR(100),
    cidade      VARCHAR(100),
    uf          CHAR(2),
    regiao      VARCHAR(20),
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE produtos (
    id          SERIAL PRIMARY KEY,
    sku         VARCHAR(20) UNIQUE,
    nome        VARCHAR(100),
    categoria   VARCHAR(50),
    custo       NUMERIC(10,2),
    preco       NUMERIC(10,2),
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE clientes (
    id          SERIAL PRIMARY KEY,
    nome        VARCHAR(150),
    email       VARCHAR(150),
    cidade      VARCHAR(100),
    uf          CHAR(2),
    created_at  TIMESTAMP DEFAULT NOW(),
    updated_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE vendas (
    id              SERIAL PRIMARY KEY,
    cliente_id      INT REFERENCES clientes(id),
    loja_id         INT REFERENCES lojas(id),
    data_venda      TIMESTAMP,
    status          VARCHAR(20),
    total           NUMERIC(10,2),
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE itens_venda (
    id          SERIAL PRIMARY KEY,
    venda_id    INT REFERENCES vendas(id),
    produto_id  INT REFERENCES produtos(id),
    quantidade  INT,
    preco_unit  NUMERIC(10,2),
    subtotal    NUMERIC(10,2),
    created_at  TIMESTAMP DEFAULT NOW()
);
""")
conn.commit()
print("✅ Tabelas criadas")

# ── Lojas ─────────────────────────────────────────────────────────────────────
lojas = [
    ("Loja SP Centro",    "São Paulo",       "SP", "Sudeste"),
    ("Loja SP Sul",       "São Paulo",       "SP", "Sudeste"),
    ("Loja RJ",           "Rio de Janeiro",  "RJ", "Sudeste"),
    ("Loja BH",           "Belo Horizonte",  "MG", "Sudeste"),
    ("Loja Curitiba",     "Curitiba",        "PR", "Sul"),
    ("Loja Porto Alegre", "Porto Alegre",    "RS", "Sul"),
    ("Loja Salvador",     "Salvador",        "BA", "Nordeste"),
    ("Loja Recife",       "Recife",          "PE", "Nordeste"),
    ("Loja Brasília",     "Brasília",        "DF", "Centro-Oeste"),
    ("Loja Manaus",       "Manaus",          "AM", "Norte"),
]
execute_values(cur, "INSERT INTO lojas (nome, cidade, uf, regiao) VALUES %s", lojas)
conn.commit()
print(f"✅ {len(lojas)} lojas inseridas")

# ── Produtos ──────────────────────────────────────────────────────────────────
categorias = ["Eletrônicos", "Eletrodomésticos", "Móveis", "Vestuário", "Esportes"]
produtos = []
for i in range(1, 51):
    cat = random.choice(categorias)
    custo = round(random.uniform(20, 800), 2)
    preco = round(custo * random.uniform(1.3, 2.5), 2)
    produtos.append((f"SKU{i:04d}", f"Produto {i} - {cat}", cat, custo, preco))
execute_values(cur, "INSERT INTO produtos (sku, nome, categoria, custo, preco) VALUES %s", produtos)
conn.commit()
print(f"✅ {len(produtos)} produtos inseridos")

# ── Clientes ──────────────────────────────────────────────────────────────────
ufs = ["SP", "RJ", "MG", "PR", "RS", "BA", "PE", "DF", "AM", "GO"]
clientes = []
for _ in range(500):
    clientes.append((
        fake.name(),
        fake.email(),
        fake.city(),
        random.choice(ufs),
    ))
execute_values(cur, "INSERT INTO clientes (nome, email, cidade, uf) VALUES %s", clientes)
conn.commit()
print(f"✅ {len(clientes)} clientes inseridos")

# ── Vendas + Itens ────────────────────────────────────────────────────────────
# Dólar simulado por mês (correlação leve com volume de vendas)
dolar_mensal = {
    2023: [5.2, 5.1, 5.3, 5.0, 4.9, 4.8, 4.9, 5.0, 5.1, 5.2, 5.3, 5.4],
    2024: [5.5, 5.4, 5.3, 5.2, 5.1, 5.0, 5.1, 5.2, 5.3, 5.5, 5.8, 6.0],
    2025: [6.1, 6.0, 5.9, 5.8, 5.7, 5.6, 5.7, 5.8, 5.9, 6.0, 6.1, 6.2],
}

data_inicio = datetime(2023, 1, 1)
data_fim = datetime(2025, 12, 31)

vendas_rows = []
itens_rows = []
venda_id = 0

cur.execute("SELECT id FROM clientes")
cliente_ids = [r[0] for r in cur.fetchall()]
cur.execute("SELECT id, preco FROM produtos")
produto_rows = cur.fetchall()

data_atual = data_inicio
while data_atual <= data_fim:
    mes = data_atual.month
    ano = data_atual.year

    # Sazonalidade: nov/dez vendem mais (Black Friday + Natal)
    fator_sazonal = 1.5 if mes in [11, 12] else (0.8 if mes in [1, 2] else 1.0)
    # Correlação leve com dólar alto = menos vendas (importados ficam caros)
    dolar = dolar_mensal.get(ano, [5.5] * 12)[mes - 1]
    fator_dolar = max(0.7, 1.5 - (dolar - 4.8) * 0.15)

    n_vendas_dia = int(np.random.poisson(15 * fator_sazonal * fator_dolar))

    for _ in range(n_vendas_dia):
        venda_id += 1
        cliente_id = random.choice(cliente_ids)
        loja_id = random.randint(1, len(lojas))
        hora = timedelta(hours=random.randint(8, 21), minutes=random.randint(0, 59))
        data_venda = data_atual + hora
        status = random.choices(["concluida", "cancelada", "devolvida"], weights=[90, 7, 3])[0]

        n_itens = random.randint(1, 5)
        itens_venda = random.sample(produto_rows, n_itens)
        total = 0
        for prod_id, preco in itens_venda:
            qtd = random.randint(1, 3)
            subtotal = round(preco * qtd, 2)
            total += subtotal
            itens_rows.append((venda_id, prod_id, qtd, preco, subtotal))

        vendas_rows.append((cliente_id, loja_id, data_venda, status, round(total, 2)))

    data_atual += timedelta(days=1)

# Insert em lotes
BATCH = 1000
for i in range(0, len(vendas_rows), BATCH):
    execute_values(cur, """
        INSERT INTO vendas (cliente_id, loja_id, data_venda, status, total)
        VALUES %s
    """, vendas_rows[i:i+BATCH])
conn.commit()

for i in range(0, len(itens_rows), BATCH):
    execute_values(cur, """
        INSERT INTO itens_venda (venda_id, produto_id, quantidade, preco_unit, subtotal)
        VALUES %s
    """, itens_rows[i:i+BATCH])
conn.commit()

print(f"✅ {len(vendas_rows)} vendas inseridas")
print(f"✅ {len(itens_rows)} itens de venda inseridos")
print("\n🎉 Dados fictícios gerados com sucesso!")
cur.close()
conn.close()
