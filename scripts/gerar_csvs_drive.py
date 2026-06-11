"""
Script para gerar os CSVs fictícios e salvá-los localmente.
Depois você os sobe manualmente para o Google Drive.
"""

import csv
import os

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "csvs_drive")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── metas_vendas.csv ──────────────────────────────────────────────────────────
metas = []
lojas = list(range(1, 11))
categorias = ["Eletrônicos", "Eletrodomésticos", "Móveis", "Vestuário", "Esportes"]

for ano in [2023, 2024, 2025]:
    for mes in range(1, 13):
        for loja_id in lojas:
            for categoria in categorias:
                # Meta cresce 5% ao ano, com sazonalidade em nov/dez
                base = 15000
                fator_ano = 1.05 ** (ano - 2023)
                fator_mes = 1.4 if mes in [11, 12] else (0.85 if mes in [1, 2] else 1.0)
                meta = round(base * fator_ano * fator_mes, 2)
                metas.append({
                    "ano": ano,
                    "mes": mes,
                    "loja_id": loja_id,
                    "categoria": categoria,
                    "meta_valor": meta,
                })

with open(f"{OUTPUT_DIR}/metas_vendas.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["ano", "mes", "loja_id", "categoria", "meta_valor"])
    writer.writeheader()
    writer.writerows(metas)
print(f"✅ metas_vendas.csv — {len(metas)} linhas")

# ── lojas.csv ─────────────────────────────────────────────────────────────────
lojas_data = [
    (1,  "Loja SP Centro",    "São Paulo",       "SP", "Sudeste"),
    (2,  "Loja SP Sul",       "São Paulo",       "SP", "Sudeste"),
    (3,  "Loja RJ",           "Rio de Janeiro",  "RJ", "Sudeste"),
    (4,  "Loja BH",           "Belo Horizonte",  "MG", "Sudeste"),
    (5,  "Loja Curitiba",     "Curitiba",        "PR", "Sul"),
    (6,  "Loja Porto Alegre", "Porto Alegre",    "RS", "Sul"),
    (7,  "Loja Salvador",     "Salvador",        "BA", "Nordeste"),
    (8,  "Loja Recife",       "Recife",          "PE", "Nordeste"),
    (9,  "Loja Brasília",     "Brasília",        "DF", "Centro-Oeste"),
    (10, "Loja Manaus",       "Manaus",          "AM", "Norte"),
]

with open(f"{OUTPUT_DIR}/lojas.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["id", "nome", "cidade", "uf", "regiao"])
    writer.writerows(lojas_data)
print(f"✅ lojas.csv — {len(lojas_data)} linhas")

print(f"\n📁 CSVs salvos em: {OUTPUT_DIR}")
print("Próximo passo: suba esses arquivos para uma pasta no Google Drive")
print("e compartilhe a pasta com a Service Account:")
print("  pipeline-dados-sa@pipeline-dados-499113.iam.gserviceaccount.com")
