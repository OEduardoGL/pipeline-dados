Spark Data Pipeline (Bronze/Silver/Gold)

Este projeto demonstra um pipeline de dados funcional e organizado com PySpark, usando camadas bronze/silver/gold, checks básicos de qualidade e configuração externa.

Visão Geral

- Bronze: ingestão de arquivos CSV raw para Parquet padronizado.
- Silver: limpeza, tipagem, deduplicação e joins para modelagem analítica.
- Gold: agregações (métricas de negócio) prontas para consumo.
- Qualidade: checagens simples (linhas mínimas, não nulos, unicidade) com falha explícita.
- Config: JSON simples, sem dependências extras.

Estrutura

- `configs/dev.json`: caminhos de entrada/saída e opções.
- `data/raw/`: amostras CSV (clientes, pedidos, itens de pedido).
- `src/pipe2/`: biblioteca de suporte (sessão, IO, schemas, quality, transforms).
- `jobs/`: scripts de pipeline para cada camada.

Pré‑requisitos

- Python 3.8+
- PySpark 3.x instalado e acessível (ex.: `pip install pyspark`)

Executando

Use `spark-submit` apontando para cada job e o arquivo de config.

1) Bronze (ingestão raw -> bronze Parquet):

```
spark-submit --master local[2] jobs/bronze_ingest.py --config configs/dev.json
```

2) Silver (refino/limpeza e joins):

```
spark-submit --master local[2] jobs/silver_refine.py --config configs/dev.json
```

3) Gold (agregações de negócio):

```
spark-submit --master local[2] jobs/gold_aggregate.py --config configs/dev.json
```

Saídas padrão:
- Bronze: `data/bronze/` (Parquet)
- Silver: `data/silver/` (Parquet)
- Gold: `data/gold/` (Parquet)

O que o pipeline faz (exemplo e-commerce)

- Clientes (`customers`), pedidos (`orders`) e itens de pedido (`order_items`).
- Silver: corrige tipos (datas, numéricos), remove linhas inválidas, deduplica chaves.
- Gold: calcula receita por dia e categoria.

Adaptação

- Ajuste schemas em `src/pipe2/schemas.py`.
- Ajuste paths e particionamento em `configs/dev.json`.
- Adapte transformações em `src/pipe2/transformations.py`.

Notas

- O pipeline usa `overwrite` para idempotência simples no ambiente de dev.
- Para cenários reais, evolua para incremental (watermarks, merge/upserts, etc.).

