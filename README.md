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
- PySpark 3.x instalado e acessível 

Executando

Use `spark-submit` apontando para cada job e o arquivo de config.

1) Bronze (ingestão raw -> bronze Parquet):

```
bash scripts/spark.sh --master local[2] jobs/bronze_ingest.py --config configs/dev.json
```

2) Silver (refino/limpeza e joins):

```
bash scripts/spark.sh --master local[2] jobs/silver_refine.py --config configs/dev.json
```

3) Gold (agregações de negócio):

```
bash scripts/spark.sh --master local[2] jobs/gold_aggregate.py --config configs/dev.json
```

Saídas padrão:
- Bronze: `data/bronze/` (Parquet)
- Silver: `data/silver/` (Parquet)
- Gold: `data/gold/` (Parquet)

Quickstart 

- Pré‑requisitos: Java 17 (JDK), Python 3.8+.
- Clonar e instalar localmente o pacote (para que `pipe2` seja importável):

```
python3 -m venv .venv && source .venv/bin/activate
pip install -U pip
pip install -e .
```

- Executar com o wrapper que já distribui o pacote para os executores:

```
bash scripts/spark.sh --master local[2] jobs/bronze_ingest.py --config configs/dev.json
bash scripts/spark.sh --master local[2] jobs/silver_refine.py --config configs/dev.json
bash scripts/spark.sh --master local[2] jobs/gold_aggregate.py --config configs/dev.json
```

Atalhos (Makefile):

```
make venv && make install
make bronze
make silver
make gold
```

Obs.: o `configs/dev.json` já define `spark.master` como `local[2]`, então também é possível rodar com `python jobs/*.py --config ...` em modo local.
