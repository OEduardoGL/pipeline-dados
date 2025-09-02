PY ?= python3
VENV ?= .venv

.PHONY: venv install bronze silver gold all clean

venv:
	$(PY) -m venv $(VENV)
	$(VENV)/bin/pip install -U pip

install: venv
	$(VENV)/bin/pip install -e .

bronze:
	bash scripts/spark.sh --master local[2] jobs/bronze_ingest.py --config configs/dev.json

silver:
	bash scripts/spark.sh --master local[2] jobs/silver_refine.py --config configs/dev.json

gold:
	bash scripts/spark.sh --master local[2] jobs/gold_aggregate.py --config configs/dev.json

all: bronze silver gold

clean:
	rm -rf data/bronze data/silver data/gold pipe2.zip

