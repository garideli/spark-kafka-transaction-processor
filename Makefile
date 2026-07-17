PYTHON ?= /opt/anaconda3/bin/python3

.PHONY: setup run test clean

setup:
	$(PYTHON) -m pip install -r python/requirements.txt

run:
	$(PYTHON) python/run_pipeline.py --events 5000

test:
	$(PYTHON) -m pytest -q python/tests

clean:
	rm -rf data/stream data/transactions_full.parquet output checkpoints reports/figures spark-warehouse metastore_db .pytest_cache
