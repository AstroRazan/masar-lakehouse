# SDA-DSC-214 workshop helpers.  Usage: make <target>
PY  := $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
DBT := $(if $(wildcard .venv/bin/dbt),.venv/bin/dbt,dbt)
export PYTHONPATH := $(CURDIR)/src
export MASAR_HOME := $(CURDIR)

.PHONY: help setup doctor data clean-data clean-lakehouse test kafka-up kafka-down kafka-topic \
        register dbt-deps dbt-debug pipeline

help:
	@echo "make setup           create .venv and install pinned packages"
	@echo "make doctor          check Java, Python and every package"
	@echo "make data            generate the Masar dataset (seeded)"
	@echo "make register        register bronze Delta tables so dbt can read them (Day 1)"
	@echo "make dbt-deps|dbt-debug   dbt helpers (Day 1)"
	@echo "make kafka-up|kafka-topic|kafka-down   single-broker Kafka (Day 2)"
	@echo "make pipeline        run the whole pipeline (you write it on Day 3)"
	@echo "make clean-lakehouse delete everything you built under lakehouse/"
	@echo "make test            run the data-generator tests"

setup:
	python3 -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements.txt
	@echo "Now run:  source .venv/bin/activate && make doctor"

doctor:
	$(PY) scripts/doctor.py

data:
	$(PY) scripts/make_data.py --seed 42

clean-data:
	rm -rf data/raw/*.csv data/raw/gps data/fixtures/*.parquet data/fixtures/*.csv data/fixtures/*.json

clean-lakehouse:
	rm -rf lakehouse metastore_db derby.log spark-warehouse dbt/masar/target dbt/masar/logs

test:
	$(PY) -m pytest -q tests

register:
	$(PY) -m masar.register

dbt-deps:
	cd dbt/masar && $(DBT) deps

dbt-debug:
	cd dbt/masar && $(DBT) debug

kafka-up:
	docker compose up -d kafka

kafka-topic:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
	  --topic masar.gps.pings --partitions 3 --bootstrap-server localhost:9092

kafka-down:
	docker compose down

pipeline:
	$(PY) -m masar.pipeline
